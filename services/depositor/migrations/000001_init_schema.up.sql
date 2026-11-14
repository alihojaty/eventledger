CREATE TABLE IF NOT EXISTS deposits (
                                        event_id     VARCHAR(255) COLLATE "C" PRIMARY KEY,
    user_id      INTEGER NOT NULL CHECK (user_id > 0),
    currency     VARCHAR(10) NOT NULL CHECK (currency IN ('USDT', 'BTC', 'IRT')),
    amount       NUMERIC(36, 18) NOT NULL CHECK (amount > 0),
    is_settled   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    ) WITH (
          fillfactor = 85,
          autovacuum_vacuum_scale_factor = 0.02,
          autovacuum_vacuum_threshold = 1000,
          autovacuum_vacuum_cost_delay = 2,
          autovacuum_vacuum_cost_limit = 2000,
          autovacuum_analyze_scale_factor = 0.02
          );

CREATE INDEX IF NOT EXISTS idx_deposits_processed_at_desc
    ON deposits (processed_at DESC)
    WITH (fillfactor = 95);

CREATE INDEX IF NOT EXISTS idx_deposits_unsettled_pending
    ON deposits (processed_at ASC)
    WITH (fillfactor = 90)
    WHERE is_settled = FALSE;

CREATE TABLE IF NOT EXISTS balances (
                                        user_id    INTEGER NOT NULL CHECK (user_id > 0),
    currency   VARCHAR(10) NOT NULL CHECK (currency IN ('USDT', 'BTC', 'IRT')),
    balance    NUMERIC(36, 18) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, currency)
    ) WITH (
          fillfactor = 70,
          autovacuum_vacuum_scale_factor = 0.01,
          autovacuum_vacuum_threshold = 50,
          autovacuum_vacuum_cost_delay = 0
          );

CREATE OR REPLACE FUNCTION settle_pending_deposits(p_batch_limit INTEGER DEFAULT 100000)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
v_settled_count BIGINT := 0;
BEGIN
    -- Serialize batch settlement across replicas to eliminate row-level lock contention on balances
    PERFORM pg_advisory_xact_lock(778899);

    IF p_batch_limit IS NULL OR p_batch_limit <= 0 THEN
        p_batch_limit := 100000;
END IF;

WITH claimed AS (
    SELECT ctid
    FROM deposits
    WHERE is_settled = FALSE
    ORDER BY processed_at ASC
    LIMIT p_batch_limit
    FOR UPDATE SKIP LOCKED
            ),
            settled_rows AS (
        UPDATE deposits d
        SET is_settled = TRUE
        FROM claimed c
        WHERE d.ctid = c.ctid
            RETURNING d.user_id, d.currency, d.amount
            ),
            agg_balances AS (
        SELECT
            user_id,
            currency,
            SUM(amount) AS total_amount,
            COUNT(*) AS tx_count
        FROM settled_rows
        GROUP BY user_id, currency
        ORDER BY user_id ASC, currency ASC
            ),
            upserted AS (
        INSERT INTO balances (user_id, currency, balance, updated_at)
        SELECT user_id, currency, total_amount, NOW()
        FROM agg_balances
        ON CONFLICT (user_id, currency)
            DO UPDATE SET
            balance = balances.balance + EXCLUDED.balance,
            updated_at = NOW()
            )
SELECT COALESCE(SUM(tx_count), 0)::BIGINT
INTO v_settled_count
FROM agg_balances;

RETURN v_settled_count;
END;
$$;