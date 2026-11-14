ALTER TABLE deposits
    ADD COLUMN IF NOT EXISTS is_settled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE deposits SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_analyze_scale_factor = 0.02
    );

ALTER TABLE balances SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
    );

CREATE INDEX IF NOT EXISTS idx_deposits_unsettled_pending
    ON deposits (processed_at ASC)
    WITH (fillfactor = 90)
    WHERE is_settled = FALSE;

CREATE OR REPLACE FUNCTION settle_pending_deposits(p_batch_limit INTEGER DEFAULT 100000)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
v_settled_count BIGINT := 0;
BEGIN
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