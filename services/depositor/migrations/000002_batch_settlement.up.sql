ALTER TABLE deposits
    ADD COLUMN IF NOT EXISTS is_settled BOOLEAN NOT NULL DEFAULT FALSE;

DROP INDEX IF EXISTS idx_deposits_processed_at_desc;

ALTER TABLE deposits SET (
    fillfactor = 75,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 5000,
    autovacuum_analyze_scale_factor = 0.02
    );

ALTER TABLE balances SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
    );

CREATE TABLE IF NOT EXISTS ledger_settled_summary (
                                                      user_id        INTEGER NOT NULL,
                                                      currency       VARCHAR(10) NOT NULL,
    settled_count  BIGINT NOT NULL DEFAULT 0,
    settled_amount NUMERIC(36, 18) NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, currency)
    ) WITH (fillfactor = 70);

DO $$
BEGIN
    PERFORM pg_advisory_xact_lock(778899);
    IF NOT EXISTS (SELECT 1 FROM ledger_settled_summary LIMIT 1) THEN
        INSERT INTO ledger_settled_summary (user_id, currency, settled_count, settled_amount)
SELECT
    user_id,
    currency,
    COUNT(*)::BIGINT,
    COALESCE(SUM(amount), 0)
FROM deposits
WHERE is_settled = TRUE
GROUP BY user_id, currency
    ON CONFLICT (user_id, currency) DO UPDATE SET
    settled_count  = EXCLUDED.settled_count,
                                           settled_amount = EXCLUDED.settled_amount;
END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'deposits'
          AND indexname = 'idx_deposits_unsettled_pending'
          AND (indexdef ILIKE '%processed_at%' OR indexdef ILIKE '%INCLUDE%')
    ) THEN
DROP INDEX idx_deposits_unsettled_pending;
END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_deposits_unsettled_pending
    ON deposits (user_id, currency)
    WITH (fillfactor = 90)
    WHERE is_settled = FALSE;

CREATE OR REPLACE FUNCTION trg_truncate_ledger_summary()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
TRUNCATE TABLE ledger_settled_summary;
RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_deposits_truncate ON deposits;
CREATE TRIGGER trg_deposits_truncate
    AFTER TRUNCATE ON deposits
    FOR EACH STATEMENT
    EXECUTE FUNCTION trg_truncate_ledger_summary();

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
            COUNT(*)::BIGINT AS tx_count
        FROM settled_rows
        GROUP BY user_id, currency
        ORDER BY user_id ASC, currency ASC
            ),
            upserted_balances AS (
        INSERT INTO balances (user_id, currency, balance, updated_at)
        SELECT user_id, currency, total_amount, NOW()
        FROM agg_balances
        ON CONFLICT (user_id, currency)
            DO UPDATE SET
            balance = balances.balance + EXCLUDED.balance,
            updated_at = NOW()
            ),
            upserted_summary AS (
        INSERT INTO ledger_settled_summary (user_id, currency, settled_count, settled_amount)
        SELECT user_id, currency, tx_count, total_amount
        FROM agg_balances
        ON CONFLICT (user_id, currency)
            DO UPDATE SET
            settled_count  = ledger_settled_summary.settled_count + EXCLUDED.settled_count,
            settled_amount = ledger_settled_summary.settled_amount + EXCLUDED.settled_amount
            )
SELECT COALESCE(SUM(tx_count), 0)::BIGINT
INTO v_settled_count
FROM agg_balances;

RETURN v_settled_count;
END;
$$;