DROP FUNCTION IF EXISTS settle_pending_deposits(INTEGER);
DROP TRIGGER IF EXISTS trg_deposits_truncate ON deposits;
DROP FUNCTION IF EXISTS trg_truncate_ledger_summary();
DROP INDEX IF EXISTS idx_deposits_unsettled_pending;
DROP TABLE IF EXISTS ledger_settled_summary;
ALTER TABLE deposits DROP COLUMN IF EXISTS is_settled;