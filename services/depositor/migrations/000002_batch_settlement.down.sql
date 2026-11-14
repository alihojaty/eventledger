DROP FUNCTION IF EXISTS settle_pending_deposits(INTEGER);
DROP INDEX IF EXISTS idx_deposits_unsettled_pending;
ALTER TABLE deposits DROP COLUMN IF EXISTS is_settled;