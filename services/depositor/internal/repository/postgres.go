package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/dbx"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	maxBatchSize         = 256
	batchWaitDelay       = 250 * time.Microsecond
	batchFlushers        = 4
	batchQueryTimeout    = 10 * time.Second
	settlementBatchLimit = 100_000
)

type DepositRepository interface {
	ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error
	GetBalance(ctx context.Context, userID int, currency string) (decimal.Decimal, error)
	Close() error
}

type batchRequest struct {
	ctx   context.Context
	event *domain.DepositEvent
	errCh chan error
}

type flusherWorkspace struct {
	activeBatch      []*batchRequest
	uniqueEventIDs   []string
	userIDs          []int64
	currencies       []string
	amounts          []string
	createdAts       []time.Time
	duplicateInBatch []bool
	seenInBatch      map[string]int
	insertedSet      map[string]struct{}
}

func newFlusherWorkspace() *flusherWorkspace {
	return &flusherWorkspace{
		activeBatch:      make([]*batchRequest, 0, maxBatchSize),
		uniqueEventIDs:   make([]string, 0, maxBatchSize),
		userIDs:          make([]int64, 0, maxBatchSize),
		currencies:       make([]string, 0, maxBatchSize),
		amounts:          make([]string, 0, maxBatchSize),
		createdAts:       make([]time.Time, 0, maxBatchSize),
		duplicateInBatch: make([]bool, 0, maxBatchSize),
		seenInBatch:      make(map[string]int, maxBatchSize),
		insertedSet:      make(map[string]struct{}, maxBatchSize),
	}
}

func (w *flusherWorkspace) reset() {
	clear(w.activeBatch)
	w.activeBatch = w.activeBatch[:0]
	w.uniqueEventIDs = w.uniqueEventIDs[:0]
	w.userIDs = w.userIDs[:0]
	w.currencies = w.currencies[:0]
	w.amounts = w.amounts[:0]
	w.createdAts = w.createdAts[:0]
	w.duplicateInBatch = w.duplicateInBatch[:0]
	clear(w.seenInBatch)
	clear(w.insertedSet)
}

type PostgresRepository struct {
	db        *sql.DB
	logger    *zap.Logger
	batchCh   chan *batchRequest
	collectMu sync.Mutex
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

func NewPostgresRepository(ctx context.Context, databaseURL, migrationsPath string, logger *zap.Logger) (*PostgresRepository, error) {
	db, err := dbx.ConnectAndMigrate(ctx, databaseURL, migrationsPath, logger)
	if err != nil {
		return nil, err
	}
	if err := ensureBatchSettlementSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ensure batch settlement schema: %w", err)
	}

	r := &PostgresRepository{
		db:      db,
		logger:  logger,
		batchCh: make(chan *batchRequest, 4096),
		stopCh:  make(chan struct{}),
	}
	for i := 0; i < batchFlushers; i++ {
		r.wg.Add(1)
		go r.batchFlusherLoop()
	}
	return r, nil
}

func ensureBatchSettlementSchema(ctx context.Context, db *sql.DB) error {
	const ddl = `
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
	`
	schemaCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	conn, err := db.Conn(schemaCtx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(schemaCtx, "SELECT pg_advisory_lock(887766)"); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(887766)")
	}()

	_, err = conn.ExecContext(schemaCtx, ddl)
	return err
}

func (r *PostgresRepository) ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	req := &batchRequest{
		ctx:   ctx,
		event: event,
		errCh: make(chan error, 1),
	}

	select {
	case r.batchCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return errors.New("repository is closing")
	}

	return <-req.errCh
}

// collectNextBatch serializes the batch-gathering window across flusher goroutines so one
// flusher packs up to 256 items into a single dense SQL query instead of 4 flushers
// splitting incoming requests into tiny fragments.
func (r *PostgresRepository) collectNextBatch(batch []*batchRequest, timer *time.Timer) ([]*batchRequest, bool) {
	r.collectMu.Lock()
	defer r.collectMu.Unlock()

	select {
	case <-r.stopCh:
		for len(batch) < maxBatchSize {
			select {
			case req := <-r.batchCh:
				batch = append(batch, req)
			default:
				return batch, false
			}
		}
		return batch, true
	case first := <-r.batchCh:
		batch = append(batch[:0], first)
	}

drainFast:
	for len(batch) < maxBatchSize {
		select {
		case next := <-r.batchCh:
			batch = append(batch, next)
		default:
			break drainFast
		}
	}

	if len(batch) < 64 {
		timer.Reset(batchWaitDelay)
	waitMore:
		for len(batch) < maxBatchSize {
			select {
			case next := <-r.batchCh:
				batch = append(batch, next)
			case <-timer.C:
				break waitMore
			case <-r.stopCh:
				break waitMore
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	return batch, true
}

func (r *PostgresRepository) batchFlusherLoop() {
	defer r.wg.Done()

	ws := newFlusherWorkspace()
	batch := make([]*batchRequest, 0, maxBatchSize)
	timer := time.NewTimer(batchWaitDelay)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		var keepRunning bool
		batch, keepRunning = r.collectNextBatch(batch, timer)
		if len(batch) > 0 {
			r.executeBatch(batch, ws)
			clear(batch)
			batch = batch[:0]
		}
		if !keepRunning {
			return
		}
	}
}

func newBatchQueryContext(activeBatch []*batchRequest, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	doneCh := make(chan struct{})
	exitedCh := make(chan struct{})

	go func() {
		defer close(exitedCh)
		for _, req := range activeBatch {
			if req.ctx == nil {
				return
			}
			select {
			case <-req.ctx.Done():
			case <-doneCh:
				return
			case <-ctx.Done():
				return
			}
		}
		cancel()
	}()

	return ctx, func() {
		close(doneCh)
		cancel()
		<-exitedCh
	}
}

func (r *PostgresRepository) executeBatch(batch []*batchRequest, ws *flusherWorkspace) {
	if len(batch) == 0 {
		return
	}

	ws.reset()

	for _, req := range batch {
		if req.ctx != nil && req.ctx.Err() != nil {
			req.errCh <- req.ctx.Err()
			continue
		}
		ws.activeBatch = append(ws.activeBatch, req)
	}

	n := len(ws.activeBatch)
	if n == 0 {
		return
	}

	for i, req := range ws.activeBatch {
		ev := req.event
		if _, exists := ws.seenInBatch[ev.EventID]; exists {
			ws.duplicateInBatch = append(ws.duplicateInBatch, true)
			continue
		}
		ws.seenInBatch[ev.EventID] = i
		ws.duplicateInBatch = append(ws.duplicateInBatch, false)
		ws.uniqueEventIDs = append(ws.uniqueEventIDs, ev.EventID)
		ws.userIDs = append(ws.userIDs, int64(ev.UserID))
		ws.currencies = append(ws.currencies, ev.Currency)
		ws.amounts = append(ws.amounts, ev.Amount.String())
		ws.createdAts = append(ws.createdAts, ev.CreatedAt)
	}

	const insertDepositsQuery = `
		INSERT INTO deposits (event_id, user_id, currency, amount, is_settled, created_at)
		SELECT
			UNNEST($1::text[]),
			UNNEST($2::int8[])::int4,
			UNNEST($3::text[]),
			UNNEST($4::text[])::numeric(36, 18),
			FALSE,
			UNNEST($5::timestamptz[])
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id
	`

	ctx, cancel := newBatchQueryContext(ws.activeBatch, batchQueryTimeout)
	defer cancel()

	rows, err := r.db.QueryContext(
		ctx,
		insertDepositsQuery,
		ws.uniqueEventIDs,
		ws.userIDs,
		ws.currencies,
		ws.amounts,
		ws.createdAts,
	)
	if err != nil {
		for _, req := range ws.activeBatch {
			req.errCh <- fmt.Errorf("failed to execute deposit ingestion batch: %w", err)
		}
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			for _, req := range ws.activeBatch {
				req.errCh <- fmt.Errorf("failed to scan inserted event_id: %w", scanErr)
			}
			return
		}
		ws.insertedSet[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		for _, req := range ws.activeBatch {
			req.errCh <- fmt.Errorf("error reading deposit ingestion result: %w", err)
		}
		return
	}

	for i, req := range ws.activeBatch {
		if ws.duplicateInBatch[i] {
			req.errCh <- domain.ErrDuplicateEvent
			continue
		}
		if _, inserted := ws.insertedSet[req.event.EventID]; inserted {
			req.errCh <- nil
		} else {
			req.errCh <- domain.ErrDuplicateEvent
		}
	}
}

func (r *PostgresRepository) SettlePendingDeposits(ctx context.Context) (int64, error) {
	var totalSettled int64

	for {
		if err := ctx.Err(); err != nil {
			return totalSettled, err
		}

		qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		var batchSettled int64
		err := r.db.QueryRowContext(qCtx, "SELECT settle_pending_deposits($1)", settlementBatchLimit).Scan(&batchSettled)
		cancel()

		if err != nil {
			return totalSettled, fmt.Errorf("failed to execute settle_pending_deposits PL/pgSQL function: %w", err)
		}

		totalSettled += batchSettled
		if batchSettled < int64(settlementBatchLimit) {
			break
		}
	}

	return totalSettled, nil
}

func (r *PostgresRepository) GetBalance(ctx context.Context, userID int, currency string) (decimal.Decimal, error) {
	const query = `
		SELECT balance
		FROM balances
		WHERE user_id = $1 AND currency = $2
	`

	var balance decimal.Decimal
	err := r.db.QueryRowContext(ctx, query, userID, currency).Scan(&balance)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decimal.Zero, nil
		}
		return decimal.Zero, fmt.Errorf("failed to query balance: %w", err)
	}

	return balance, nil
}

func (r *PostgresRepository) DB() *sql.DB {
	return r.db
}

func (r *PostgresRepository) Close() error {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()

	if r.db != nil {
		err := r.db.Close()
		r.logger.Info("Database connection closed")
		return err
	}
	return nil
}

type UserBalanceRow struct {
	UserID    int    `json:"user_id"`
	USDT      string `json:"usdt"`
	BTC       string `json:"btc"`
	IRT       string `json:"irt"`
	UpdatedAt string `json:"updated_at"`
}

func (r *PostgresRepository) GetAllUserBalances(ctx context.Context) ([]UserBalanceRow, error) {
	const query = `
		SELECT 
			user_id,
			COALESCE(MAX(CASE WHEN currency = 'USDT' THEN balance END), 0)::TEXT AS usdt,
			COALESCE(MAX(CASE WHEN currency = 'BTC'  THEN balance END), 0)::TEXT AS btc,
			COALESCE(MAX(CASE WHEN currency = 'IRT'  THEN balance END), 0)::TEXT AS irt,
			TO_CHAR(MAX(updated_at), 'HH24:MI:SS') AS last_update
		FROM balances
		GROUP BY user_id
		ORDER BY user_id ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []UserBalanceRow
	for rows.Next() {
		var item UserBalanceRow
		if err := rows.Scan(&item.UserID, &item.USDT, &item.BTC, &item.IRT, &item.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

type ProcessedDepositItem struct {
	OrderNum    int    `json:"order_num"`
	EventID     string `json:"event_id"`
	UserID      int    `json:"user_id"`
	Currency    string `json:"currency"`
	Amount      string `json:"amount"`
	ProcessedAt string `json:"processed_at"`
}

func (r *PostgresRepository) GetTotalProcessedCount(ctx context.Context) (uint64, error) {
	const fastCountQuery = `
		SELECT (
			COALESCE((SELECT SUM(settled_count) FROM ledger_settled_summary), 0) +
			COALESCE((SELECT COUNT(*) FROM deposits WHERE is_settled = FALSE), 0)
		)::BIGINT
	`
	var count uint64
	err := r.db.QueryRowContext(ctx, fastCountQuery).Scan(&count)
	return count, err
}

func (r *PostgresRepository) GetRecentProcessedDeposits(ctx context.Context, limit int) ([]ProcessedDepositItem, error) {
	const query = `
		WITH recent AS (
			SELECT
				event_id,
				user_id,
				currency,
				TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM amount::TEXT)) AS amount,
				TO_CHAR(processed_at, 'HH24:MI:SS.MS') AS processed_at,
				processed_at AS raw_processed_at
			FROM deposits
			ORDER BY event_id DESC
			LIMIT $1
		)
		SELECT
			ROW_NUMBER() OVER (ORDER BY raw_processed_at DESC, event_id DESC)::INT AS order_num,
			event_id,
			user_id,
			currency,
			amount,
			processed_at
		FROM recent
		ORDER BY order_num ASC
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []ProcessedDepositItem
	for rows.Next() {
		var item ProcessedDepositItem
		if err := rows.Scan(&item.OrderNum, &item.EventID, &item.UserID, &item.Currency, &item.Amount, &item.ProcessedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *PostgresRepository) ReconcileLedger(ctx context.Context) ([]domain.LedgerDiscrepancy, error) {
	const query = `
		WITH expected AS (
			SELECT
				user_id,
				currency,
				settled_amount AS expected_amount
			FROM ledger_settled_summary
			WHERE settled_amount > 0
		)
		SELECT
			COALESCE(e.user_id, b.user_id) AS user_id,
			COALESCE(e.currency, b.currency) AS currency,
			COALESCE(e.expected_amount, 0) AS expected_balance,
			COALESCE(b.balance, 0) AS actual_balance,
			(COALESCE(b.balance, 0) - COALESCE(e.expected_amount, 0)) AS difference
		FROM expected e
		FULL OUTER JOIN balances b
			ON b.user_id = e.user_id
		   AND b.currency = e.currency
		WHERE
			COALESCE(e.expected_amount, 0)
			IS DISTINCT FROM
			COALESCE(b.balance, 0)
		ORDER BY user_id ASC, currency ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute ledger reconciliation query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var discrepancies []domain.LedgerDiscrepancy
	for rows.Next() {
		var d domain.LedgerDiscrepancy
		if err := rows.Scan(
			&d.UserID,
			&d.Currency,
			&d.ExpectedBalance,
			&d.ActualBalance,
			&d.Difference,
		); err != nil {
			return nil, fmt.Errorf("failed to scan ledger discrepancy row: %w", err)
		}
		discrepancies = append(discrepancies, d)
	}

	return discrepancies, rows.Err()
}
