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
	maxBatchSize         = 128
	batchWaitDelay       = 200 * time.Microsecond
	batchFlushers        = 4
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

type PostgresRepository struct {
	db       *sql.DB
	logger   *zap.Logger
	batchCh  chan *batchRequest
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewPostgresRepository(ctx context.Context, databaseURL, migrationsPath string, logger *zap.Logger) (*PostgresRepository, error) {
	repairDirtyMigrationsIfNeeded(ctx, databaseURL)

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
		batchCh: make(chan *batchRequest, 2048),
		stopCh:  make(chan struct{}),
	}
	for i := 0; i < batchFlushers; i++ {
		r.wg.Add(1)
		go r.batchFlusherLoop()
	}
	return r, nil
}

func repairDirtyMigrationsIfNeeded(ctx context.Context, databaseURL string) {
	for _, drv := range []string{"pgx", "postgres"} {
		db, err := sql.Open(drv, databaseURL)
		if err != nil {
			continue
		}
		cCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, _ = db.ExecContext(cCtx, `UPDATE schema_migrations SET dirty = FALSE WHERE dirty = TRUE`)
		cancel()
		_ = db.Close()
	}
}

func ensureBatchSettlementSchema(ctx context.Context, db *sql.DB) error {
	const ddl = `
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

	select {
	case err := <-req.errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *PostgresRepository) batchFlusherLoop() {
	defer r.wg.Done()

	batch := make([]*batchRequest, 0, maxBatchSize)
	timer := time.NewTimer(batchWaitDelay)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		select {
		case <-r.stopCh:
			for {
				select {
				case req := <-r.batchCh:
					batch = append(batch, req)
					if len(batch) >= maxBatchSize {
						r.executeBatch(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						r.executeBatch(batch)
					}
					return
				}
			}
		case first := <-r.batchCh:
			batch = append(batch[:0], first)

		drainFast:
			for len(batch) < maxBatchSize {
				select {
				case next := <-r.batchCh:
					batch = append(batch, next)
				default:
					break drainFast
				}
			}

			if len(batch) < 8 {
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

			r.executeBatch(batch)
		}
	}
}

func (r *PostgresRepository) executeBatch(batch []*batchRequest) {
	if len(batch) == 0 {
		return
	}

	activeBatch := make([]*batchRequest, 0, len(batch))
	for _, req := range batch {
		if req.ctx != nil && req.ctx.Err() != nil {
			req.errCh <- req.ctx.Err()
			continue
		}
		activeBatch = append(activeBatch, req)
	}

	n := len(activeBatch)
	if n == 0 {
		return
	}

	uniqueEventIDs := make([]string, 0, n)
	userIDs := make([]int64, 0, n)
	currencies := make([]string, 0, n)
	amounts := make([]string, 0, n)
	createdAts := make([]time.Time, 0, n)

	seenInBatch := make(map[string]int, n)
	duplicateInBatch := make([]bool, n)

	for i, req := range activeBatch {
		ev := req.event
		if _, exists := seenInBatch[ev.EventID]; exists {
			duplicateInBatch[i] = true
			continue
		}
		seenInBatch[ev.EventID] = i
		uniqueEventIDs = append(uniqueEventIDs, ev.EventID)
		userIDs = append(userIDs, int64(ev.UserID))
		currencies = append(currencies, ev.Currency)
		amounts = append(amounts, ev.Amount.String())
		createdAts = append(createdAts, ev.CreatedAt)
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

	parentCtx := activeBatch[0].ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, insertDepositsQuery, uniqueEventIDs, userIDs, currencies, amounts, createdAts)
	if err != nil {
		for _, req := range activeBatch {
			req.errCh <- fmt.Errorf("failed to execute deposit ingestion batch: %w", err)
		}
		return
	}
	defer func() { _ = rows.Close() }()

	insertedSet := make(map[string]struct{}, len(uniqueEventIDs))
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr == nil {
			insertedSet[id] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		for _, req := range activeBatch {
			req.errCh <- fmt.Errorf("error reading deposit ingestion result: %w", err)
		}
		return
	}

	for i, req := range activeBatch {
		if duplicateInBatch[i] {
			req.errCh <- domain.ErrDuplicateEvent
			continue
		}
		if _, inserted := insertedSet[req.event.EventID]; inserted {
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
	var count uint64
	err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deposits").Scan(&count)
	return count, err
}

func (r *PostgresRepository) GetRecentProcessedDeposits(ctx context.Context, limit int) ([]ProcessedDepositItem, error) {
	const query = `
		WITH recent AS (
			SELECT
				event_id,
				user_id,
				currency,
				ROUND(amount, 4)::TEXT AS amount,
				TO_CHAR(processed_at, 'HH24:MI:SS.MS') AS processed_at,
				processed_at AS raw_processed_at
			FROM deposits
			ORDER BY processed_at DESC, event_id DESC
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
				SUM(amount) AS expected_amount
			FROM deposits
			WHERE is_settled = TRUE
			GROUP BY user_id, currency
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
