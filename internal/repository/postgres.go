package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type DepositRepository interface {
	ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error
	GetBalance(ctx context.Context, userID int, currency string) (decimal.Decimal, error)
	Close() error
}

type PostgresRepository struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewPostgresRepository(ctx context.Context, databaseURL, migrationsPath string, logger *zap.Logger) (*PostgresRepository, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	var pingErr error
	for attempt := 1; attempt <= 10; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pingErr = db.PingContext(pingCtx)
		cancel()

		if pingErr == nil {
			break
		}

		logger.Warn("Temporary failure",
			zap.String("component", "postgres_repository"),
			zap.Int("attempt", attempt),
			zap.Error(pingErr),
		)

		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, fmt.Errorf("context canceled while waiting for database: %w", ctx.Err())
		case <-time.After(1 * time.Second):
		}
	}

	if pingErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database after retries: %w", pingErr)
	}

	logger.Info("Connected to Database")

	if err := runMigrations(databaseURL, migrationsPath, logger); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to run database migrations: %w", err)
	}

	return &PostgresRepository{
		db:     db,
		logger: logger,
	}, nil
}

func runMigrations(databaseURL, migrationsPath string, logger *zap.Logger) error {
	m, err := migrate.New(migrationsPath, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply up migrations: %w", err)
	}

	logger.Debug("Database migrations applied successfully")
	return nil
}

func (r *PostgresRepository) ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	const insertDepositQuery = `
		INSERT INTO deposits (event_id, user_id, currency, amount, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING
	`

	res, err := tx.ExecContext(
		ctx,
		insertDepositQuery,
		event.EventID,
		event.UserID,
		event.Currency,
		event.Amount,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert deposit event %s: %w", event.EventID, err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for event %s: %w", event.EventID, err)
	}

	if rowsAffected == 0 {
		return domain.ErrDuplicateEvent
	}

	const upsertBalanceQuery = `
		INSERT INTO balances (user_id, currency, balance, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (user_id, currency)
		DO UPDATE SET
			balance = balances.balance + EXCLUDED.balance,
			updated_at = NOW()
	`

	_, err = tx.ExecContext(
		ctx,
		upsertBalanceQuery,
		event.UserID,
		event.Currency,
		event.Amount,
	)
	if err != nil {
		return fmt.Errorf("failed to update balance for user %d (%s): %w", event.UserID, event.Currency, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for event %s: %w", event.EventID, err)
	}

	return nil
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
	if r.db != nil {
		err := r.db.Close()
		r.logger.Info("Database connection closed")
		return err
	}
	return nil
}

// UserBalanceRow ساختار نمایشی موجودی هر کاربر برای داشبورد گرافیکی است.
type UserBalanceRow struct {
	UserID    int    `json:"user_id"`
	USDT      string `json:"usdt"`
	BTC       string `json:"btc"`
	IRT       string `json:"irt"`
	UpdatedAt string `json:"updated_at"`
}

// GetAllUserBalances موجودی تمام کاربران را به تفکیک ارز از دیتابیس استخراج می‌کند.
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

// ProcessedDepositItem اطلاعات هر پیام پردازش‌شده به همراه شماره ردیف (Sequence) آن در دیتابیس است.
type ProcessedDepositItem struct {
	OrderNum    int    `json:"order_num"`
	EventID     string `json:"event_id"`
	UserID      int    `json:"user_id"`
	Currency    string `json:"currency"`
	Amount      string `json:"amount"`
	ProcessedAt string `json:"processed_at"`
}

// GetTotalProcessedCount تعداد کل رویدادهای واریزی که تا این لحظه در دیتابیس Commit شده‌اند را برمی‌گرداند.
func (r *PostgresRepository) GetTotalProcessedCount(ctx context.Context) (uint64, error) {
	var count uint64
	err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM deposits").Scan(&count)
	return count, err
}

// GetRecentProcessedDeposits آخرین رویدادهای پردازش‌شده را به ترتیب زمان ثبت برمی‌گرداند
// تا در داشبورد دقیقاً ببینیم برنامه پس از Restart از کدام شماره پیام کار را ادامه داده است.
func (r *PostgresRepository) GetRecentProcessedDeposits(ctx context.Context, limit int) ([]ProcessedDepositItem, error) {
	const query = `
		WITH numbered AS (
			SELECT 
				ROW_NUMBER() OVER (ORDER BY processed_at ASC, event_id ASC) AS order_num,
				event_id,
				user_id,
				currency,
				ROUND(amount, 4)::TEXT AS amount,
				TO_CHAR(processed_at, 'HH24:MI:SS.MS') AS processed_at
			FROM deposits
		)
		SELECT order_num, event_id, user_id, currency, amount, processed_at
		FROM numbered
		ORDER BY order_num DESC
		LIMIT $1
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
