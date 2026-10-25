package dbx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

func ConnectAndMigrate(ctx context.Context, databaseURL, migrationsPath string, logger *zap.Logger) (*sql.DB, error) {
	if databaseURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}

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
			zap.String("component", "dbx_postgres"),
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

	if migrationsPath != "" {
		if err := RunMigrations(databaseURL, migrationsPath, logger); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to run database migrations: %w", err)
		}
	}

	return db, nil
}

func RunMigrations(databaseURL, migrationsPath string, logger *zap.Logger) error {
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		m, err := migrate.New(migrationsPath, databaseURL)
		if err != nil {
			lastErr = fmt.Errorf("failed to create migrate instance: %w", err)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			continue
		}

		err = m.Up()
		_, _ = m.Close()

		if err == nil || errors.Is(err, migrate.ErrNoChange) {
			logger.Debug("Database migrations applied successfully")
			return nil
		}

		lastErr = fmt.Errorf("failed to apply up migrations: %w", err)
		logger.Warn("Migration lock contention or temporary failure; retrying",
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}

	return lastErr
}
