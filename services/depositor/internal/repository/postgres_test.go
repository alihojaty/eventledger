package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func setupTestRepo(t *testing.T) *PostgresRepository {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/deposits_db?sslmode=disable"
	}

	migrationsPath := "file://../../migrations"
	ctx := context.Background()
	nopLogger := zap.NewNop()

	repo, err := NewPostgresRepository(ctx, dbURL, migrationsPath, nopLogger)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}

	lockConn, err := repo.DB().Conn(ctx)
	if err != nil {
		_ = repo.Close()
		t.Fatalf("failed to acquire dedicated lock connection: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "SELECT pg_advisory_lock(998877)"); err != nil {
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to acquire postgres advisory lock: %v", err)
	}

	_, err = repo.DB().ExecContext(ctx, "TRUNCATE TABLE deposits, balances RESTART IDENTITY CASCADE")
	if err != nil {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to truncate tables before test: %v", err)
	}

	t.Cleanup(func() {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
	})

	return repo
}

func TestProcessDeposit_DuplicateEvent_Idempotency(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	amount := decimal.RequireFromString("125.50")
	event := &domain.DepositEvent{
		EventID:   "evt-dup-test-001",
		UserID:    7,
		Currency:  domain.CurrencyUSDT,
		Amount:    amount,
		CreatedAt: time.Now().UTC(),
	}

	if err := repo.ProcessDeposit(ctx, event); err != nil {
		t.Fatalf("expected first deposit to succeed, got error: %v", err)
	}

	err := repo.ProcessDeposit(ctx, event)
	if !errors.Is(err, domain.ErrDuplicateEvent) {
		t.Fatalf("expected domain.ErrDuplicateEvent on duplicate event, got: %v", err)
	}

	balance, err := repo.GetBalance(ctx, 7, domain.CurrencyUSDT)
	if err != nil {
		t.Fatalf("failed to get balance: %v", err)
	}

	if !balance.Equal(amount) {
		t.Errorf("expected balance to be %s, but got %s", amount.String(), balance.String())
	}
}

func TestProcessDeposit_1000ConcurrentDeposits_NoLostUpdates(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	const (
		concurrentDeposits = 1000
		targetUserID       = 7
		targetCurrency     = domain.CurrencyBTC
	)

	singleAmount := decimal.RequireFromString("10.12345678")
	expectedFinalBalance := singleAmount.Mul(decimal.NewFromInt(concurrentDeposits))

	var wg sync.WaitGroup
	errCh := make(chan error, concurrentDeposits)
	startCh := make(chan struct{})

	for i := 1; i <= concurrentDeposits; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			<-startCh

			ev := &domain.DepositEvent{
				EventID:   fmt.Sprintf("evt-concurrent-%04d", seq),
				UserID:    targetUserID,
				Currency:  targetCurrency,
				Amount:    singleAmount,
				CreatedAt: time.Now().UTC(),
			}

			if err := repo.ProcessDeposit(ctx, ev); err != nil {
				errCh <- fmt.Errorf("goroutine %d failed: %w", seq, err)
			}
		}(i)
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error during concurrent deposits: %v", err)
	}

	actualBalance, err := repo.GetBalance(ctx, targetUserID, targetCurrency)
	if err != nil {
		t.Fatalf("failed to fetch final balance: %v", err)
	}

	if !actualBalance.Equal(expectedFinalBalance) {
		t.Fatalf("LOST UPDATE DETECTED! Expected final balance %s, but got %s",
			expectedFinalBalance.String(), actualBalance.String())
	}

	t.Logf("SUCCESS: 1000 concurrent deposits processed accurately. Final balance: %s %s",
		actualBalance.String(), targetCurrency)
}
