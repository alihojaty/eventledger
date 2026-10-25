package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
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

func TestProcessDeposit_ConcurrentDuplicateEvent_ExactlyOnceEffect(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	const goroutines = 50
	amount := decimal.RequireFromString("250.75")
	sharedEvent := &domain.DepositEvent{
		EventID:   "evt-concurrent-dup-001",
		UserID:    12,
		Currency:  domain.CurrencyUSDT,
		Amount:    amount,
		CreatedAt: time.Now().UTC(),
	}

	var wg sync.WaitGroup
	startCh := make(chan struct{})
	var successCount atomic.Int32
	var dupCount atomic.Int32
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh

			err := repo.ProcessDeposit(ctx, sharedEvent)
			if err == nil {
				successCount.Add(1)
			} else if errors.Is(err, domain.ErrDuplicateEvent) {
				dupCount.Add(1)
			} else {
				errCh <- err
			}
		}()
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected database error during concurrent duplicate test: %v", err)
	}

	if successCount.Load() != 1 {
		t.Fatalf("expected exactly 1 successful insert, got %d", successCount.Load())
	}
	if dupCount.Load() != goroutines-1 {
		t.Fatalf("expected %d duplicate detections, got %d", goroutines-1, dupCount.Load())
	}

	balance, err := repo.GetBalance(ctx, 12, domain.CurrencyUSDT)
	if err != nil {
		t.Fatalf("failed to get balance: %v", err)
	}
	if !balance.Equal(amount) {
		t.Fatalf("expected balance %s, got %s", amount.String(), balance.String())
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

func TestProcessDeposit_UserAndCurrencyIsolation(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	events := []*domain.DepositEvent{
		{EventID: "iso-1", UserID: 7, Currency: domain.CurrencyUSDT, Amount: decimal.RequireFromString("100.00"), CreatedAt: time.Now().UTC()},
		{EventID: "iso-2", UserID: 7, Currency: domain.CurrencyBTC, Amount: decimal.RequireFromString("0.50000000"), CreatedAt: time.Now().UTC()},
		{EventID: "iso-3", UserID: 8, Currency: domain.CurrencyUSDT, Amount: decimal.RequireFromString("300.00"), CreatedAt: time.Now().UTC()},
	}

	for _, ev := range events {
		if err := repo.ProcessDeposit(ctx, ev); err != nil {
			t.Fatalf("failed to process event %s: %v", ev.EventID, err)
		}
	}

	u7USDT, _ := repo.GetBalance(ctx, 7, domain.CurrencyUSDT)
	u7BTC, _ := repo.GetBalance(ctx, 7, domain.CurrencyBTC)
	u8USDT, _ := repo.GetBalance(ctx, 8, domain.CurrencyUSDT)
	u8BTC, _ := repo.GetBalance(ctx, 8, domain.CurrencyBTC)

	if !u7USDT.Equal(decimal.RequireFromString("100.00")) {
		t.Errorf("user 7 USDT mismatch: got %s", u7USDT)
	}
	if !u7BTC.Equal(decimal.RequireFromString("0.50000000")) {
		t.Errorf("user 7 BTC mismatch: got %s", u7BTC)
	}
	if !u8USDT.Equal(decimal.RequireFromString("300.00")) {
		t.Errorf("user 8 USDT mismatch: got %s", u8USDT)
	}
	if !u8BTC.Equal(decimal.Zero) {
		t.Errorf("user 8 BTC expected 0, got %s", u8BTC)
	}
}
