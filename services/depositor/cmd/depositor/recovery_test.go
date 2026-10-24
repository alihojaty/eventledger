package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/broker"
	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/alihojaty/reliable-deposit-processor/internal/metrics"
	"github.com/alihojaty/reliable-deposit-processor/internal/repository"
	"github.com/alihojaty/reliable-deposit-processor/internal/worker"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type flakyRepository struct {
	realRepo  *repository.PostgresRepository
	failAfter int32
	calls     atomic.Int32
}

func (f *flakyRepository) ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error {
	current := f.calls.Add(1)
	if current > f.failAfter {
		return errors.New("simulated crash / database connection lost before commit")
	}
	return f.realRepo.ProcessDeposit(ctx, event)
}

func (f *flakyRepository) GetBalance(ctx context.Context, userID int, currency string) (decimal.Decimal, error) {
	return f.realRepo.GetBalance(ctx, userID, currency)
}

func (f *flakyRepository) Close() error {
	return nil
}

func TestEndToEnd_CrashRecovery_ZeroDataLossAndZeroDoubleSpend(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/deposits_db?sslmode=disable"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	ctx := context.Background()
	nopLogger := zap.NewNop()

	realRepo, err := repository.NewPostgresRepository(ctx, dbURL, "file://../../migrations", nopLogger)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	lockConn, err := realRepo.DB().Conn(ctx)
	if err != nil {
		_ = realRepo.Close()
		t.Fatalf("failed to get lock connection: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "SELECT pg_advisory_lock(998877)"); err != nil {
		_ = lockConn.Close()
		_ = realRepo.Close()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}
	defer func() {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = realRepo.Close()
	}()

	_, err = realRepo.DB().ExecContext(ctx, "TRUNCATE TABLE deposits, balances RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}

	natsBroker, err := broker.NewNATSBroker(ctx, natsURL, nopLogger)
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}
	_ = natsBroker.JetStream().DeleteStream(ctx, broker.StreamDeposits)
	_ = natsBroker.JetStream().DeleteStream(ctx, broker.StreamDepositsDLQ)
	natsBroker.Close()

	natsBroker, err = broker.NewNATSBroker(ctx, natsURL, nopLogger)
	if err != nil {
		t.Fatalf("failed to reconnect to nats: %v", err)
	}
	defer natsBroker.Close()

	const (
		totalEvents  = 5
		targetUserID = 10
	)
	singleAmount := decimal.RequireFromString("100.00")
	expectedMid := decimal.RequireFromString("200.00")
	expectedTotal := decimal.RequireFromString("500.00")

	for i := 1; i <= totalEvents; i++ {
		ev := domain.DepositEvent{
			EventID:   fmt.Sprintf("evt-recovery-%d-%s", i, uuid.NewString()[:8]),
			UserID:    targetUserID,
			Currency:  domain.CurrencyUSDT,
			Amount:    singleAmount,
			CreatedAt: time.Now().UTC(),
		}
		payload, _ := json.Marshal(ev)
		if _, err := natsBroker.PublishDeposit(ctx, ev.EventID, payload); err != nil {
			t.Fatalf("failed to publish event %d: %v", i, err)
		}
	}

	flakyRepo := &flakyRepository{
		realRepo:  realRepo,
		failAfter: 2,
	}

	consumer1, err := natsBroker.CreateOrUpdateDepositConsumerWithAckWait(ctx, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var wg1 sync.WaitGroup
	proc1 := worker.NewProcessor(ctx, &wg1, flakyRepo, natsBroker, 300*time.Millisecond, nopLogger, metrics.NewCollector())
	group1 := worker.NewConsumerGroup(2, consumer1, proc1, nopLogger)

	if err := group1.Start(); err != nil {
		t.Fatalf("failed to start group1: %v", err)
	}

	midDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(midDeadline) {
		bal, _ := realRepo.GetBalance(ctx, targetUserID, domain.CurrencyUSDT)
		if bal.Equal(expectedMid) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	group1.DrainAll()
	wg1.Wait()

	midBalance, err := realRepo.GetBalance(ctx, targetUserID, domain.CurrencyUSDT)
	if err != nil {
		t.Fatalf("failed to query mid balance: %v", err)
	}
	if !midBalance.Equal(expectedMid) {
		t.Fatalf("expected balance of 200.00 after partial processing, got %s", midBalance.String())
	}

	var wg2 sync.WaitGroup
	collector2 := metrics.NewCollector()
	proc2 := worker.NewProcessor(ctx, &wg2, realRepo, natsBroker, 200*time.Millisecond, nopLogger, collector2)
	group2 := worker.NewConsumerGroup(3, consumer1, proc2, nopLogger)

	if err := group2.Start(); err != nil {
		t.Fatalf("failed to start recovery group2: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		bal, _ := realRepo.GetBalance(ctx, targetUserID, domain.CurrencyUSDT)
		if bal.Equal(expectedTotal) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	group2.DrainAll()
	wg2.Wait()

	finalBalance, err := realRepo.GetBalance(ctx, targetUserID, domain.CurrencyUSDT)
	if err != nil {
		t.Fatalf("failed to query final balance: %v", err)
	}
	if !finalBalance.Equal(expectedTotal) {
		t.Fatalf("CRASH RECOVERY FAILED: expected final balance %s, got %s",
			expectedTotal.String(), finalBalance.String())
	}
}
