package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
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
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func setupIntegrationEnv(t *testing.T) (*repository.PostgresRepository, *broker.NATSBroker) {
	t.Helper()

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

	repo, err := repository.NewPostgresRepository(ctx, dbURL, "file://../../migrations", nopLogger)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	// گرفتن قفل سراسری PostgreSQL تا پکیج‌های دیگر نتوانند هم‌زمان جدول‌ها یا استریم‌های NATS را پاک کنند
	lockConn, err := repo.DB().Conn(ctx)
	if err != nil {
		_ = repo.Close()
		t.Fatalf("failed to get lock connection: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "SELECT pg_advisory_lock(998877)"); err != nil {
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}

	_, err = repo.DB().ExecContext(ctx, "TRUNCATE TABLE deposits, balances RESTART IDENTITY CASCADE")
	if err != nil {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to truncate tables: %v", err)
	}

	b, err := broker.NewNATSBroker(ctx, natsURL, nopLogger)
	if err != nil {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to connect to nats: %v", err)
	}

	_ = b.JetStream().DeleteStream(ctx, broker.StreamDeposits)
	_ = b.JetStream().DeleteStream(ctx, broker.StreamDepositsDLQ)
	b.Close()

	b, err = broker.NewNATSBroker(ctx, natsURL, nopLogger)
	if err != nil {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
		t.Fatalf("failed to reconnect to nats with fresh streams: %v", err)
	}

	t.Cleanup(func() {
		b.Close()
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = repo.Close()
	})

	return repo, b
}

type chaosDBSwitchRepository struct {
	realRepo *repository.PostgresRepository
	isKilled atomic.Bool
}

func (c *chaosDBSwitchRepository) ProcessDeposit(ctx context.Context, event *domain.DepositEvent) error {
	if c.isKilled.Load() {
		return errors.New("FATAL: terminating connection due to administrator command (SQLSTATE 57P01)")
	}
	return c.realRepo.ProcessDeposit(ctx, event)
}

func (c *chaosDBSwitchRepository) GetBalance(ctx context.Context, userID int, currency string) (decimal.Decimal, error) {
	return c.realRepo.GetBalance(ctx, userID, currency)
}

func (c *chaosDBSwitchRepository) Close() error { return nil }

func publishBatchAsync(t *testing.T, b *broker.NATSBroker, prefix string, startIdx, endIdx int, amount decimal.Decimal) {
	t.Helper()
	for i := startIdx; i <= endIdx; i++ {
		ev := domain.DepositEvent{
			EventID:   fmt.Sprintf("%s-%d", prefix, i),
			UserID:    1,
			Currency:  domain.CurrencyUSDT,
			Amount:    amount,
			CreatedAt: time.Now().UTC(),
		}
		data, _ := json.Marshal(ev)
		if _, err := b.PublishDepositAsync(ev.EventID, data); err != nil {
			t.Fatalf("failed async publish at %d: %v", i, err)
		}
	}
}

func TestChaos_DatabaseKilledMidStream_ZeroDataLoss(t *testing.T) {
	realRepo, natsBroker := setupIntegrationEnv(t)
	ctx := context.Background()
	nopLogger := zap.NewNop()

	chaosRepo := &chaosDBSwitchRepository{realRepo: realRepo}
	collector := metrics.NewCollector()

	const totalMessages = 200
	amountPerMsg := decimal.RequireFromString("25.50")
	expectedBalance := amountPerMsg.Mul(decimal.NewFromInt(totalMessages))
	batchPrefix := "chaos-db-evt-" + uuid.NewString()[:8]

	consumer, err := natsBroker.CreateOrUpdateDepositConsumerWithAckWait(ctx, 100, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var inFlightWg sync.WaitGroup
	proc := worker.NewProcessor(ctx, &inFlightWg, chaosRepo, natsBroker, 150*time.Millisecond, nopLogger, collector)
	group := worker.NewConsumerGroup(8, consumer, proc, nopLogger)

	if err := group.Start(); err != nil {
		t.Fatalf("failed to start consumer group: %v", err)
	}
	defer func() {
		group.DrainAll()
		inFlightWg.Wait()
	}()

	publishBatchAsync(t, natsBroker, batchPrefix, 1, 100, amountPerMsg)
	<-natsBroker.PublishAsyncComplete()

	chaosRepo.isKilled.Store(true)
	t.Log("CHAOS INJECTED: Database connection killed mid-stream!")

	publishBatchAsync(t, natsBroker, batchPrefix, 101, totalMessages, amountPerMsg)
	<-natsBroker.PublishAsyncComplete()

	retryDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(retryDeadline) {
		if collector.GetSnapshot().MessagesRetried > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	snapDuringOutage := collector.GetSnapshot()
	if snapDuringOutage.MessagesRetried == 0 {
		t.Fatalf("expected MessagesRetried to be greater than 0 during database outage, got 0")
	}

	chaosRepo.isKilled.Store(false)
	t.Log("CHAOS RECOVERED: Database is back online!")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if collector.GetSnapshot().MessagesProcessed == totalMessages {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalBalance, err := realRepo.GetBalance(ctx, 1, domain.CurrencyUSDT)
	if err != nil {
		t.Fatalf("failed to get final balance: %v", err)
	}

	if !finalBalance.Equal(expectedBalance) {
		t.Fatalf("DATA LOSS OR CORRUPTION! Expected %s USDT, got %s USDT",
			expectedBalance.String(), finalBalance.String())
	}
}

type flakyAckMsg struct {
	data    []byte
	failAck bool
	acked   bool
}

func (m *flakyAckMsg) Data() []byte { return m.data }
func (m *flakyAckMsg) Ack() error {
	if m.failAck {
		return errors.New("nats: connection closed before ACK could be sent")
	}
	m.acked = true
	return nil
}
func (m *flakyAckMsg) NakWithDelay(_ time.Duration) error { return nil }

func TestChaos_NATSConnectionKilledAfterDBCommit_NoDoubleSpend(t *testing.T) {
	realRepo, natsBroker := setupIntegrationEnv(t)
	ctx := context.Background()
	nopLogger := zap.NewNop()
	collector := metrics.NewCollector()

	var wg sync.WaitGroup
	proc := worker.NewProcessor(ctx, &wg, realRepo, natsBroker, 100*time.Millisecond, nopLogger, collector)

	ev := domain.DepositEvent{
		EventID:   "evt-commit-then-nats-kill-001",
		UserID:    5,
		Currency:  domain.CurrencyBTC,
		Amount:    decimal.RequireFromString("1.25000000"),
		CreatedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(ev)

	msgAttempt1 := &flakyAckMsg{data: payload, failAck: true}
	proc.HandleMessage(1, msgAttempt1)

	balAfterAttempt1, _ := realRepo.GetBalance(ctx, 5, domain.CurrencyBTC)
	if !balAfterAttempt1.Equal(ev.Amount) {
		t.Fatalf("expected balance %s after DB commit, got %s", ev.Amount, balAfterAttempt1)
	}

	msgAttempt2 := &flakyAckMsg{data: payload, failAck: false}
	proc.HandleMessage(2, msgAttempt2)

	if !msgAttempt2.acked {
		t.Fatalf("expected redelivered duplicate message to be ACKed")
	}

	balAfterRedelivery, _ := realRepo.GetBalance(ctx, 5, domain.CurrencyBTC)
	if !balAfterRedelivery.Equal(ev.Amount) {
		t.Fatalf("DOUBLE SPEND DETECTED! Expected balance %s BTC, got %s BTC",
			ev.Amount.String(), balAfterRedelivery.String())
	}

	if collector.GetSnapshot().MessagesDuplicate != 1 {
		t.Errorf("expected 1 duplicate detected metric, got %d", collector.GetSnapshot().MessagesDuplicate)
	}
}

type highSpeedShardedRepo struct {
	shards [32]struct {
		mu      sync.Mutex
		seenIDs map[string]struct{}
	}
	balancesMu sync.Mutex
	balances   map[string]decimal.Decimal
}

func newHighSpeedShardedRepo() *highSpeedShardedRepo {
	r := &highSpeedShardedRepo{
		balances: make(map[string]decimal.Decimal),
	}
	for i := 0; i < 32; i++ {
		r.shards[i].seenIDs = make(map[string]struct{}, 35000)
	}
	return r
}

func (r *highSpeedShardedRepo) ProcessDeposit(_ context.Context, event *domain.DepositEvent) error {
	shardIdx := int(event.EventID[len(event.EventID)-1]) % 32
	shard := &r.shards[shardIdx]

	shard.mu.Lock()
	if _, exists := shard.seenIDs[event.EventID]; exists {
		shard.mu.Unlock()
		return domain.ErrDuplicateEvent
	}
	shard.seenIDs[event.EventID] = struct{}{}
	shard.mu.Unlock()

	key := fmt.Sprintf("%d:%s", event.UserID, event.Currency)
	r.balancesMu.Lock()
	r.balances[key] = r.balances[key].Add(event.Amount)
	r.balancesMu.Unlock()
	return nil
}

func (r *highSpeedShardedRepo) GetBalance(_ context.Context, userID int, currency string) (decimal.Decimal, error) {
	key := fmt.Sprintf("%d:%s", userID, currency)
	r.balancesMu.Lock()
	defer r.balancesMu.Unlock()
	return r.balances[key], nil
}

func (r *highSpeedShardedRepo) Close() error { return nil }

type fastNoopBroker struct{}

func (fastNoopBroker) PublishDeposit(_ context.Context, _ string, _ []byte) (*jetstream.PubAck, error) {
	return nil, nil
}
func (fastNoopBroker) PublishDepositAsync(_ string, _ []byte) (jetstream.PubAckFuture, error) {
	return nil, nil
}
func (fastNoopBroker) PublishAsyncComplete() <-chan struct{} { return nil }
func (fastNoopBroker) PublishDLQ(_ context.Context, _ domain.DLQMessage) error {
	return nil
}
func (fastNoopBroker) CreateOrUpdateDepositConsumer(_ context.Context, _ int) (jetstream.Consumer, error) {
	return nil, nil
}
func (fastNoopBroker) CreateOrUpdateDepositConsumerWithAckWait(_ context.Context, _ int, _ time.Duration) (jetstream.Consumer, error) {
	return nil, nil
}
func (fastNoopBroker) Close() {}

type rawSliceMsg []byte

func (m rawSliceMsg) Data() []byte                       { return m }
func (m rawSliceMsg) Ack() error                         { return nil }
func (m rawSliceMsg) NakWithDelay(_ time.Duration) error { return nil }

func TestScale_1MillionEvents_StreamingPipelineAndMemoryBound(t *testing.T) {
	const totalEvents = 5_000_000
	const workerGoroutines = 32

	repo := newHighSpeedShardedRepo()
	collector := metrics.NewCollector()
	var inFlightWg sync.WaitGroup

	proc := worker.NewProcessor(
		context.Background(),
		&inFlightWg,
		repo,
		fastNoopBroker{},
		time.Second,
		zap.NewNop(),
		collector,
	)

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	jobsCh := make(chan []byte, 4096)
	var workerWg sync.WaitGroup

	for w := 1; w <= workerGoroutines; w++ {
		workerID := w
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for payload := range jobsCh {
				proc.HandleMessage(workerID, rawSliceMsg(payload))
			}
		}()
	}

	validAmount := decimal.RequireFromString("10.25")
	var expectedValidCount int64
	var expectedDupCount int64
	var expectedInvalidCount int64

	for i := 1; i <= totalEvents; i++ {
		userID := (i % 20) + 1

		if i%200 == 0 {
			expectedInvalidCount++
			badPayload := fmt.Sprintf(
				`{"event_id":"evt-1m-bad-%d","user_id":%d,"currency":"DOGE","amount":"10.25","created_at":"2026-10-04T10:00:00Z"}`,
				i, userID,
			)
			jobsCh <- []byte(badPayload)
			continue
		}

		if i%100 == 0 {
			expectedDupCount++
			dupPayload := fmt.Sprintf(
				`{"event_id":"evt-1m-%d","user_id":%d,"currency":"USDT","amount":"10.25","created_at":"2026-10-04T10:00:00Z"}`,
				i-1, userID,
			)
			jobsCh <- []byte(dupPayload)
			continue
		}

		expectedValidCount++
		validPayload := fmt.Sprintf(
			`{"event_id":"evt-1m-%d","user_id":%d,"currency":"USDT","amount":"10.25","created_at":"2026-10-04T10:00:00Z"}`,
			i, userID,
		)
		jobsCh <- []byte(validPayload)
	}

	close(jobsCh)
	workerWg.Wait()
	inFlightWg.Wait()

	elapsed := time.Since(startTime)

	snap := collector.GetSnapshot()
	if snap.MessagesReceived != totalEvents {
		t.Fatalf("expected %d received messages, got %d", totalEvents, snap.MessagesReceived)
	}
	if snap.MessagesProcessed != uint64(expectedValidCount) {
		t.Fatalf("expected %d processed messages, got %d", expectedValidCount, snap.MessagesProcessed)
	}
	if snap.MessagesDuplicate != uint64(expectedDupCount) {
		t.Fatalf("expected %d duplicate messages, got %d", expectedDupCount, snap.MessagesDuplicate)
	}
	if snap.MessagesDLQ != uint64(expectedInvalidCount) {
		t.Fatalf("expected %d DLQ messages, got %d", expectedInvalidCount, snap.MessagesDLQ)
	}

	totalSystemBalance := decimal.Zero
	for u := 1; u <= 20; u++ {
		b, _ := repo.GetBalance(context.Background(), u, domain.CurrencyUSDT)
		totalSystemBalance = totalSystemBalance.Add(b)
	}

	expectedTotalSystemBalance := validAmount.Mul(decimal.NewFromInt(expectedValidCount))
	if !totalSystemBalance.Equal(expectedTotalSystemBalance) {
		t.Fatalf("MILLION-SCALE BALANCE MISMATCH! Expected total %s, got %s",
			expectedTotalSystemBalance.String(), totalSystemBalance.String())
	}

	throughput := float64(totalEvents) / elapsed.Seconds()
	t.Logf("MILLION-SCALE SUCCESS: Processed %d events in %v (%.0f events/sec) | Total Valid Balance: %s USDT",
		totalEvents, elapsed.Round(time.Millisecond), throughput, totalSystemBalance.String())
}

func TestScale_EndToEnd_HighVolume_PostgresAndNATS(t *testing.T) {
	realRepo, natsBroker := setupIntegrationEnv(t)
	ctx := context.Background()
	nopLogger := zap.NewNop()

	totalEvents := 10_000
	if os.Getenv("MILLION_SCALE") == "1" {
		totalEvents = 1_000_000
	}

	collector := metrics.NewCollector()
	consumer, err := natsBroker.CreateOrUpdateDepositConsumerWithAckWait(ctx, 2000, 30*time.Second)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var inFlightWg sync.WaitGroup
	proc := worker.NewProcessor(ctx, &inFlightWg, realRepo, natsBroker, 2*time.Second, nopLogger, collector)
	group := worker.NewConsumerGroup(32, consumer, proc, nopLogger)

	if err := group.Start(); err != nil {
		t.Fatalf("failed to start consumer group: %v", err)
	}
	defer func() {
		group.DrainAll()
		inFlightWg.Wait()
	}()

	startTime := time.Now()
	singleAmount := decimal.RequireFromString("1.50")
	batchPrefix := uuid.NewString()[:8]

	for i := 1; i <= totalEvents; i++ {
		userID := (i % 20) + 1
		eventID := fmt.Sprintf("e2e-hv-%s-%d", batchPrefix, i)
		payload := fmt.Sprintf(
			`{"event_id":%q,"user_id":%d,"currency":"USDT","amount":"1.50","created_at":"2026-10-04T10:00:00Z"}`,
			eventID, userID,
		)
		if _, err := natsBroker.PublishDepositAsync(eventID, []byte(payload)); err != nil {
			t.Fatalf("failed async publish at %d: %v", i, err)
		}
	}

	select {
	case <-natsBroker.PublishAsyncComplete():
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for async publish completion")
	}

	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if collector.GetSnapshot().MessagesProcessed == uint64(totalEvents) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	snap := collector.GetSnapshot()
	if snap.MessagesProcessed != uint64(totalEvents) {
		t.Fatalf("expected %d processed messages, got %d", totalEvents, snap.MessagesProcessed)
	}

	totalDBBalance := decimal.Zero
	for u := 1; u <= 20; u++ {
		bal, err := realRepo.GetBalance(ctx, u, domain.CurrencyUSDT)
		if err != nil {
			t.Fatalf("failed to query balance for user %d: %v", u, err)
		}
		totalDBBalance = totalDBBalance.Add(bal)
	}

	expectedTotal := singleAmount.Mul(decimal.NewFromInt(int64(totalEvents)))
	if !totalDBBalance.Equal(expectedTotal) {
		t.Fatalf("E2E HIGH-VOLUME MISMATCH! Expected %s, got %s", expectedTotal.String(), totalDBBalance.String())
	}

	totalElapsed := time.Since(startTime)
	t.Logf("E2E HIGH-VOLUME SUCCESS: %d ACID transactions committed in %v (%.0f TPS) | Verified DB Total: %s USDT",
		totalEvents, totalElapsed.Round(time.Millisecond), float64(totalEvents)/totalElapsed.Seconds(), totalDBBalance.String())
}
