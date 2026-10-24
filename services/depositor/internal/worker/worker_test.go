package worker

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockMsg struct {
	data        []byte
	acked       bool
	naked       bool
	nakDelay    time.Duration
	actionOrder *[]string
}

func (m *mockMsg) Data() []byte { return m.data }

func (m *mockMsg) Ack() error {
	m.acked = true
	if m.actionOrder != nil {
		*m.actionOrder = append(*m.actionOrder, "ACK")
	}
	return nil
}

func (m *mockMsg) NakWithDelay(delay time.Duration) error {
	m.naked = true
	m.nakDelay = delay
	if m.actionOrder != nil {
		*m.actionOrder = append(*m.actionOrder, "NAK_WITH_DELAY")
	}
	return nil
}

type mockRepo struct {
	processErr error
	called     bool
}

func (r *mockRepo) ProcessDeposit(_ context.Context, _ *domain.DepositEvent) error {
	r.called = true
	return r.processErr
}

func (r *mockRepo) GetBalance(_ context.Context, _ int, _ string) (decimal.Decimal, error) {
	return decimal.Zero, nil
}

func (r *mockRepo) Close() error { return nil }

type mockBroker struct {
	dlqMessages []domain.DLQMessage
	dlqErr      error
	actionOrder *[]string
}

func (b *mockBroker) PublishDeposit(_ context.Context, _ string, _ []byte) (*jetstream.PubAck, error) {
	return nil, nil
}

func (b *mockBroker) PublishDepositAsync(_ string, _ []byte) (jetstream.PubAckFuture, error) {
	return nil, nil
}

func (b *mockBroker) PublishAsyncComplete() <-chan struct{} {
	return nil
}

func (b *mockBroker) PublishDLQ(_ context.Context, dlqMsg domain.DLQMessage) error {
	if b.dlqErr != nil {
		return b.dlqErr
	}
	b.dlqMessages = append(b.dlqMessages, dlqMsg)
	if b.actionOrder != nil {
		*b.actionOrder = append(*b.actionOrder, "PUBLISH_DLQ")
	}
	return nil
}

func (b *mockBroker) CreateOrUpdateDepositConsumer(_ context.Context, _ int) (jetstream.Consumer, error) {
	return nil, nil
}

func (b *mockBroker) CreateOrUpdateDepositConsumerWithAckWait(_ context.Context, _ int, _ time.Duration) (jetstream.Consumer, error) {
	return nil, nil
}

func (b *mockBroker) Close() {}

func TestProcessor_TemporaryDatabaseFailure_NakWithDelay(t *testing.T) {
	core, recordedLogs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)

	repo := &mockRepo{processErr: errors.New("connection refused: database is down")}
	mb := &mockBroker{}
	retryDelay := 5 * time.Second

	var wg sync.WaitGroup
	proc := NewProcessor(context.Background(), &wg, repo, mb, retryDelay, testLogger, nil)

	validPayload := []byte(`{
		"event_id": "evt-db-down-1",
		"user_id": 7,
		"currency": "USDT",
		"amount": "125.50",
		"created_at": "2026-10-04T10:00:00Z"
	}`)
	msg := &mockMsg{data: validPayload}

	proc.HandleMessage(1, msg)
	wg.Wait()

	if msg.acked {
		t.Fatalf("expected message NOT to be ACKed when database fails")
	}
	if !msg.naked {
		t.Fatalf("expected message to be NakWithDelay when database fails")
	}
	if msg.nakDelay != retryDelay {
		t.Errorf("expected NakWithDelay duration %v, got %v", retryDelay, msg.nakDelay)
	}

	assertLogExists(t, recordedLogs, "Temporary failure")
	assertLogExists(t, recordedLogs, "Retry started")
}

func TestProcessor_InvalidMessage_MovesToDLQThenAck(t *testing.T) {
	core, recordedLogs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)

	var order []string
	repo := &mockRepo{}
	mb := &mockBroker{actionOrder: &order}

	var wg sync.WaitGroup
	proc := NewProcessor(context.Background(), &wg, repo, mb, 5*time.Second, testLogger, nil)

	invalidPayload := []byte(`{
		"event_id": "evt-doge-99",
		"user_id": 7,
		"currency": "DOGE",
		"amount": "100.00",
		"created_at": "2026-10-04T10:00:00Z"
	}`)
	msg := &mockMsg{data: invalidPayload, actionOrder: &order}

	proc.HandleMessage(2, msg)
	wg.Wait()

	if repo.called {
		t.Fatalf("expected repository NOT to be called for invalid message")
	}
	if len(mb.dlqMessages) != 1 {
		t.Fatalf("expected 1 message published to DLQ, got %d", len(mb.dlqMessages))
	}
	if !strings.Contains(mb.dlqMessages[0].Reason, "unsupported currency") {
		t.Errorf("expected DLQ reason to mention unsupported currency, got %q", mb.dlqMessages[0].Reason)
	}
	if mb.dlqMessages[0].OriginalEvent != string(invalidPayload) {
		t.Errorf("expected DLQ original_event to match raw payload")
	}
	if len(order) != 2 || order[0] != "PUBLISH_DLQ" || order[1] != "ACK" {
		t.Fatalf("expected strict execution order [PUBLISH_DLQ, ACK], got %v", order)
	}

	assertLogExists(t, recordedLogs, "Invalid message")
	assertLogExists(t, recordedLogs, "Message moved to DLQ")
}

func TestProcessor_InvalidMessage_DLQPublishFails_DoesNotAck(t *testing.T) {
	repo := &mockRepo{}
	mb := &mockBroker{dlqErr: errors.New("nats connection timeout")}

	var wg sync.WaitGroup
	proc := NewProcessor(context.Background(), &wg, repo, mb, 3*time.Second, zap.NewNop(), nil)

	msg := &mockMsg{data: []byte(`{corrupted_json`)}
	proc.HandleMessage(1, msg)
	wg.Wait()

	if msg.acked {
		t.Fatalf("expected invalid message NOT to be ACKed when PublishDLQ fails")
	}
	if !msg.naked {
		t.Fatalf("expected message to be NakWithDelay when PublishDLQ fails")
	}
}

func TestProcessor_DuplicateEvent_AckWithoutBalanceChange(t *testing.T) {
	core, recordedLogs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)

	repo := &mockRepo{processErr: domain.ErrDuplicateEvent}
	mb := &mockBroker{}

	var wg sync.WaitGroup
	proc := NewProcessor(context.Background(), &wg, repo, mb, 5*time.Second, testLogger, nil)

	validPayload := []byte(`{
		"event_id": "evt-dup-77",
		"user_id": 3,
		"currency": "BTC",
		"amount": "0.5",
		"created_at": "2026-10-04T10:00:00Z"
	}`)
	msg := &mockMsg{data: validPayload}

	proc.HandleMessage(1, msg)
	wg.Wait()

	if !msg.acked {
		t.Fatalf("expected duplicate message to be ACKed")
	}
	if msg.naked {
		t.Fatalf("expected duplicate message NOT to be NAKed")
	}

	assertLogExists(t, recordedLogs, "Duplicate message detected")
}

type fakeWorkerStopper struct {
	workerCount int
	stopped     map[int]bool
}

func (f *fakeWorkerStopper) StopWorker(workerID int) error {
	if workerID < 1 || workerID > f.workerCount {
		return ErrInvalidWorkerID
	}
	if f.stopped[workerID] {
		return ErrWorkerAlreadyStopped
	}
	f.stopped[workerID] = true
	return nil
}

func (f *fakeWorkerStopper) WorkerCount() int {
	return f.workerCount
}

func TestTerminalController_Commands(t *testing.T) {
	stopper := &fakeWorkerStopper{
		workerCount: 3,
		stopped:     make(map[int]bool),
	}

	userInput := "2\n2\nabc\n4\n-1\n1\n"
	inBuf := strings.NewReader(userInput)
	var outBuf bytes.Buffer

	tc := NewTerminalController(stopper, inBuf, &outBuf, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tc.Start(ctx)

	output := outBuf.String()

	if !stopper.stopped[2] || !stopper.stopped[1] {
		t.Errorf("expected workers 1 and 2 to be stopped, got map: %v", stopper.stopped)
	}
	if stopper.stopped[3] {
		t.Errorf("expected worker 3 to remain active")
	}

	expectedAlreadyStoppedMsg := "Worker 2 is already stopped."
	if !strings.Contains(output, expectedAlreadyStoppedMsg) {
		t.Errorf("expected terminal output to contain %q, got:\n%s", expectedAlreadyStoppedMsg, output)
	}

	if !strings.Contains(output, `Invalid input "abc"`) {
		t.Errorf("expected warning for non-numeric input 'abc', got:\n%s", output)
	}
	if !strings.Contains(output, "Invalid worker number 4") {
		t.Errorf("expected warning for out-of-range worker 4, got:\n%s", output)
	}
	if !strings.Contains(output, "Invalid worker number -1") {
		t.Errorf("expected warning for negative worker -1, got:\n%s", output)
	}
}

func assertLogExists(t *testing.T, logs *observer.ObservedLogs, expectedMessage string) {
	t.Helper()
	for _, entry := range logs.All() {
		if entry.Message == expectedMessage {
			return
		}
	}
	t.Errorf("expected log message %q to be recorded, but it was not found", expectedMessage)
}
