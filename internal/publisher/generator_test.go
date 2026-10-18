package publisher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type mockBroker struct {
	publishedCount atomic.Int32
}

func (m *mockBroker) PublishDeposit(_ context.Context, _ string, _ []byte) (*jetstream.PubAck, error) {
	m.publishedCount.Add(1)
	return &jetstream.PubAck{Stream: "DEPOSITS", Sequence: uint64(m.publishedCount.Load())}, nil
}

func (m *mockBroker) PublishDepositAsync(_ string, _ []byte) (jetstream.PubAckFuture, error) {
	return nil, nil
}

func (m *mockBroker) PublishAsyncComplete() <-chan struct{} {
	return nil
}

func (m *mockBroker) PublishDLQ(_ context.Context, _ domain.DLQMessage) error {
	return nil
}

func (m *mockBroker) CreateOrUpdateDepositConsumer(_ context.Context, _ int) (jetstream.Consumer, error) {
	return nil, nil
}

func (m *mockBroker) CreateOrUpdateDepositConsumerWithAckWait(_ context.Context, _ int, _ time.Duration) (jetstream.Consumer, error) {
	return nil, nil
}

func (m *mockBroker) Close() {}

func TestPublisher_Disabled(t *testing.T) {
	mb := &mockBroker{}
	pub := NewPublisher(mb, 10*time.Millisecond, false, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	pub.Start(ctx)

	if got := mb.publishedCount.Load(); got != 0 {
		t.Errorf("expected 0 published messages when PUBLISH_ENABLED=false, got %d", got)
	}
}

func TestPublisher_EnabledAndGracefulStop(t *testing.T) {
	mb := &mockBroker{}
	pub := NewPublisher(mb, 15*time.Millisecond, true, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()

	pub.Start(ctx)

	if got := mb.publishedCount.Load(); got < 2 {
		t.Errorf("expected at least 2 published messages, got %d", got)
	}
}
