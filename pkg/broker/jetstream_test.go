package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func setupTestBroker(t *testing.T) *NATSBroker {
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

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("failed to open db for advisory lock: %v", err)
	}
	lockConn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("failed to get lock conn: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "SELECT pg_advisory_lock(998877)"); err != nil {
		_ = lockConn.Close()
		_ = db.Close()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}

	b, err := NewNATSBroker(ctx, natsURL, nopLogger)
	if err != nil {
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = db.Close()
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	if stream, err := b.JetStream().Stream(ctx, StreamDeposits); err == nil {
		_ = stream.Purge(ctx)
	}
	if dlqStream, err := b.JetStream().Stream(ctx, StreamDepositsDLQ); err == nil {
		_ = dlqStream.Purge(ctx)
	}

	t.Cleanup(func() {
		b.Close()
		_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock(998877)")
		_ = lockConn.Close()
		_ = db.Close()
	})

	return b
}

func TestNATSBroker_PublishDeduplication(t *testing.T) {
	b := setupTestBroker(t)
	ctx := context.Background()

	eventID := "evt-dedup-" + uuid.NewString()
	payload := []byte(`{"event_id":"` + eventID + `","user_id":1,"currency":"USDT","amount":"100","created_at":"2026-10-04T10:00:00Z"}`)

	ack1, err := b.PublishDeposit(ctx, eventID, payload)
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	if ack1.Duplicate {
		t.Errorf("expected first publish Duplicate=false, got true")
	}

	ack2, err := b.PublishDeposit(ctx, eventID, payload)
	if err != nil {
		t.Fatalf("second publish failed: %v", err)
	}
	if !ack2.Duplicate {
		t.Errorf("expected second publish with same MsgID to return Duplicate=true, got false")
	}
	if ack1.Sequence != ack2.Sequence {
		t.Errorf("expected both PubAcks to point to the same stream sequence %d, got %d", ack1.Sequence, ack2.Sequence)
	}

	stream, err := b.JetStream().Stream(ctx, StreamDeposits)
	if err != nil {
		t.Fatalf("failed to get stream info: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("failed to get stream state: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Errorf("expected exactly 1 message in DEPOSITS stream after duplicate publish, got %d", info.State.Msgs)
	}
}

func TestNATSBroker_PublishDLQ(t *testing.T) {
	b := setupTestBroker(t)
	ctx := context.Background()

	dlqPayload := domain.DLQMessage{
		OriginalEvent: `{"event_id":"bad-1","currency":"DOGE"}`,
		Reason:        "unsupported currency \"DOGE\"",
		Timestamp:     time.Now().UTC().Truncate(time.Second),
	}

	if err := b.PublishDLQ(ctx, dlqPayload); err != nil {
		t.Fatalf("failed to publish to DLQ: %v", err)
	}

	dlqConsumer, err := b.JetStream().CreateOrUpdateConsumer(ctx, StreamDepositsDLQ, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create DLQ consumer: %v", err)
	}

	msgs, err := dlqConsumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch message from DLQ: %v", err)
	}

	var received domain.DLQMessage
	count := 0
	for msg := range msgs.Messages() {
		count++
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Fatalf("failed to unmarshal DLQ message: %v", err)
		}
		_ = msg.Ack()
	}

	if count != 1 {
		t.Fatalf("expected 1 message in DLQ stream, got %d", count)
	}
	if received.Reason != dlqPayload.Reason || received.OriginalEvent != dlqPayload.OriginalEvent {
		t.Errorf("DLQ message mismatch: got %+v, want %+v", received, dlqPayload)
	}
}
