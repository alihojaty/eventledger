package grpcserver

import (
	"context"
	"os"
	"testing"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublishDeposit_IntegrationWithNATS(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	ctx := context.Background()
	nopLog := zap.NewNop()

	b, err := broker.NewNATSBroker(ctx, natsURL, nopLog)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	defer b.Close()

	srv := NewServer(b, nopLog)
	eventID := "evt-grpc-test-" + uuid.NewString()

	req := &depositv1.PublishDepositRequest{
		EventId:   eventID,
		UserId:    7,
		Currency:  "USDT",
		Amount:    "125.50",
		CreatedAt: timestamppb.Now(),
	}

	// 1. First publish -> succeeds with sequence > 0 and duplicate == false
	resp1, err := srv.PublishDeposit(ctx, req)
	if err != nil {
		t.Fatalf("expected first publish to succeed, got error: %v", err)
	}
	if resp1.GetStreamSequence() == 0 {
		t.Fatalf("expected non-zero stream sequence")
	}
	if resp1.GetDuplicate() {
		t.Fatalf("expected duplicate=false on first publish")
	}

	// 2. Second publish with same event_id -> NATS deduplication returns duplicate == true
	resp2, err := srv.PublishDeposit(ctx, req)
	if err != nil {
		t.Fatalf("expected duplicate publish to succeed, got error: %v", err)
	}
	if !resp2.GetDuplicate() {
		t.Fatalf("expected duplicate=true on second publish with identical event_id")
	}
	if resp2.GetStreamSequence() != resp1.GetStreamSequence() {
		t.Fatalf("expected same sequence %d, got %d", resp1.GetStreamSequence(), resp2.GetStreamSequence())
	}

	// 3. Invalid request -> rejected with InvalidArgument and stream LastSeq does NOT move
	stream, err := b.JetStream().Stream(ctx, broker.StreamDeposits)
	var beforeSeq uint64
	if err == nil {
		if info, err := stream.Info(ctx); err == nil {
			beforeSeq = info.State.LastSeq
		}
	}

	badReq := &depositv1.PublishDepositRequest{
		EventId:   "evt-bad-" + uuid.NewString(),
		UserId:    7,
		Currency:  "DOGE",
		Amount:    "100.00",
		CreatedAt: timestamppb.Now(),
	}
	_, err = srv.PublishDeposit(ctx, badReq)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for unsupported currency, got %v", err)
	}

	var afterSeq uint64
	if stream, err = b.JetStream().Stream(ctx, broker.StreamDeposits); err == nil {
		if info, err := stream.Info(ctx); err == nil {
			afterSeq = info.State.LastSeq
		}
	}

	if afterSeq != beforeSeq {
		t.Fatalf("expected stream LastSeq not to move on invalid input: before=%d, after=%d",
			beforeSeq, afterSeq)
	}
}
