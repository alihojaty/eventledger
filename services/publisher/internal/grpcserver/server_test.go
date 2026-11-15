package grpcserver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/healthmgr"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nopLog := zap.NewNop()

	b, err := broker.NewNATSBroker(ctx, natsURL, nopLog)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	defer b.Close()

	hm := healthmgr.NewManager("pub-test-1", false, nopLog)
	hm.SetNATSConnected(true)
	reg := prometheus.NewRegistry()
	pubMetrics := metrics.NewPublisherMetrics(reg, hm)

	srv := NewServerWithMetrics(b, nopLog, hm, pubMetrics)
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
	if got := testutil.ToFloat64(pubMetrics.JetStreamPublishTotal.WithLabelValues(metrics.PublishResultOK)); got != 1 {
		t.Fatalf("expected publisher_jetstream_publish_total{result=\"ok\"} == 1, got %v", got)
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
	if got := testutil.ToFloat64(pubMetrics.JetStreamPublishTotal.WithLabelValues(metrics.PublishResultDuplicate)); got != 1 {
		t.Fatalf("expected publisher_jetstream_publish_total{result=\"duplicate\"} == 1, got %v", got)
	}

	// 3. Conflicting payload with same event_id -> increments publisher_idempotency_conflicts_total
	conflictReq := &depositv1.PublishDepositRequest{
		EventId:   eventID,
		UserId:    7,
		Currency:  "USDT",
		Amount:    "999.00",
		CreatedAt: timestamppb.Now(),
	}
	_, err = srv.PublishDeposit(ctx, conflictReq)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on conflicting payload, got %v", err)
	}
	if got := testutil.ToFloat64(pubMetrics.IdempotencyConflicts); got != 1 {
		t.Fatalf("expected publisher_idempotency_conflicts_total == 1, got %v", got)
	}

	// 4. Verify health gauges reflect healthmgr state
	if got := testutil.ToFloat64(pubMetrics.ReadyGauge); got != 1 {
		t.Fatalf("expected publisher_ready == 1, got %v", got)
	}
	if got := testutil.ToFloat64(pubMetrics.DrainingGauge); got != 0 {
		t.Fatalf("expected publisher_draining == 0, got %v", got)
	}

	// 5. Invalid request -> rejected with InvalidArgument and stream LastSeq does NOT move
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
