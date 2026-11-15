package grpcserver

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/healthmgr"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	MetadataKeyRequestID = "x-request-id"
	pubIdemShards        = 32
	pubMaxIdemPerShard   = 4096
)

type pubIdemEntry struct {
	fingerprint string
	committed   bool
}

type pubIdemShard struct {
	mu       sync.Mutex
	current  map[string]pubIdemEntry
	previous map[string]pubIdemEntry
}

type ServerOption func(*Server)

func WithPublisherMetrics(m *metrics.PublisherMetrics) ServerOption {
	return func(s *Server) {
		if m != nil {
			s.metrics = m
		}
	}
}

type Server struct {
	depositv1.UnimplementedDepositPublisherServiceServer
	broker     broker.Broker
	health     *healthmgr.Manager
	metrics    *metrics.PublisherMetrics
	logger     *zap.Logger
	idemShards [pubIdemShards]pubIdemShard
}

func NewServer(b broker.Broker, logger *zap.Logger, hm ...*healthmgr.Manager) *Server {
	var h *healthmgr.Manager
	if len(hm) > 0 {
		h = hm[0]
	}
	s := &Server{
		broker:  b,
		health:  h,
		metrics: metrics.NewPublisherMetrics(prometheus.NewRegistry(), h),
		logger:  logger,
	}
	if h != nil {
		h.AttachMetrics(s.metrics)
	}
	for i := 0; i < pubIdemShards; i++ {
		s.idemShards[i].current = make(map[string]pubIdemEntry, 256)
	}
	return s
}

func NewServerWithMetrics(b broker.Broker, logger *zap.Logger, hm *healthmgr.Manager, pm *metrics.PublisherMetrics) *Server {
	s := NewServer(b, logger, hm)
	if pm != nil {
		s.metrics = pm
		if hm != nil {
			hm.AttachMetrics(pm)
		}
	}
	return s
}

func (s *Server) Metrics() *metrics.PublisherMetrics {
	return s.metrics
}

func (s *Server) shardFor(eventID string) *pubIdemShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(eventID))
	return &s.idemShards[h.Sum32()%pubIdemShards]
}

func (s *Server) checkOrRememberPayload(eventID, fingerprint string) bool {
	shard := s.shardFor(eventID)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.current[eventID]; ok {
		return existing.fingerprint == fingerprint
	}
	if shard.previous != nil {
		if existing, ok := shard.previous[eventID]; ok {
			if existing.fingerprint != fingerprint {
				return false
			}
			if len(shard.current) >= pubMaxIdemPerShard {
				shard.previous = shard.current
				shard.current = make(map[string]pubIdemEntry, pubMaxIdemPerShard)
			}
			shard.current[eventID] = existing
			return true
		}
	}
	if len(shard.current) >= pubMaxIdemPerShard {
		shard.previous = shard.current
		shard.current = make(map[string]pubIdemEntry, pubMaxIdemPerShard)
	}
	shard.current[eventID] = pubIdemEntry{
		fingerprint: fingerprint,
		committed:   false,
	}
	return true
}

func (s *Server) markPayloadCommitted(eventID, fingerprint string) {
	shard := s.shardFor(eventID)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.current[eventID]; ok {
		existing.committed = true
		shard.current[eventID] = existing
		return
	}
	if shard.previous != nil {
		if existing, ok := shard.previous[eventID]; ok {
			existing.committed = true
			shard.previous[eventID] = existing
			return
		}
	}
	shard.current[eventID] = pubIdemEntry{
		fingerprint: fingerprint,
		committed:   true,
	}
}

func (s *Server) forgetPayloadIfUncommitted(eventID string) {
	shard := s.shardFor(eventID)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.current[eventID]; ok && existing.committed {
		return
	}
	if shard.previous != nil {
		if existing, ok := shard.previous[eventID]; ok && existing.committed {
			return
		}
	}

	delete(shard.current, eventID)
	if shard.previous != nil {
		delete(shard.previous, eventID)
	}
}

func buildPublisherFingerprint(userID int, currency, amountStr string) string {
	var buf [64]byte
	b := strconv.AppendInt(buf[:0], int64(userID), 10)
	b = append(b, ':')
	b = append(b, currency...)
	b = append(b, ':')
	b = append(b, amountStr...)
	return string(b)
}

func marshalValidatedEventFast(ev *domain.DepositEvent, amountStr string) []byte {
	b := make([]byte, 0, 160)
	b = append(b, `{"event_id":"`...)
	b = append(b, ev.EventID...)
	b = append(b, `","user_id":`...)
	b = strconv.AppendInt(b, int64(ev.UserID), 10)
	b = append(b, `,"currency":"`...)
	b = append(b, ev.Currency...)
	b = append(b, `","amount":"`...)
	b = append(b, amountStr...)
	b = append(b, `","created_at":"`...)
	b = ev.CreatedAt.UTC().AppendFormat(b, time.RFC3339Nano)
	b = append(b, `"}`...)
	return b
}

func (s *Server) PublishDeposit(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
	s.metrics.RequestsInFlight.Inc()
	var receivedAt time.Time
	var draining bool
	if s.health != nil {
		receivedAt = s.health.RecordRequestStart()
		draining = s.health.IsDraining()
	}
	succeeded := false
	defer func() {
		s.metrics.RequestsInFlight.Dec()
		if s.health != nil {
			completedAt := s.health.RecordRequestEnd(succeeded)
			if draining && req != nil {
				s.logger.Info("In-flight deposit request completed during drain/shutdown",
					zap.String("event_id", req.GetEventId()),
					zap.Time("received_at", receivedAt),
					zap.Time("completed_at", completedAt),
					zap.Bool("succeeded", succeeded),
				)
			}
		}
	}()

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	reqID := extractRequestID(ctx)

	if draining {
		s.logger.Info("Deposit request arrived during DRAIN_DELAY window; processing normally",
			zap.String("request_id", reqID),
			zap.String("event_id", req.GetEventId()),
			zap.Time("received_at", receivedAt),
		)
	}

	var createdAt time.Time
	if req.GetCreatedAt() != nil {
		createdAt = req.GetCreatedAt().AsTime()
	}

	event, err := domain.NewValidatedDepositEvent(
		req.GetEventId(),
		int(req.GetUserId()),
		req.GetCurrency(),
		req.GetAmount(),
		createdAt,
	)
	if err != nil {
		s.logger.Warn("Rejected invalid deposit request at publisher gRPC boundary",
			zap.String("request_id", reqID),
			zap.String("event_id", req.GetEventId()),
			zap.Error(err),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	normAmountStr := event.Amount.String()
	fingerprint := buildPublisherFingerprint(event.UserID, event.Currency, normAmountStr)
	if !s.checkOrRememberPayload(event.EventID, fingerprint) {
		s.metrics.IdempotencyConflicts.Inc()
		s.logger.Warn("Rejected conflicting idempotency key with different deposit parameters",
			zap.String("request_id", reqID),
			zap.String("event_id", event.EventID),
		)
		return nil, status.Error(codes.AlreadyExists, "event_id was already used with different deposit parameters")
	}

	payload := marshalValidatedEventFast(event, normAmountStr)

	pubStart := time.Now()
	ack, err := s.broker.PublishDeposit(ctx, event.EventID, payload)
	s.metrics.JetStreamPublishDuration.Observe(time.Since(pubStart).Seconds())

	if err != nil {
		s.metrics.JetStreamPublishTotal.WithLabelValues(metrics.PublishResultError).Inc()
		s.logger.Error("Failed to publish deposit event to NATS JetStream",
			zap.String("request_id", reqID),
			zap.String("event_id", event.EventID),
			zap.Error(err),
		)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "timed out publishing event to broker")
		}
		s.forgetPayloadIfUncommitted(event.EventID)
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, status.Error(codes.Canceled, "request canceled before broker ack")
		}
		return nil, status.Error(codes.Unavailable, "message broker is currently unavailable")
	}

	s.markPayloadCommitted(event.EventID, fingerprint)
	succeeded = true
	if ack.Duplicate {
		s.metrics.JetStreamPublishTotal.WithLabelValues(metrics.PublishResultDuplicate).Inc()
	} else {
		s.metrics.JetStreamPublishTotal.WithLabelValues(metrics.PublishResultOK).Inc()
	}

	if s.logger.Core().Enabled(zapcore.InfoLevel) {
		s.logger.Info("Deposit event published via gRPC",
			zap.String("request_id", reqID),
			zap.String("event_id", event.EventID),
			zap.Int("user_id", event.UserID),
			zap.String("currency", event.Currency),
			zap.String("amount", normAmountStr),
			zap.Uint64("stream_sequence", ack.Sequence),
			zap.Bool("duplicate", ack.Duplicate),
		)
	}

	return &depositv1.PublishDepositResponse{
		StreamSequence: ack.Sequence,
		Duplicate:      ack.Duplicate,
	}, nil
}

func extractRequestID(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(MetadataKeyRequestID); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	return "unknown"
}

func UnaryInterceptors(logger *zap.Logger, extra ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	chain := make([]grpc.UnaryServerInterceptor, 0, 2+len(extra))
	chain = append(chain,
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("Recovered from panic in gRPC handler",
						zap.Any("panic", r),
						zap.String("stack", string(debug.Stack())),
					)
					err = status.Error(codes.Internal, "internal server error")
				}
			}()
			return handler(ctx, req)
		},
	)
	chain = append(chain, extra...)
	chain = append(chain,
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if !logger.Core().Enabled(zapcore.InfoLevel) {
				return handler(ctx, req)
			}
			start := time.Now()
			reqID := extractRequestID(ctx)
			resp, err := handler(ctx, req)
			st, _ := status.FromError(err)
			logger.Info("gRPC call completed",
				zap.String("method", info.FullMethod),
				zap.String("request_id", reqID),
				zap.String("grpc_code", st.Code().String()),
				zap.Duration("duration", time.Since(start)),
			)
			return resp, err
		},
	)
	return grpc.ChainUnaryInterceptor(chain...)
}
