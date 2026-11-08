package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime/debug"
	"sync"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/healthmgr"
	"go.uber.org/zap"
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

type pubIdemShard struct {
	mu      sync.Mutex
	entries map[string]string
}

type Server struct {
	depositv1.UnimplementedDepositPublisherServiceServer
	broker     broker.Broker
	health     *healthmgr.Manager
	logger     *zap.Logger
	idemShards [pubIdemShards]pubIdemShard
}

func NewServer(b broker.Broker, logger *zap.Logger, hm ...*healthmgr.Manager) *Server {
	var h *healthmgr.Manager
	if len(hm) > 0 {
		h = hm[0]
	}
	s := &Server{
		broker: b,
		health: h,
		logger: logger,
	}
	for i := 0; i < pubIdemShards; i++ {
		s.idemShards[i].entries = make(map[string]string, 256)
	}
	return s
}

func (s *Server) checkOrRememberPayload(eventID, fingerprint string) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(eventID))
	shard := &s.idemShards[h.Sum32()%pubIdemShards]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, ok := shard.entries[eventID]; ok {
		return existing == fingerprint
	}
	if len(shard.entries) >= pubMaxIdemPerShard {
		for k := range shard.entries {
			delete(shard.entries, k)
			break
		}
	}
	shard.entries[eventID] = fingerprint
	return true
}

func (s *Server) forgetPayload(eventID string) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(eventID))
	shard := &s.idemShards[h.Sum32()%pubIdemShards]

	shard.mu.Lock()
	delete(shard.entries, eventID)
	shard.mu.Unlock()
}

func (s *Server) PublishDeposit(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
	var receivedAt time.Time
	var draining bool
	if s.health != nil {
		receivedAt = s.health.RecordRequestStart()
		draining = s.health.IsDraining()
	}
	succeeded := false
	defer func() {
		if s.health != nil {
			completedAt := s.health.RecordRequestEnd(succeeded)
			if draining {
				s.logger.Info("In-flight deposit request completed during drain/shutdown",
					zap.String("event_id", req.GetEventId()),
					zap.Time("received_at", receivedAt),
					zap.Time("completed_at", completedAt),
					zap.Bool("succeeded", succeeded),
				)
			}
		}
	}()

	reqID := extractRequestID(ctx)
	log := s.logger.With(zap.String("request_id", reqID))

	if draining {
		log.Info("Deposit request arrived during DRAIN_DELAY window; processing normally",
			zap.String("event_id", req.GetEventId()),
			zap.Time("received_at", receivedAt),
		)
	}

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
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
		log.Warn("Rejected invalid deposit request at publisher gRPC boundary",
			zap.String("event_id", req.GetEventId()),
			zap.Error(err),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	fingerprint := fmt.Sprintf("%d:%s:%s", event.UserID, event.Currency, event.Amount.String())
	if !s.checkOrRememberPayload(event.EventID, fingerprint) {
		log.Warn("Rejected conflicting idempotency key with different deposit parameters",
			zap.String("event_id", event.EventID),
		)
		return nil, status.Error(codes.AlreadyExists, "event_id was already used with different deposit parameters")
	}

	payload, err := json.Marshal(event)
	if err != nil {
		s.forgetPayload(event.EventID)
		log.Error("Failed to marshal validated deposit event", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to serialize deposit event")
	}

	ack, err := s.broker.PublishDeposit(ctx, event.EventID, payload)
	if err != nil {
		s.forgetPayload(event.EventID)
		log.Error("Failed to publish deposit event to NATS JetStream",
			zap.String("event_id", event.EventID),
			zap.Error(err),
		)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "timed out publishing event to broker")
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, status.Error(codes.Canceled, "request canceled before broker ack")
		}
		return nil, status.Error(codes.Unavailable, "message broker is currently unavailable")
	}

	succeeded = true
	log.Info("Deposit event published via gRPC",
		zap.String("event_id", event.EventID),
		zap.Int("user_id", event.UserID),
		zap.String("currency", event.Currency),
		zap.String("amount", event.Amount.String()),
		zap.Uint64("stream_sequence", ack.Sequence),
		zap.Bool("duplicate", ack.Duplicate),
	)

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

func UnaryInterceptors(logger *zap.Logger) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
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
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
}
