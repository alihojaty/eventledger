package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"runtime/debug"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const MetadataKeyRequestID = "x-request-id"

type Server struct {
	depositv1.UnimplementedDepositPublisherServiceServer
	broker broker.Broker
	logger *zap.Logger
}

func NewServer(b broker.Broker, logger *zap.Logger) *Server {
	return &Server{
		broker: b,
		logger: logger,
	}
}

func (s *Server) PublishDeposit(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
	reqID := extractRequestID(ctx)
	log := s.logger.With(zap.String("request_id", reqID))

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	var createdAt time.Time
	if req.GetCreatedAt() != nil {
		createdAt = req.GetCreatedAt().AsTime()
	}

	// Zero-trust re-validation inside publisher using shared domain rules
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

	payload, err := json.Marshal(event)
	if err != nil {
		log.Error("Failed to marshal validated deposit event", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to serialize deposit event")
	}

	// Publish to NATS JetStream with Nats-Msg-Id = event_id and wait for PubAck
	ack, err := s.broker.PublishDeposit(ctx, event.EventID, payload)
	if err != nil {
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

// UnaryInterceptors provides panic recovery and structured request logging (Bonus).
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
