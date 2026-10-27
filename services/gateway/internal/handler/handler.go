package handler

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaxBodyBytes         = 16 * 1024 // 16 KB
	HeaderAPIKey         = "X-API-Key"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderRequestID      = "X-Request-ID"
	MetadataKeyRequestID = "x-request-id"
)

type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type CreateDepositRequest struct {
	UserID   int             `json:"user_id"`
	Currency string          `json:"currency"`
	Amount   json.RawMessage `json:"amount"`
}

type CreateDepositResponse struct {
	EventID        string `json:"event_id"`
	Status         string `json:"status"`
	StreamSequence uint64 `json:"stream_sequence"`
	Duplicate      bool   `json:"duplicate"`
}

type GatewayHandler struct {
	pubClient    depositv1.DepositPublisherServiceClient
	healthClient grpc_health_v1.HealthClient
	apiKey       string
	reqTimeout   time.Duration
	logger       *zap.Logger
}

func NewGatewayHandler(
	pubClient depositv1.DepositPublisherServiceClient,
	healthClient grpc_health_v1.HealthClient,
	apiKey string,
	reqTimeout time.Duration,
	logger *zap.Logger,
) *GatewayHandler {
	return &GatewayHandler{
		pubClient:    pubClient,
		healthClient: healthClient,
		apiKey:       apiKey,
		reqTimeout:   reqTimeout,
		logger:       logger,
	}
}

func (h *GatewayHandler) NewRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(h.requestIDAndLoggerMiddleware())

	r.GET("/healthz", h.handleHealthz)
	r.GET("/readyz", h.handleReadyz)

	v1 := r.Group("/v1")
	v1.Use(h.authMiddleware(), h.bodyLimitMiddleware(MaxBodyBytes))
	{
		v1.POST("/deposits", h.handleCreateDeposit)
	}

	return r
}

func (h *GatewayHandler) requestIDAndLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		reqID := uuid.NewString()
		c.Set("request_id", reqID)
		c.Header(HeaderRequestID, reqID)

		c.Next()

		h.logger.Info("HTTP request processed",
			zap.String("request_id", reqID),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(start)),
		)
	}
}

func (h *GatewayHandler) authMiddleware() gin.HandlerFunc {
	expected := []byte(h.apiKey)
	return func(c *gin.Context) {
		provided := c.GetHeader(HeaderAPIKey)
		if len(expected) == 0 || subtle.ConstantTimeCompare([]byte(provided), expected) != 1 {
			respondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid X-API-Key header")
			c.Abort()
			return
		}
		c.Next()
	}
}

func (h *GatewayHandler) bodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			respondError(c, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds 16KB limit")
			c.Abort()
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

func (h *GatewayHandler) handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *GatewayHandler) handleReadyz(c *gin.Context) {
	if h.healthClient == nil {
		respondError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "publisher health client not configured")
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 1*time.Second)
	defer cancel()

	resp, err := h.healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		respondError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "publisher service is not ready")
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (h *GatewayHandler) handleCreateDeposit(c *gin.Context) {
	reqID := c.GetString("request_id")
	log := h.logger.With(zap.String("request_id", reqID))

	idempotencyKey := c.GetHeader(HeaderIdempotencyKey)
	if err := domain.ValidateEventID(idempotencyKey); err != nil {
		respondError(c, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", err.Error())
		return
	}

	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			respondError(c, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds 16KB limit")
			return
		}
		respondError(c, http.StatusBadRequest, "INVALID_BODY", "failed to read request body")
		return
	}

	if len(bytes.TrimSpace(bodyBytes)) == 0 {
		respondError(c, http.StatusBadRequest, "INVALID_JSON", "request body cannot be empty")
		return
	}

	var req CreateDepositRequest
	dec := json.NewDecoder(bytes.NewReader(bodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		respondError(c, http.StatusBadRequest, "INVALID_JSON", "malformed JSON or unknown fields")
		return
	}

	// Ensure amount in JSON is strictly a string (not a number/float)
	if len(req.Amount) < 2 || req.Amount[0] != '"' || req.Amount[len(req.Amount)-1] != '"' {
		respondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "amount must be a JSON string")
		return
	}

	var amountStr string
	if err := json.Unmarshal(req.Amount, &amountStr); err != nil {
		respondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "amount must be a valid string")
		return
	}

	createdAt := time.Now().UTC()
	validatedEvent, err := domain.NewValidatedDepositEvent(
		idempotencyKey,
		req.UserID,
		req.Currency,
		amountStr,
		createdAt,
	)
	if err != nil {
		respondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.reqTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(MetadataKeyRequestID, reqID))

	grpcResp, err := h.pubClient.PublishDeposit(ctx, &depositv1.PublishDepositRequest{
		EventId:   validatedEvent.EventID,
		UserId:    int64(validatedEvent.UserID),
		Currency:  validatedEvent.Currency,
		Amount:    validatedEvent.Amount.String(),
		CreatedAt: timestamppb.New(validatedEvent.CreatedAt),
	})
	if err != nil {
		httpStatus, code, msg := mapGRPCErrorToHTTP(err)
		log.Warn("gRPC call to publisher failed",
			zap.String("event_id", validatedEvent.EventID),
			zap.Int("http_status", httpStatus),
			zap.String("error_code", code),
			zap.Error(err),
		)
		respondError(c, httpStatus, code, msg)
		return
	}

	log.Info("Deposit accepted by publisher",
		zap.String("event_id", validatedEvent.EventID),
		zap.Uint64("stream_sequence", grpcResp.GetStreamSequence()),
		zap.Bool("duplicate", grpcResp.GetDuplicate()),
	)

	c.JSON(http.StatusAccepted, CreateDepositResponse{
		EventID:        validatedEvent.EventID,
		Status:         "accepted",
		StreamSequence: grpcResp.GetStreamSequence(),
		Duplicate:      grpcResp.GetDuplicate(),
	})
}

func mapGRPCErrorToHTTP(err error) (int, string, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "GATEWAY_TIMEOUT", "request timed out waiting for publisher"
	}
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected internal error"
	}

	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, "VALIDATION_ERROR", st.Message()
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "GATEWAY_TIMEOUT", "request timed out waiting for publisher"
	case codes.Unavailable, codes.Canceled:
		return http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "publisher or message broker is temporarily unavailable"
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "RATE_LIMITED", st.Message()
	default:
		return http.StatusBadGateway, "UPSTREAM_ERROR", "failed to process deposit upstream"
	}
}

func respondError(c *gin.Context, httpStatus int, code, message string) {
	c.JSON(httpStatus, ErrorBody{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	})
}
