package handler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/metrics"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/ratelimit"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaxBodyBytes           = 16 * 1024 // 16 KB
	HeaderAPIKey           = "X-API-Key"
	HeaderIdempotencyKey   = "Idempotency-Key"
	HeaderRequestID        = "X-Request-ID"
	MetadataKeyRequestID   = "x-request-id"
	RouteUnmatched         = "unmatched"
	idemShards             = 32
	maxIdemEntriesPerShard = 4096
	readyCacheTTL          = 1 * time.Second
)

var (
	bodyBufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 256))
		},
	}
	reqIDPrefix string
	reqIDSeq    atomic.Uint64
)

func init() {
	var b [8]byte
	_, _ = rand.Read(b[:])
	reqIDPrefix = "req-" + hex.EncodeToString(b[:]) + "-"
}

func nextFastRequestID() string {
	var buf [32]byte
	b := append(buf[:0], reqIDPrefix...)
	b = strconv.AppendUint(b, reqIDSeq.Add(1), 36)
	return string(b)
}

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

type GatewayStatsSnapshot struct {
	Ready            bool    `json:"ready"`
	MTLSEnabled      bool    `json:"mtls_enabled"`
	RateLimitRPS     float64 `json:"rate_limit_rps"`
	RateLimitBurst   int     `json:"rate_limit_burst"`
	TotalRequests    uint64  `json:"total_requests"`
	Accepted202      uint64  `json:"accepted_202"`
	Duplicates       uint64  `json:"duplicates"`
	BadRequest400    uint64  `json:"bad_request_400"`
	Unauthorized401  uint64  `json:"unauthorized_401"`
	PayloadLarge413  uint64  `json:"payload_large_413"`
	RateLimited429   uint64  `json:"rate_limited_429"`
	UpstreamError5xx uint64  `json:"upstream_error_5xx"`
	GRPCRetries      uint64  `json:"grpc_retries"`
}

type idemEntry struct {
	fingerprint string
	committed   bool
}

type idemShard struct {
	mu       sync.Mutex
	current  map[string]idemEntry
	previous map[string]idemEntry
}

type idempotencyPayloadGuard struct {
	shards [idemShards]idemShard
}

func newIdempotencyPayloadGuard() *idempotencyPayloadGuard {
	g := &idempotencyPayloadGuard{}
	for i := 0; i < idemShards; i++ {
		g.shards[i].current = make(map[string]idemEntry, 256)
	}
	return g
}

func (g *idempotencyPayloadGuard) shardFor(eventID string) *idemShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(eventID))
	return &g.shards[h.Sum32()%idemShards]
}

func (g *idempotencyPayloadGuard) checkOrRemember(eventID, fingerprint string) bool {
	shard := g.shardFor(eventID)

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
			if len(shard.current) >= maxIdemEntriesPerShard {
				shard.previous = shard.current
				shard.current = make(map[string]idemEntry, maxIdemEntriesPerShard)
			}
			shard.current[eventID] = existing
			return true
		}
	}
	if len(shard.current) >= maxIdemEntriesPerShard {
		shard.previous = shard.current
		shard.current = make(map[string]idemEntry, maxIdemEntriesPerShard)
	}
	shard.current[eventID] = idemEntry{
		fingerprint: fingerprint,
		committed:   false,
	}
	return true
}

func (g *idempotencyPayloadGuard) markCommitted(eventID, fingerprint string) {
	shard := g.shardFor(eventID)

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
	shard.current[eventID] = idemEntry{
		fingerprint: fingerprint,
		committed:   true,
	}
}

func (g *idempotencyPayloadGuard) forgetIfUncommitted(eventID string) {
	shard := g.shardFor(eventID)

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

func buildFingerprint(userID int, currency, amountStr string) string {
	var buf [64]byte
	b := strconv.AppendInt(buf[:0], int64(userID), 10)
	b = append(b, ':')
	b = append(b, currency...)
	b = append(b, ':')
	b = append(b, amountStr...)
	return string(b)
}

type GatewayOption func(*GatewayHandler)

func WithRateLimiter(limiter *ratelimit.KeyRateLimiter) GatewayOption {
	return func(h *GatewayHandler) {
		h.limiter = limiter
	}
}

func WithMTLSEnabled(enabled bool) GatewayOption {
	return func(h *GatewayHandler) {
		h.mtlsEnabled = enabled
	}
}

func WithMetrics(m *metrics.GatewayMetrics) GatewayOption {
	return func(h *GatewayHandler) {
		if m != nil {
			h.metrics = m
		}
	}
}

type GatewayHandler struct {
	pubClient    depositv1.DepositPublisherServiceClient
	healthClient grpc_health_v1.HealthClient
	apiKey       string
	apiKeyHash   [32]byte
	reqTimeout   time.Duration
	limiter      *ratelimit.KeyRateLimiter
	idemGuard    *idempotencyPayloadGuard
	mtlsEnabled  bool
	metrics      *metrics.GatewayMetrics
	logger       *zap.Logger

	readyProbeMu     sync.Mutex
	cachedReady      atomic.Bool
	cachedReadyUntil atomic.Int64
	probingReady     atomic.Bool
}

func NewGatewayHandler(
	pubClient depositv1.DepositPublisherServiceClient,
	healthClient grpc_health_v1.HealthClient,
	apiKey string,
	reqTimeout time.Duration,
	logger *zap.Logger,
	opts ...GatewayOption,
) *GatewayHandler {
	h := &GatewayHandler{
		pubClient:    pubClient,
		healthClient: healthClient,
		apiKey:       apiKey,
		apiKeyHash:   sha256.Sum256([]byte(apiKey)),
		reqTimeout:   reqTimeout,
		idemGuard:    newIdempotencyPayloadGuard(),
		logger:       logger,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.metrics == nil {
		h.metrics = metrics.NewGatewayMetrics(prometheus.NewRegistry())
	}
	return h
}

func (h *GatewayHandler) Metrics() *metrics.GatewayMetrics {
	return h.metrics
}

func (h *GatewayHandler) IncGRPCRetry() {
	h.metrics.GRPCRetriesTotal.Inc()
}

// NewRouter creates the public HTTP router on port 8081.
// Note: /metrics is intentionally NOT registered on the public router; it is served exclusively on METRICS_PORT.
func (h *GatewayHandler) NewRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(h.requestIDMetricsAndLoggerMiddleware())

	r.GET("/healthz", h.handleHealthz)
	r.GET("/readyz", h.handleReadyz)
	r.GET("/stats", h.handleStats)

	v1 := r.Group("/v1")
	v1.Use(h.authMiddleware(), h.rateLimitMiddleware(), h.bodyLimitMiddleware(MaxBodyBytes))
	{
		v1.POST("/deposits", h.handleCreateDeposit)
	}

	return r
}

func (h *GatewayHandler) requestIDMetricsAndLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		reqID := nextFastRequestID()
		c.Set("request_id", reqID)
		c.Header(HeaderRequestID, reqID)

		h.metrics.HTTPRequestsInFlight.Inc()
		defer h.metrics.HTTPRequestsInFlight.Dec()

		c.Next()

		duration := time.Since(start)
		route := c.FullPath()
		if route == "" {
			route = RouteUnmatched
		}
		method := c.Request.Method
		statusCode := c.Writer.Status()

		h.metrics.HTTPRequestsTotal.WithLabelValues(method, route, strconv.Itoa(statusCode)).Inc()

		obs := h.metrics.HTTPRequestDuration.WithLabelValues(method, route)
		if exObs, ok := obs.(prometheus.ExemplarObserver); ok && reqID != "" {
			exObs.ObserveWithExemplar(duration.Seconds(), prometheus.Labels{"request_id": reqID})
		} else {
			obs.Observe(duration.Seconds())
		}

		if h.logger.Core().Enabled(zapcore.InfoLevel) {
			h.logger.Info("HTTP request processed",
				zap.String("request_id", reqID),
				zap.String("method", method),
				zap.String("route", route),
				zap.Int("status", statusCode),
				zap.Duration("duration", duration),
			)
		}
	}
}

func (h *GatewayHandler) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		provided := c.GetHeader(HeaderAPIKey)
		providedHash := sha256.Sum256([]byte(provided))
		if len(h.apiKey) == 0 || subtle.ConstantTimeCompare(providedHash[:], h.apiKeyHash[:]) != 1 {
			h.metrics.RecordDepositResult(metrics.ResultUnauthorized)
			respondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid X-API-Key header")
			c.Abort()
			return
		}
		c.Next()
	}
}

func (h *GatewayHandler) rateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.limiter == nil {
			c.Next()
			return
		}
		apiKey := c.GetHeader(HeaderAPIKey)
		if !h.limiter.Allow(apiKey) {
			h.metrics.RecordDepositResult(metrics.ResultRateLimited)
			respondError(c, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded for X-API-Key")
			c.Abort()
			return
		}
		c.Next()
	}
}

func (h *GatewayHandler) bodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			h.metrics.RecordDepositResult(metrics.ResultPayloadTooLarge)
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

func (h *GatewayHandler) isPublisherReady(ctx context.Context) bool {
	if h.healthClient == nil {
		return false
	}

	h.readyProbeMu.Lock()
	defer h.readyProbeMu.Unlock()

	checkCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()

	resp, err := h.healthClient.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{})
	ready := err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING

	h.cachedReady.Store(ready)
	h.cachedReadyUntil.Store(time.Now().Add(readyCacheTTL).UnixNano())
	return ready
}

func (h *GatewayHandler) isPublisherReadyNonBlocking(ctx context.Context) bool {
	if h.healthClient == nil {
		return false
	}

	untilNs := h.cachedReadyUntil.Load()
	nowNs := time.Now().UnixNano()

	if untilNs == 0 {
		return h.isPublisherReady(ctx)
	}

	if nowNs >= untilNs && h.probingReady.CompareAndSwap(false, true) {
		go func() {
			defer h.probingReady.Store(false)
			bgCtx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			_ = h.isPublisherReady(bgCtx)
		}()
	}

	return h.cachedReady.Load()
}

func (h *GatewayHandler) handleReadyz(c *gin.Context) {
	if !h.isPublisherReady(c.Request.Context()) {
		respondError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "publisher service is not ready")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// handleStats reads all counters directly from the Prometheus metrics (single source of counting).
func (h *GatewayHandler) handleStats(c *gin.Context) {
	var rps float64
	var burst int
	if h.limiter != nil {
		rps = h.limiter.RPS()
		burst = h.limiter.Burst()
	}

	acceptedFresh := h.metrics.CountByResult(metrics.ResultAccepted)
	duplicates := h.metrics.CountByResult(metrics.ResultDuplicate)
	invalid := h.metrics.CountByResult(metrics.ResultInvalid)
	unauthorized := h.metrics.CountByResult(metrics.ResultUnauthorized)
	payloadLarge := h.metrics.CountByResult(metrics.ResultPayloadTooLarge)
	rateLimited := h.metrics.CountByResult(metrics.ResultRateLimited)
	upstreamUnavail := h.metrics.CountByResult(metrics.ResultUpstreamUnavailable)
	upstreamTimeout := h.metrics.CountByResult(metrics.ResultUpstreamTimeout)
	internalErr := h.metrics.CountByResult(metrics.ResultInternalError)

	c.JSON(http.StatusOK, GatewayStatsSnapshot{
		Ready:            h.isPublisherReadyNonBlocking(c.Request.Context()),
		MTLSEnabled:      h.mtlsEnabled,
		RateLimitRPS:     rps,
		RateLimitBurst:   burst,
		TotalRequests:    h.metrics.TotalDepositRequests(),
		Accepted202:      acceptedFresh + duplicates,
		Duplicates:       duplicates,
		BadRequest400:    invalid,
		Unauthorized401:  unauthorized,
		PayloadLarge413:  payloadLarge,
		RateLimited429:   rateLimited,
		UpstreamError5xx: upstreamUnavail + upstreamTimeout + internalErr,
		GRPCRetries:      h.metrics.RetriesCount(),
	})
}

func (h *GatewayHandler) handleCreateDeposit(c *gin.Context) {
	reqID := c.GetString("request_id")

	idempotencyKey := c.GetHeader(HeaderIdempotencyKey)
	if err := domain.ValidateEventID(idempotencyKey); err != nil {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", err.Error())
		return
	}

	buf := bodyBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		if buf.Cap() <= MaxBodyBytes*2 {
			bodyBufferPool.Put(buf)
		}
	}()

	if _, err := buf.ReadFrom(c.Request.Body); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.metrics.RecordDepositResult(metrics.ResultPayloadTooLarge)
			respondError(c, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds 16KB limit")
			return
		}
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "INVALID_BODY", "failed to read request body")
		return
	}

	bodyBytes := bytes.TrimSpace(buf.Bytes())
	if len(bodyBytes) == 0 {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "INVALID_JSON", "request body cannot be empty")
		return
	}

	var req CreateDepositRequest
	dec := json.NewDecoder(bytes.NewReader(bodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "INVALID_JSON", "malformed JSON or unknown fields")
		return
	}
	if dec.More() || dec.Decode(&struct{}{}) != io.EOF {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "INVALID_JSON", "unexpected trailing data after JSON object")
		return
	}

	amountStr, err := domain.UnquoteJSONStringFast(req.Amount)
	if err != nil {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
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
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	normAmountStr := validatedEvent.Amount.String()
	fingerprint := buildFingerprint(validatedEvent.UserID, validatedEvent.Currency, normAmountStr)
	if !h.idemGuard.checkOrRemember(validatedEvent.EventID, fingerprint) {
		h.metrics.RecordDepositResult(metrics.ResultInvalid)
		respondError(c, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", "Idempotency-Key was already used with different deposit parameters")
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.reqTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(MetadataKeyRequestID, reqID))

	grpcResp, err := h.pubClient.PublishDeposit(ctx, &depositv1.PublishDepositRequest{
		EventId:   validatedEvent.EventID,
		UserId:    int64(validatedEvent.UserID),
		Currency:  validatedEvent.Currency,
		Amount:    normAmountStr,
		CreatedAt: timestamppb.New(validatedEvent.CreatedAt),
	})
	if err != nil {
		httpStatus, code, msg, metricResult := mapGRPCErrorToHTTPAndMetric(err)
		if httpStatus != http.StatusGatewayTimeout {
			h.idemGuard.forgetIfUncommitted(validatedEvent.EventID)
		}
		h.metrics.RecordDepositResult(metricResult)
		h.logger.Warn("gRPC call to publisher failed",
			zap.String("request_id", reqID),
			zap.String("event_id", validatedEvent.EventID),
			zap.Int("http_status", httpStatus),
			zap.String("error_code", code),
			zap.Error(err),
		)
		respondError(c, httpStatus, code, msg)
		return
	}

	h.idemGuard.markCommitted(validatedEvent.EventID, fingerprint)
	if grpcResp.GetDuplicate() {
		h.metrics.RecordDepositResult(metrics.ResultDuplicate)
	} else {
		h.metrics.RecordDepositResult(metrics.ResultAccepted)
	}

	if h.logger.Core().Enabled(zapcore.InfoLevel) {
		h.logger.Info("Deposit accepted by publisher",
			zap.String("request_id", reqID),
			zap.String("event_id", validatedEvent.EventID),
			zap.Uint64("stream_sequence", grpcResp.GetStreamSequence()),
			zap.Bool("duplicate", grpcResp.GetDuplicate()),
		)
	}

	c.JSON(http.StatusAccepted, CreateDepositResponse{
		EventID:        validatedEvent.EventID,
		Status:         "accepted",
		StreamSequence: grpcResp.GetStreamSequence(),
		Duplicate:      grpcResp.GetDuplicate(),
	})
}

func mapGRPCErrorToHTTPAndMetric(err error) (int, string, string, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "GATEWAY_TIMEOUT", "request timed out waiting for publisher", metrics.ResultUpstreamTimeout
	}
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected internal error", metrics.ResultInternalError
	}

	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, "VALIDATION_ERROR", st.Message(), metrics.ResultInvalid
	case codes.AlreadyExists:
		return http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", st.Message(), metrics.ResultInvalid
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "GATEWAY_TIMEOUT", "request timed out waiting for publisher", metrics.ResultUpstreamTimeout
	case codes.Unavailable, codes.Canceled:
		return http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "publisher or message broker is temporarily unavailable", metrics.ResultUpstreamUnavailable
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "RATE_LIMITED", st.Message(), metrics.ResultRateLimited
	default:
		return http.StatusBadGateway, "UPSTREAM_ERROR", "failed to process deposit upstream", metrics.ResultInternalError
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
