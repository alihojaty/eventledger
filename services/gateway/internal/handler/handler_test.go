package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakePublisherServer struct {
	depositv1.UnimplementedDepositPublisherServiceServer
	fn func(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error)
}

func (f *fakePublisherServer) PublishDeposit(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
	return f.fn(ctx, req)
}

func setupBufconnGatewayWithMetrics(
	t *testing.T,
	fn func(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error),
) (http.Handler, *metrics.GatewayMetrics) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	depositv1.RegisterDepositPublisherServiceServer(grpcSrv, &fakePublisherServer{fn: fn})

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcSrv.Stop()
		_ = lis.Close()
	})

	reg := prometheus.NewRegistry()
	gwMetrics := metrics.NewGatewayMetrics(reg)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(gwMetrics.GRPCClientMetrics.UnaryClientInterceptor()),
	)
	if err != nil {
		t.Fatalf("failed to create bufconn client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	gw := NewGatewayHandler(
		depositv1.NewDepositPublisherServiceClient(conn),
		grpc_health_v1.NewHealthClient(conn),
		"secret-test-key",
		500*time.Millisecond,
		zap.NewNop(),
		WithMetrics(gwMetrics),
	)
	return gw.NewRouter(), gwMetrics
}

func setupBufconnGateway(
	t *testing.T,
	fn func(ctx context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error),
) http.Handler {
	router, _ := setupBufconnGatewayWithMetrics(t, fn)
	return router
}

func TestGateway_ValidDeposit_Returns202(t *testing.T) {
	router, gwMetrics := setupBufconnGatewayWithMetrics(t, func(_ context.Context, _ *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
		return &depositv1.PublishDepositResponse{StreamSequence: 101, Duplicate: false}, nil
	})

	body := `{"user_id": 7, "currency": "USDT", "amount": "125.50"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body))
	req.Header.Set(HeaderAPIKey, "secret-test-key")
	req.Header.Set(HeaderIdempotencyKey, "idem-key-001")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateDepositResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.EventID != "idem-key-001" || resp.StreamSequence != 101 || resp.Duplicate {
		t.Errorf("unexpected response payload: %+v", resp)
	}

	if got := testutil.ToFloat64(gwMetrics.DepositRequestsTotal.WithLabelValues(metrics.ResultAccepted)); got != 1 {
		t.Errorf("expected gateway_deposit_requests_total{result=\"accepted\"} == 1, got %v", got)
	}
	if got := testutil.ToFloat64(gwMetrics.HTTPRequestsTotal.WithLabelValues(http.MethodPost, "/v1/deposits", "202")); got != 1 {
		t.Errorf("expected gateway_http_requests_total{method=\"POST\",route=\"/v1/deposits\",code=\"202\"} == 1, got %v", got)
	}
}

func TestGateway_PrometheusMetrics_DuplicateAndUnmatchedRouteCardinality(t *testing.T) {
	seen := make(map[string]bool)
	router, gwMetrics := setupBufconnGatewayWithMetrics(t, func(_ context.Context, req *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
		dup := seen[req.GetEventId()]
		seen[req.GetEventId()] = true
		return &depositv1.PublishDepositResponse{StreamSequence: 55, Duplicate: dup}, nil
	})

	body := `{"user_id": 7, "currency": "USDT", "amount": "125.50"}`

	// 1. First request -> increments result="accepted"
	req1 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body))
	req1.Header.Set(HeaderAPIKey, "secret-test-key")
	req1.Header.Set(HeaderIdempotencyKey, "idem-dup-metric-1")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	// 2. Duplicate retry -> increments result="duplicate" by exactly 1
	req2 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body))
	req2.Header.Set(HeaderAPIKey, "secret-test-key")
	req2.Header.Set(HeaderIdempotencyKey, "idem-dup-metric-1")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if got := testutil.ToFloat64(gwMetrics.DepositRequestsTotal.WithLabelValues(metrics.ResultAccepted)); got != 1 {
		t.Fatalf("expected accepted=1, got %v", got)
	}
	if got := testutil.ToFloat64(gwMetrics.DepositRequestsTotal.WithLabelValues(metrics.ResultDuplicate)); got != 1 {
		t.Fatalf("expected duplicate=1, got %v", got)
	}

	// 3. Random unknown paths must map to route="unmatched" and /metrics must not exist on public router
	for _, path := range []string{"/v1/random-a8f3", "/v1/random-99b1", "/metrics"} {
		req404 := httptest.NewRequest(http.MethodGet, path, nil)
		w404 := httptest.NewRecorder()
		router.ServeHTTP(w404, req404)
		if w404.Code != http.StatusNotFound {
			t.Fatalf("expected 404 on %s, got %d", path, w404.Code)
		}
	}

	if got := testutil.ToFloat64(gwMetrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, RouteUnmatched, "404")); got != 3 {
		t.Fatalf("expected 3 unmatched 404 requests coalesced under route=\"unmatched\", got %v", got)
	}
}

func TestGateway_IdempotencyKeyConflict_409(t *testing.T) {
	router := setupBufconnGateway(t, func(_ context.Context, _ *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
		return &depositv1.PublishDepositResponse{StreamSequence: 102, Duplicate: false}, nil
	})

	// 1. First request with idem-conflict-1 -> 202 Accepted
	body1 := `{"user_id": 7, "currency": "USDT", "amount": "100.00"}`
	req1 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body1))
	req1.Header.Set(HeaderAPIKey, "secret-test-key")
	req1.Header.Set(HeaderIdempotencyKey, "idem-conflict-1")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on first request, got %d", w1.Code)
	}

	// 2. Retry with identical payload -> 202 Accepted
	req2 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body1))
	req2.Header.Set(HeaderAPIKey, "secret-test-key")
	req2.Header.Set(HeaderIdempotencyKey, "idem-conflict-1")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on identical retry, got %d", w2.Code)
	}

	// 3. Reuse same Idempotency-Key with different amount -> 409 Conflict
	bodyDiff := `{"user_id": 7, "currency": "USDT", "amount": "999.00"}`
	req3 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(bodyDiff))
	req3.Header.Set(HeaderAPIKey, "secret-test-key")
	req3.Header.Set(HeaderIdempotencyKey, "idem-conflict-1")
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, req3)
	if w3.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when reusing Idempotency-Key with different amount, got %d: %s",
			w3.Code, w3.Body.String())
	}
}

func TestGateway_Authentication_401(t *testing.T) {
	router, gwMetrics := setupBufconnGatewayWithMetrics(t, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(`{}`))
	req.Header.Set(HeaderAPIKey, "wrong-key")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", w.Code)
	}
	if got := testutil.ToFloat64(gwMetrics.DepositRequestsTotal.WithLabelValues(metrics.ResultUnauthorized)); got != 1 {
		t.Errorf("expected unauthorized metric == 1, got %v", got)
	}
}

func TestGateway_BodyLimit_413(t *testing.T) {
	router, gwMetrics := setupBufconnGatewayWithMetrics(t, nil)

	largePayload := `{"user_id": 7, "currency": "USDT", "amount": "` + strings.Repeat("1", 20*1024) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/deposits", bytes.NewReader([]byte(largePayload)))
	req.Header.Set(HeaderAPIKey, "secret-test-key")
	req.Header.Set(HeaderIdempotencyKey, "idem-large-1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Request Entity Too Large, got %d", w.Code)
	}
	if got := testutil.ToFloat64(gwMetrics.DepositRequestsTotal.WithLabelValues(metrics.ResultPayloadTooLarge)); got != 1 {
		t.Errorf("expected payload_too_large metric == 1, got %v", got)
	}
}

func TestGateway_ValidationErrors_400(t *testing.T) {
	router := setupBufconnGateway(t, nil)

	cases := []struct {
		name    string
		idemKey string
		body    string
	}{
		{"missing idempotency key", "", `{"user_id": 7, "currency": "USDT", "amount": "10.00"}`},
		{"too long idempotency key", strings.Repeat("a", 129), `{"user_id": 7, "currency": "USDT", "amount": "10.00"}`},
		{"negative amount", "idem-1", `{"user_id": 7, "currency": "USDT", "amount": "-5.00"}`},
		{"non-numeric amount", "idem-2", `{"user_id": 7, "currency": "USDT", "amount": "abc"}`},
		{"json number instead of string", "idem-3", `{"user_id": 7, "currency": "USDT", "amount": 125.50}`},
		{"19 decimal places", "idem-4", `{"user_id": 7, "currency": "BTC", "amount": "0.1234567890123456789"}`},
		{"unsupported currency", "idem-5", `{"user_id": 7, "currency": "DOGE", "amount": "10.00"}`},
		{"user_id out of range", "idem-6", `{"user_id": 999, "currency": "USDT", "amount": "10.00"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(tc.body))
			req.Header.Set(HeaderAPIKey, "secret-test-key")
			if tc.idemKey != "" {
				req.Header.Set(HeaderIdempotencyKey, tc.idemKey)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("case %q: expected 400 Bad Request, got %d (%s)", tc.name, w.Code, w.Body.String())
			}
		})
	}
}

func TestGateway_GRPCErrorMapping_503And504(t *testing.T) {
	router503, gwMetrics503 := setupBufconnGatewayWithMetrics(t, func(_ context.Context, _ *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
		return nil, status.Error(codes.Unavailable, "nats down")
	})

	body := `{"user_id": 7, "currency": "USDT", "amount": "10.00"}`
	req1 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body))
	req1.Header.Set(HeaderAPIKey, "secret-test-key")
	req1.Header.Set(HeaderIdempotencyKey, "idem-503")
	w1 := httptest.NewRecorder()
	router503.ServeHTTP(w1, req1)

	if w1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", w1.Code)
	}
	if got := testutil.ToFloat64(gwMetrics503.DepositRequestsTotal.WithLabelValues(metrics.ResultUpstreamUnavailable)); got != 1 {
		t.Errorf("expected upstream_unavailable == 1, got %v", got)
	}

	router504, gwMetrics504 := setupBufconnGatewayWithMetrics(t, func(_ context.Context, _ *depositv1.PublishDepositRequest) (*depositv1.PublishDepositResponse, error) {
		return nil, status.Error(codes.DeadlineExceeded, "timeout")
	})

	req2 := httptest.NewRequest(http.MethodPost, "/v1/deposits", strings.NewReader(body))
	req2.Header.Set(HeaderAPIKey, "secret-test-key")
	req2.Header.Set(HeaderIdempotencyKey, "idem-504")
	w2 := httptest.NewRecorder()
	router504.ServeHTTP(w2, req2)

	if w2.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 Gateway Timeout, got %d", w2.Code)
	}
	if got := testutil.ToFloat64(gwMetrics504.DepositRequestsTotal.WithLabelValues(metrics.ResultUpstreamTimeout)); got != 1 {
		t.Errorf("expected upstream_timeout == 1, got %v", got)
	}
}
