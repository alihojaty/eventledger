package healthmgr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestManager_UnifiedHealthSource(t *testing.T) {
	mgr := NewManager("pub-test-1", false, zap.NewNop())
	handler := mgr.NewHTTPHandler()
	ctx := context.Background()

	// 1. Initial state: Starting -> livez=200, readyz=503, gRPC=NOT_SERVING
	assertHTTPStatus(t, handler, "/livez", http.StatusOK)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// 2. NATS Connected -> Healthy -> livez=200, readyz=200, gRPC=SERVING
	mgr.SetNATSConnected(true)
	assertHTTPStatus(t, handler, "/livez", http.StatusOK)
	assertHTTPStatus(t, handler, "/readyz", http.StatusOK)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_SERVING)

	// 3. NATS Disconnected -> Degraded -> livez=200 (alive), readyz=503, gRPC=NOT_SERVING
	mgr.SetNATSConnected(false)
	assertHTTPStatus(t, handler, "/livez", http.StatusOK)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// 4. NATS Reconnected -> Healthy again
	mgr.SetNATSConnected(true)
	assertHTTPStatus(t, handler, "/readyz", http.StatusOK)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_SERVING)

	// 5. SIGTERM Draining -> Unhealthy across ALL three (/livez=503, /readyz=503, gRPC=NOT_SERVING)
	mgr.SetDraining()
	assertHTTPStatus(t, handler, "/livez", http.StatusServiceUnavailable)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Even if NATS reports connected during drain, state must remain Unhealthy
	mgr.SetNATSConnected(true)
	assertHTTPStatus(t, handler, "/livez", http.StatusServiceUnavailable)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	assertGRPCStatus(t, mgr, ctx, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

func assertHTTPStatus(t *testing.T, h http.Handler, path string, want int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != want {
		t.Fatalf("%s: expected HTTP %d, got %d (%s)", path, want, w.Code, w.Body.String())
	}
}

func assertGRPCStatus(t *testing.T, mgr *Manager, ctx context.Context, want grpc_health_v1.HealthCheckResponse_ServingStatus) {
	t.Helper()
	resp, err := mgr.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("unexpected gRPC Check error: %v", err)
	}
	if resp.GetStatus() != want {
		t.Fatalf("expected gRPC status %v, got %v", want, resp.GetStatus())
	}
}
