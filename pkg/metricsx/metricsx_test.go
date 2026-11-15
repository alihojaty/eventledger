package metricsx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewRegistry_IncludesBuildInfoAndStandardCollectors(t *testing.T) {
	t.Setenv("REVISION", "abc1234sha")
	t.Setenv("SERVICE_VERSION", "2.1.0")

	reg := NewRegistry("gateway", nil, "")

	c := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "test_events_total",
		Help:      "Total test events.",
	})
	reg.MustRegister(c)
	c.Add(7)

	if got := ReadCounterValue(c); got != 7 {
		t.Fatalf("expected ReadCounterValue=7, got %d", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	Handler(reg).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /metrics, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, `gateway_build_info{`) || !strings.Contains(body, `revision="abc1234sha"`) {
		t.Errorf("expected gateway_build_info with revision label in /metrics output, got:\n%s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("expected standard Go collector metrics in /metrics output")
	}
}

func TestStartServer_CleanShutdown(t *testing.T) {
	reg := NewRegistry("publisher", nil, "")
	srv := StartServer(0, reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
