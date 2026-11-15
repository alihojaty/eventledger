package server

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestValidateReplayPayload(t *testing.T) {
	validJSON := `{"event_id":"evt-fixed-1","user_id":7,"currency":"USDT","amount":"250.00","created_at":"2026-10-04T10:00:00Z"}`
	eventID, normalized, err := validateReplayPayload(validJSON)
	if err != nil {
		t.Fatalf("expected valid payload to pass, got error: %v", err)
	}
	if eventID != "evt-fixed-1" || len(normalized) == 0 {
		t.Fatalf("unexpected validation output: eventID=%q, normalized=%s", eventID, string(normalized))
	}

	invalidCases := []struct {
		name    string
		payload string
	}{
		{"unsupported currency DOGE", `{"event_id":"evt-1","user_id":7,"currency":"DOGE","amount":"250.00"}`},
		{"numeric amount instead of string", `{"event_id":"evt-1","user_id":7,"currency":"USDT","amount":250.00}`},
		{"negative amount", `{"event_id":"evt-1","user_id":7,"currency":"USDT","amount":"-10.00"}`},
		{"zero amount", `{"event_id":"evt-1","user_id":7,"currency":"USDT","amount":"0.000"}`},
		{"19 decimal places", `{"event_id":"evt-1","user_id":7,"currency":"BTC","amount":"0.1234567890123456789"}`},
		{"user_id out of range", `{"event_id":"evt-1","user_id":99,"currency":"USDT","amount":"10.00"}`},
		{"unknown field", `{"event_id":"evt-1","user_id":7,"currency":"USDT","amount":"10.00","extra":1}`},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := validateReplayPayload(tc.payload); err == nil {
				t.Errorf("case %q: expected validation error, got nil", tc.name)
			}
		})
	}
}

func TestDashboard_GuardPost_BlocksUnauthenticatedEvenWithoutOrigin(t *testing.T) {
	srvNoKey := NewDashboardServer(nil, nil, "http://localhost:8081", "")
	muxNoKey := http.NewServeMux()
	srvNoKey.RegisterRoutes(muxNoKey)

	mutatingEndpoints := []string{
		"/api/publisher/toggle",
		"/api/settlement/trigger",
		"/api/dlq/replay",
		"/api/dlq/inject",
		"/api/gateway/send",
		"/api/gateway/loadgen",
	}

	for _, ep := range mutatingEndpoints {
		req := httptest.NewRequest(http.MethodPost, "http://localhost:8080"+ep, strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		muxNoKey.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("endpoint %s: expected 401 Unauthorized without credentials (even with no Origin), got %d", ep, w.Code)
		}
	}

	srv := NewDashboardServer(nil, nil, "http://localhost:8081", "secret-api-key")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	reqCross := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/dlq/replay", strings.NewReader(`{}`))
	reqCross.Header.Set("Origin", "http://evil.example.com")
	wCross := httptest.NewRecorder()
	mux.ServeHTTP(wCross, reqCross)
	if wCross.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on cross-origin POST, got %d", wCross.Code)
	}

	reqNoAuth := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/publisher/toggle", strings.NewReader(`{}`))
	wNoAuth := httptest.NewRecorder()
	mux.ServeHTTP(wNoAuth, reqNoAuth)
	if wNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on unauthenticated POST without Origin, got %d", wNoAuth.Code)
	}

	reqBadKey := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/dlq/replay", strings.NewReader(`{}`))
	reqBadKey.Header.Set("X-API-Key", "wrong-key")
	wBadKey := httptest.NewRecorder()
	mux.ServeHTTP(wBadKey, reqBadKey)
	if wBadKey.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on wrong X-API-Key, got %d", wBadKey.Code)
	}

	reqValidKey := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/publisher/toggle", strings.NewReader(`{}`))
	reqValidKey.Header.Set("X-API-Key", "secret-api-key")
	wValidKey := httptest.NewRecorder()
	mux.ServeHTTP(wValidKey, reqValidKey)
	if wValidKey.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with valid X-API-Key, got %d", wValidKey.Code)
	}

	if got := testutil.ToFloat64(srv.Metrics().HTTPRequestsTotal.WithLabelValues(http.MethodPost, "/api/publisher/toggle", "200")); got != 1 {
		t.Errorf("expected dashboard_http_requests_total{method=\"POST\",route=\"/api/publisher/toggle\",code=\"200\"} == 1, got %v", got)
	}

	req404 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/random-unknown-path-123", nil)
	w404 := httptest.NewRecorder()
	mux.ServeHTTP(w404, req404)
	if w404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown path, got %d", w404.Code)
	}
	if got := testutil.ToFloat64(srv.Metrics().HTTPRequestsTotal.WithLabelValues(http.MethodGet, routeUnmatched, "404")); got != 1 {
		t.Errorf("expected dashboard_http_requests_total{method=\"GET\",route=\"unmatched\",code=\"404\"} == 1, got %v", got)
	}
}

func TestDashboard_FetchPrometheusTelemetry_HandlesNaNAndTargets(t *testing.T) {
	if got := sanitizeFloat(math.NaN()); got != 0 {
		t.Fatalf("expected sanitizeFloat(NaN) == 0, got %v", got)
	}
	if got := sanitizeFloat(math.Inf(1)); got != 0 {
		t.Fatalf("expected sanitizeFloat(+Inf) == 0, got %v", got)
	}

	fakeProm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/api/v1/targets") {
			_, _ = w.Write([]byte(`{"status":"success","data":{"activeTargets":[{"health":"up"},{"health":"up"}]}}`))
			return
		}
		q := r.URL.Query().Get("query")
		if strings.Contains(q, "prometheus_tsdb_head_series") {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"value":[1700000000,"1420"]}]}}`))
			return
		}
		// Return NaN to ensure sanitizeFloat converts idle histogram_quantile NaN to 0
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"value":[1700000000,"NaN"]}]}}`))
	}))
	defer fakeProm.Close()

	t.Setenv("PROMETHEUS_URL", fakeProm.URL)
	srv := NewDashboardServer(nil, nil, "http://localhost:8081", "secret-api-key")

	snap := srv.fetchPrometheusTelemetry(context.Background())
	if !snap.Online {
		t.Fatalf("expected PrometheusTelemetry.Online == true")
	}
	if snap.TargetsUp != 2 || snap.TargetsTotal != 2 {
		t.Fatalf("expected 2/2 targets up, got %d/%d", snap.TargetsUp, snap.TargetsTotal)
	}
	if snap.TSDBHeadSeries != 1420 {
		t.Fatalf("expected TSDBHeadSeries == 1420, got %d", snap.TSDBHeadSeries)
	}
	if snap.GatewayP99Ms != 0 {
		t.Fatalf("expected NaN GatewayP99Ms to be sanitized to 0, got %v", snap.GatewayP99Ms)
	}
}
