package server

import (
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
	// Even if apiKey is empty or Origin header is absent, mutating endpoints must reject unauthenticated requests
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
		// Intentionally no Origin header and no X-API-Key / session cookie
		w := httptest.NewRecorder()
		muxNoKey.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("endpoint %s: expected 401 Unauthorized without credentials (even with no Origin), got %d", ep, w.Code)
		}
	}

	// Verify with configured API key: Cross-Origin blocked (403), missing credentials blocked (401), wrong key blocked (401), valid key allowed
	srv := NewDashboardServer(nil, nil, "http://localhost:8081", "secret-api-key")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// 1. Cross-Origin POST must be rejected with 403 Forbidden
	reqCross := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/dlq/replay", strings.NewReader(`{}`))
	reqCross.Header.Set("Origin", "http://evil.example.com")
	wCross := httptest.NewRecorder()
	mux.ServeHTTP(wCross, reqCross)
	if wCross.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on cross-origin POST, got %d", wCross.Code)
	}

	// 2. Missing credentials without Origin header must be rejected with 401 Unauthorized
	reqNoAuth := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/publisher/toggle", strings.NewReader(`{}`))
	wNoAuth := httptest.NewRecorder()
	mux.ServeHTTP(wNoAuth, reqNoAuth)
	if wNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on unauthenticated POST without Origin, got %d", wNoAuth.Code)
	}

	// 3. Wrong X-API-Key header must be rejected with 401 Unauthorized
	reqBadKey := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/dlq/replay", strings.NewReader(`{}`))
	reqBadKey.Header.Set("X-API-Key", "wrong-key")
	wBadKey := httptest.NewRecorder()
	mux.ServeHTTP(wBadKey, reqBadKey)
	if wBadKey.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on wrong X-API-Key, got %d", wBadKey.Code)
	}

	// 4. Valid X-API-Key header on /api/publisher/toggle succeeds (200 OK)
	reqValidKey := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/publisher/toggle", strings.NewReader(`{}`))
	reqValidKey.Header.Set("X-API-Key", "secret-api-key")
	wValidKey := httptest.NewRecorder()
	mux.ServeHTTP(wValidKey, reqValidKey)
	if wValidKey.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with valid X-API-Key, got %d", wValidKey.Code)
	}

	// 5. Verify Prometheus HTTP metrics on dashboard
	if got := testutil.ToFloat64(srv.Metrics().HTTPRequestsTotal.WithLabelValues(http.MethodPost, "/api/publisher/toggle", "200")); got != 1 {
		t.Errorf("expected dashboard_http_requests_total{method=\"POST\",route=\"/api/publisher/toggle\",code=\"200\"} == 1, got %v", got)
	}

	// 6. Unmatched paths coalesce into route="unmatched"
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
