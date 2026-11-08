package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestDashboard_GuardPost_BlocksCrossOriginAndInvalidAPIKey(t *testing.T) {
	// Pass nil nats conn; guard validations occur prior to jetstream operations
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

	// 2. Wrong X-API-Key header must be rejected with 401 Unauthorized
	reqBadKey := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/dlq/replay", strings.NewReader(`{}`))
	reqBadKey.Header.Set("X-API-Key", "wrong-key")
	wBadKey := httptest.NewRecorder()
	mux.ServeHTTP(wBadKey, reqBadKey)
	if wBadKey.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on wrong X-API-Key, got %d", wBadKey.Code)
	}
}
