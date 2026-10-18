package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestCollector_ConcurrentIncrementsAndHTTPHandler(t *testing.T) {
	collector := NewCollector()

	const goroutines = 100
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.IncReceived()
			collector.IncProcessed()
			collector.IncDuplicate()
			collector.IncInvalid()
			collector.IncDLQ()
			collector.IncRetried()
		}()
	}

	wg.Wait()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	collector.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK, got %d", rec.Code)
	}

	var snap Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to decode metrics JSON response: %v", err)
	}

	if snap.MessagesReceived != goroutines ||
		snap.MessagesProcessed != goroutines ||
		snap.MessagesDuplicate != goroutines ||
		snap.MessagesInvalid != goroutines ||
		snap.MessagesDLQ != goroutines ||
		snap.MessagesRetried != goroutines {
		t.Errorf("unexpected metrics snapshot values: %+v", snap)
	}
}
