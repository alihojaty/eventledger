package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Collector struct {
	received          atomic.Uint64
	processed         atomic.Uint64
	duplicate         atomic.Uint64
	invalid           atomic.Uint64
	dlq               atomic.Uint64
	retried           atomic.Uint64
	restoredAtStartup atomic.Uint64

	dashboard *DashboardDeps
}

type Snapshot struct {
	MessagesReceived  uint64 `json:"messages_received"`
	MessagesProcessed uint64 `json:"messages_processed"`
	MessagesDuplicate uint64 `json:"messages_duplicate"`
	MessagesInvalid   uint64 `json:"messages_invalid"`
	MessagesDLQ       uint64 `json:"messages_dlq"`
	MessagesRetried   uint64 `json:"messages_retried"`
	RestoredFromDB    uint64 `json:"restored_from_db"`
}

func NewCollector() *Collector {
	return &Collector{}
}

// RestoreState شمارنده‌های اولیه را در بدو بالا آمدن برنامه از دیتابیس و بروکر بازیابی می‌کند
// تا پس از Restart، داشبورد از عدد واقعی قبلی (مثلاً ۲۴) به سمت ۲۵ ادامه دهد!
func (c *Collector) RestoreState(processedInDB, dlqInBroker uint64) {
	c.restoredAtStartup.Store(processedInDB)
	c.processed.Store(processedInDB)
	c.dlq.Store(dlqInBroker)
	c.invalid.Store(dlqInBroker)
	c.received.Store(processedInDB + dlqInBroker)
}

func (c *Collector) IncReceived()  { c.received.Add(1) }
func (c *Collector) IncProcessed() { c.processed.Add(1) }
func (c *Collector) IncDuplicate() { c.duplicate.Add(1) }
func (c *Collector) IncInvalid()   { c.invalid.Add(1) }
func (c *Collector) IncDLQ()       { c.dlq.Add(1) }
func (c *Collector) IncRetried()   { c.retried.Add(1) }

func (c *Collector) GetSnapshot() Snapshot {
	return Snapshot{
		MessagesReceived:  c.received.Load(),
		MessagesProcessed: c.processed.Load(),
		MessagesDuplicate: c.duplicate.Load(),
		MessagesInvalid:   c.invalid.Load(),
		MessagesDLQ:       c.dlq.Load(),
		MessagesRetried:   c.retried.Load(),
		RestoredFromDB:    c.restoredAtStartup.Load(),
	}
}

func (c *Collector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		snap := c.GetSnapshot()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(snap)
	}
}

func (c *Collector) StartServer(ctx context.Context, port int, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", c.Handler())

	if c.dashboard != nil {
		c.registerDashboardRoutes(mux)
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("Web Dashboard & Metrics Server listening",
		zap.String("dashboard_url", fmt.Sprintf("http://localhost:%d", port)),
		zap.String("metrics_url", fmt.Sprintf("http://localhost:%d/metrics", port)),
	)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server failed", zap.Error(err))
	}
}
