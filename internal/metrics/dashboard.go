package metrics

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/broker"
	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/alihojaty/reliable-deposit-processor/internal/repository"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed index.html
var dashboardFS embed.FS

type WorkerController interface {
	StopWorker(workerID int) error
	WorkerCount() int
	StatusMap() map[int]bool
}

type BalanceViewer interface {
	GetAllUserBalances(ctx context.Context) ([]repository.UserBalanceRow, error)
	GetRecentProcessedDeposits(ctx context.Context, limit int) ([]repository.ProcessedDepositItem, error)
	GetTotalProcessedCount(ctx context.Context) (uint64, error)
}

type QueueInspector interface {
	broker.Broker
	GetQueueStats(ctx context.Context) (broker.QueueStats, error)
}

// PublisherController رابط کنترل زنده سرویس Publisher از طریق داشبورد وب است.
type PublisherController interface {
	IsEnabled() bool
	SetEnabled(enable bool)
}

type DashboardDeps struct {
	workers       WorkerController
	broker        QueueInspector
	balanceViewer BalanceViewer
	publisher     PublisherController

	mu            sync.Mutex
	lastEventID   string
	lastEventJSON []byte
}

func (c *Collector) EnableDashboard(w WorkerController, b QueueInspector, bv BalanceViewer, pub PublisherController) {
	c.dashboard = &DashboardDeps{
		workers:       w,
		broker:        b,
		balanceViewer: bv,
		publisher:     pub,
	}
}

type workerStatusItem struct {
	ID     int  `json:"id"`
	Active bool `json:"active"`
}

type dashboardStateResponse struct {
	Metrics        Snapshot                          `json:"metrics"`
	Queue          broker.QueueStats                 `json:"queue"`
	PublishEnabled bool                              `json:"publish_enabled"`
	Workers        []workerStatusItem                `json:"workers"`
	Balances       []repository.UserBalanceRow       `json:"balances"`
	RecentDeposits []repository.ProcessedDepositItem `json:"recent_deposits"`
	DBHealthy      bool                              `json:"db_healthy"`
}

func (c *Collector) registerDashboardRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		htmlContent, err := dashboardFS.ReadFile("index.html")
		if err != nil {
			http.Error(w, "dashboard template not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(htmlContent)
	})

	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
		defer cancel()

		statusMap := c.dashboard.workers.StatusMap()
		workers := make([]workerStatusItem, 0, c.dashboard.workers.WorkerCount())
		for i := 1; i <= c.dashboard.workers.WorkerCount(); i++ {
			workers = append(workers, workerStatusItem{
				ID:     i,
				Active: statusMap[i],
			})
		}

		balances, err := c.dashboard.balanceViewer.GetAllUserBalances(ctx)
		dbHealthy := err == nil
		if balances == nil {
			balances = []repository.UserBalanceRow{}
		}

		recentDeposits, _ := c.dashboard.balanceViewer.GetRecentProcessedDeposits(ctx, 15)
		if recentDeposits == nil {
			recentDeposits = []repository.ProcessedDepositItem{}
		}

		qStats, _ := c.dashboard.broker.GetQueueStats(ctx)

		pubEnabled := false
		if c.dashboard.publisher != nil {
			pubEnabled = c.dashboard.publisher.IsEnabled()
		}

		resp := dashboardStateResponse{
			Metrics:        c.GetSnapshot(),
			Queue:          qStats,
			PublishEnabled: pubEnabled,
			Workers:        workers,
			Balances:       balances,
			RecentDeposits: recentDeposits,
			DBHealthy:      dbHealthy,
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(resp)
	})

	// وب‌سرویس تغییر زنده وضعیت PUBLISH_ENABLED با کلیک روی دکمه داخل داشبورد
	mux.HandleFunc("/api/publisher/toggle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if c.dashboard.publisher == nil {
			http.Error(w, "publisher controller not configured", http.StatusInternalServerError)
			return
		}

		newState := !c.dashboard.publisher.IsEnabled()
		c.dashboard.publisher.SetEnabled(newState)

		msg := "تولیدکننده خودکار (Publisher) متوقف شد (PUBLISH_ENABLED = false)."
		if newState {
			msg = "تولیدکننده خودکار (Publisher) به صورت زنده فعال شد (PUBLISH_ENABLED = true)!"
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"publish_enabled": newState,
			"message":         msg,
		})
	})

	mux.HandleFunc("/api/workers/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			WorkerID int `json:"worker_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := c.dashboard.workers.StopWorker(req.WorkerID); err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "error",
				"message": fmt.Sprintf("Worker %d is already stopped or invalid.", req.WorkerID),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"message": fmt.Sprintf("Worker %d gracefully drained and stopped.", req.WorkerID),
		})
	})

	mux.HandleFunc("/api/simulate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		switch req.Action {
		case "batch50":
			batchTag := uuid.NewString()[:4]
			for i := 1; i <= 50; i++ {
				evID := fmt.Sprintf("msg-%03d-%s", i, batchTag)
				ev := domain.DepositEvent{
					EventID:   evID,
					UserID:    (i % 20) + 1,
					Currency:  domain.CurrencyUSDT,
					Amount:    decimal.RequireFromString("10.00"),
					CreatedAt: time.Now().UTC(),
				}
				payload, _ := json.Marshal(ev)
				_, _ = c.dashboard.broker.PublishDepositAsync(evID, payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": "۵۰ پیام شماره‌گذاری‌شده (msg-001 تا msg-050) به استریم NATS ارسال شد!",
			})

		case "valid":
			ev := domain.DepositEvent{
				EventID:   "ui-valid-" + uuid.NewString()[:8],
				UserID:    7,
				Currency:  domain.CurrencyUSDT,
				Amount:    decimal.RequireFromString("100.00"),
				CreatedAt: time.Now().UTC(),
			}
			payload, _ := json.Marshal(ev)
			c.dashboard.mu.Lock()
			c.dashboard.lastEventID = ev.EventID
			c.dashboard.lastEventJSON = payload
			c.dashboard.mu.Unlock()

			if _, err := c.dashboard.broker.PublishDeposit(ctx, ev.EventID, payload); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": fmt.Sprintf("رویداد واریز 100 USDT برای کاربر 7 ارسال شد (ID: %s)", ev.EventID),
			})

		case "duplicate":
			c.dashboard.mu.Lock()
			lastPayload := c.dashboard.lastEventJSON
			c.dashboard.mu.Unlock()

			if len(lastPayload) == 0 {
				ev := domain.DepositEvent{
					EventID:   "ui-dup-" + uuid.NewString()[:8],
					UserID:    7,
					Currency:  domain.CurrencyUSDT,
					Amount:    decimal.RequireFromString("250.00"),
					CreatedAt: time.Now().UTC(),
				}
				lastPayload, _ = json.Marshal(ev)
				_, _ = c.dashboard.broker.PublishDeposit(ctx, ev.EventID, lastPayload)
			}
			_, err := c.dashboard.broker.PublishDeposit(ctx, "bypass-broker-dedup-"+uuid.NewString()[:8], lastPayload)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": "پیام تکراری ارسال شد! دیتابیس آن را مسدود کرده و شمارنده Duplicate یک واحد افزایش یافت.",
			})

		case "invalid":
			badID := "ui-bad-doge-" + uuid.NewString()[:8]
			badPayload := fmt.Sprintf(
				`{"event_id":%q,"user_id":7,"currency":"DOGE","amount":"999.00","created_at":%q}`,
				badID, time.Now().UTC().Format(time.RFC3339),
			)
			if _, err := c.dashboard.broker.PublishDeposit(ctx, badID, []byte(badPayload)); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": fmt.Sprintf("پیام نامعتبر با ارز DOGE ارسال شد و به صف DLQ منتقل گردید (ID: %s)", badID),
			})

		case "burst":
			batchPrefix := uuid.NewString()[:8]
			for i := 1; i <= 500; i++ {
				userID := (i % 20) + 1
				evID := fmt.Sprintf("ui-burst-%s-%d", batchPrefix, i)
				payload := fmt.Sprintf(
					`{"event_id":%q,"user_id":%d,"currency":"USDT","amount":"10.00","created_at":%q}`,
					evID, userID, time.Now().UTC().Format(time.RFC3339),
				)
				_, _ = c.dashboard.broker.PublishDepositAsync(evID, []byte(payload))
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": "رگبار ۵۰۰ واریز هم‌زمان (5,000 USDT) با موفقیت به استریم NATS تزریق شد!",
			})
		}
	})
}
