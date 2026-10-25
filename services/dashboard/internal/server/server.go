package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamDeposits       = "DEPOSITS"
	subjectDepositEvents = "deposits.events"
	streamDepositsDLQ    = "DEPOSITS_DLQ"
)

type DepositorTelemetry struct {
	InstanceID      string    `json:"instance_id"`
	WorkerCount     int       `json:"worker_count"`
	ProcessedTotal  uint64    `json:"processed_total"`
	FailedTotal     uint64    `json:"failed_total"`
	DuplicatesTotal uint64    `json:"duplicates_total"`
	CurrentTPS      float64   `json:"current_tps"`
	Timestamp       time.Time `json:"timestamp"`
	LastSeen        time.Time `json:"-"`
}

type LedgerReconciliationReport struct {
	IsBalanced       bool                    `json:"is_balanced"`
	DiscrepancyCount int                     `json:"discrepancy_count"`
	TotalLedgerUSDT  string                  `json:"total_ledger_usdt"`
	TotalLedgerBTC   string                  `json:"total_ledger_btc"`
	TotalLedgerIRT   string                  `json:"total_ledger_irt"`
	CheckedAt        string                  `json:"checked_at"`
	Discrepancies    []LedgerDiscrepancyItem `json:"discrepancies"`
}

type LedgerDiscrepancyItem struct {
	UserID          int    `json:"user_id"`
	Currency        string `json:"currency"`
	ExpectedBalance string `json:"expected_balance"`
	ActualBalance   string `json:"actual_balance"`
	Difference      string `json:"difference"`
}

type DLQInspectorItem struct {
	DLQSequence    uint64 `json:"dlq_sequence"`
	EventID        string `json:"event_id"`
	ErrorCode      string `json:"error_code"`
	SourceStream   string `json:"source_stream"`
	Subject        string `json:"subject"`
	StreamSequence uint64 `json:"stream_sequence"`
	OriginalEvent  string `json:"original_event"`
	Reason         string `json:"reason"`
	Timestamp      string `json:"timestamp"`
}

type AggregatedResponse struct {
	ActiveReplicas  int                        `json:"active_replicas"`
	TotalWorkers    int                        `json:"total_workers"`
	GlobalTPS       float64                    `json:"global_tps"`
	GlobalProcessed uint64                     `json:"global_processed"`
	GlobalFailed    uint64                     `json:"global_failed"`
	GlobalDuplicate uint64                     `json:"global_duplicates"`
	DBTotalDeposits int64                      `json:"db_total_deposits"`
	DLQTotalCount   uint64                     `json:"dlq_total_count"`
	PublishEnabled  bool                       `json:"publish_enabled"`
	Reconciliation  LedgerReconciliationReport `json:"reconciliation"`
	Replicas        []DepositorTelemetry       `json:"replicas"`
	RecentDeposits  []RecentDepositItem        `json:"recent_deposits"`
	DLQMessages     []DLQInspectorItem         `json:"dlq_messages"`
	Balances        []UserBalanceRow           `json:"balances"`
}

type RecentDepositItem struct {
	OrderNum    int64  `json:"order_num"`
	EventID     string `json:"event_id"`
	UserID      int    `json:"user_id"`
	Currency    string `json:"currency"`
	Amount      string `json:"amount"`
	ProcessedAt string `json:"processed_at"`
}

type UserBalanceRow struct {
	UserID    int    `json:"user_id"`
	USDT      string `json:"usdt"`
	BTC       string `json:"btc"`
	IRT       string `json:"irt"`
	UpdatedAt string `json:"updated_at"`
}

type DashboardServer struct {
	mu             sync.RWMutex
	replicas       map[string]DepositorTelemetry
	nc             *nats.Conn
	js             jetstream.JetStream
	db             *sql.DB
	publishEnabled atomic.Bool
}

func NewDashboardServer(nc *nats.Conn, db *sql.DB) *DashboardServer {
	js, _ := jetstream.New(nc)
	s := &DashboardServer{
		replicas: make(map[string]DepositorTelemetry),
		nc:       nc,
		js:       js,
		db:       db,
	}
	s.publishEnabled.Store(true)
	return s
}

func (s *DashboardServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleUI)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/publisher/toggle", s.handleTogglePublisher)
	mux.HandleFunc("/api/dlq/replay", s.handleReplayDLQ)
	mux.HandleFunc("/api/dlq/inject", s.handleInjectInvalidEvent)
}

func (s *DashboardServer) StartNATSListener(ctx context.Context) error {
	_, err := s.nc.Subscribe("telemetry.depositor", func(msg *nats.Msg) {
		var t DepositorTelemetry
		if err := json.Unmarshal(msg.Data, &t); err != nil {
			return
		}
		t.LastSeen = time.Now()

		s.mu.Lock()
		s.replicas[t.InstanceID] = t
		s.mu.Unlock()
	})
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.mu.Lock()
				for id, rep := range s.replicas {
					if now.Sub(rep.LastSeen) > 5*time.Second {
						delete(s.replicas, id)
					}
				}
				s.mu.Unlock()
			}
		}
	}()

	return nil
}

func (s *DashboardServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	replicasList := make([]DepositorTelemetry, 0, len(s.replicas))
	var totalWorkers int
	var globalTPS float64
	var globalProc, globalFail, globalDup uint64

	for _, rep := range s.replicas {
		replicasList = append(replicasList, rep)
		totalWorkers += rep.WorkerCount
		globalTPS += rep.CurrentTPS
		globalProc += rep.ProcessedTotal
		globalFail += rep.FailedTotal
		globalDup += rep.DuplicatesTotal
	}
	s.mu.RUnlock()

	sort.Slice(replicasList, func(i, j int) bool {
		return replicasList[i].InstanceID < replicasList[j].InstanceID
	})

	var dbCount int64
	var recentDeposits []RecentDepositItem
	var balances []UserBalanceRow
	reconReport := LedgerReconciliationReport{
		IsBalanced:      true,
		TotalLedgerUSDT: "0",
		TotalLedgerBTC:  "0",
		TotalLedgerIRT:  "0",
		CheckedAt:       time.Now().Format("15:04:05"),
		Discrepancies:   []LedgerDiscrepancyItem{},
	}

	if s.db != nil {
		_ = s.db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM deposits").Scan(&dbCount)

		rows, err := s.db.QueryContext(r.Context(), `
			SELECT ROW_NUMBER() OVER (ORDER BY processed_at DESC) as rn, event_id, user_id, currency, ROUND(amount, 8)::TEXT, TO_CHAR(processed_at, 'YYYY-MM-DD HH24:MI:SS') 
			FROM deposits ORDER BY processed_at DESC LIMIT 15
		`)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var item RecentDepositItem
				_ = rows.Scan(&item.OrderNum, &item.EventID, &item.UserID, &item.Currency, &item.Amount, &item.ProcessedAt)
				recentDeposits = append(recentDeposits, item)
			}
		}

		balRows, err := s.db.QueryContext(r.Context(), `
			SELECT user_id, 
			       COALESCE(MAX(CASE WHEN currency='USDT' THEN balance END), 0)::TEXT,
			       COALESCE(MAX(CASE WHEN currency='BTC' THEN balance END), 0)::TEXT,
			       COALESCE(MAX(CASE WHEN currency='IRT' THEN balance END), 0)::TEXT,
			       TO_CHAR(MAX(updated_at), 'HH24:MI:SS')
			FROM balances GROUP BY user_id ORDER BY user_id ASC
		`)
		if err == nil {
			defer balRows.Close()
			for balRows.Next() {
				var b UserBalanceRow
				_ = balRows.Scan(&b.UserID, &b.USDT, &b.BTC, &b.IRT, &b.UpdatedAt)
				balances = append(balances, b)
			}
		}

		reconReport = s.runLedgerReconciliation(r.Context())
	}

	dlqTotal, dlqItems := s.fetchDLQMessages(r.Context(), 15)

	if recentDeposits == nil {
		recentDeposits = []RecentDepositItem{}
	}
	if balances == nil {
		balances = []UserBalanceRow{}
	}

	resp := AggregatedResponse{
		ActiveReplicas:  len(replicasList),
		TotalWorkers:    totalWorkers,
		GlobalTPS:       globalTPS,
		GlobalProcessed: globalProc,
		GlobalFailed:    globalFail,
		GlobalDuplicate: globalDup,
		DBTotalDeposits: dbCount,
		DLQTotalCount:   dlqTotal,
		PublishEnabled:  s.publishEnabled.Load(),
		Reconciliation:  reconReport,
		Replicas:        replicasList,
		RecentDeposits:  recentDeposits,
		DLQMessages:     dlqItems,
		Balances:        balances,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *DashboardServer) runLedgerReconciliation(ctx context.Context) LedgerReconciliationReport {
	report := LedgerReconciliationReport{
		IsBalanced:      true,
		TotalLedgerUSDT: "0",
		TotalLedgerBTC:  "0",
		TotalLedgerIRT:  "0",
		CheckedAt:       time.Now().Format("15:04:05"),
		Discrepancies:   []LedgerDiscrepancyItem{},
	}

	_ = s.db.QueryRowContext(ctx, `
		SELECT 
			COALESCE(SUM(CASE WHEN currency='USDT' THEN amount END), 0)::TEXT,
			COALESCE(SUM(CASE WHEN currency='BTC'  THEN amount END), 0)::TEXT,
			COALESCE(SUM(CASE WHEN currency='IRT'  THEN amount END), 0)::TEXT
		FROM deposits
	`).Scan(&report.TotalLedgerUSDT, &report.TotalLedgerBTC, &report.TotalLedgerIRT)

	const reconQuery = `
		WITH expected AS (
			SELECT user_id, currency, SUM(amount) AS expected_amount
			FROM deposits
			GROUP BY user_id, currency
		)
		SELECT
			COALESCE(e.user_id, b.user_id) AS user_id,
			COALESCE(e.currency, b.currency) AS currency,
			COALESCE(e.expected_amount, 0)::TEXT AS expected_balance,
			COALESCE(b.balance, 0)::TEXT AS actual_balance,
			(COALESCE(b.balance, 0) - COALESCE(e.expected_amount, 0))::TEXT AS difference
		FROM expected e
		FULL OUTER JOIN balances b
			ON b.user_id = e.user_id AND b.currency = e.currency
		WHERE COALESCE(e.expected_amount, 0) IS DISTINCT FROM COALESCE(b.balance, 0)
		ORDER BY user_id ASC, currency ASC
	`

	rows, err := s.db.QueryContext(ctx, reconQuery)
	if err != nil {
		return report
	}
	defer rows.Close()

	for rows.Next() {
		var item LedgerDiscrepancyItem
		if err := rows.Scan(&item.UserID, &item.Currency, &item.ExpectedBalance, &item.ActualBalance, &item.Difference); err == nil {
			report.Discrepancies = append(report.Discrepancies, item)
		}
	}

	report.DiscrepancyCount = len(report.Discrepancies)
	report.IsBalanced = report.DiscrepancyCount == 0
	return report
}

func (s *DashboardServer) fetchDLQMessages(ctx context.Context, limit int) (uint64, []DLQInspectorItem) {
	items := []DLQInspectorItem{}
	if s.js == nil {
		return 0, items
	}

	stream, err := s.js.Stream(ctx, streamDepositsDLQ)
	if err != nil {
		return 0, items
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, items
	}

	totalMsgs := info.State.Msgs
	if totalMsgs == 0 || info.State.LastSeq == 0 {
		return 0, items
	}

	count := 0
	for seq := info.State.LastSeq; seq >= info.State.FirstSeq && count < limit; seq-- {
		rawMsg, err := stream.GetMsg(ctx, seq)
		if err == nil {
			var parsed struct {
				EventID        string    `json:"event_id"`
				ErrorCode      string    `json:"error_code"`
				SourceStream   string    `json:"source_stream"`
				Subject        string    `json:"subject"`
				StreamSequence uint64    `json:"stream_sequence"`
				OriginalEvent  string    `json:"original_event"`
				Reason         string    `json:"reason"`
				Timestamp      time.Time `json:"timestamp"`
			}

			if err := json.Unmarshal(rawMsg.Data, &parsed); err == nil {
				items = append(items, DLQInspectorItem{
					DLQSequence:    rawMsg.Sequence,
					EventID:        parsed.EventID,
					ErrorCode:      parsed.ErrorCode,
					SourceStream:   parsed.SourceStream,
					Subject:        parsed.Subject,
					StreamSequence: parsed.StreamSequence,
					OriginalEvent:  parsed.OriginalEvent,
					Reason:         parsed.Reason,
					Timestamp:      parsed.Timestamp.Format("2006-01-02 15:04:05"),
				})
				count++
			}
		}

		if seq == 0 {
			break
		}
	}

	return totalMsgs, items
}

func (s *DashboardServer) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DLQSequence uint64 `json:"dlq_sequence"`
		NewPayload  string `json:"new_payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if s.js == nil {
		http.Error(w, "jetstream not available", http.StatusServiceUnavailable)
		return
	}

	replayMsgID := fmt.Sprintf("replay-dlq-%d-%d", req.DLQSequence, time.Now().UnixNano())

	ack, err := s.js.Publish(
		r.Context(),
		subjectDepositEvents,
		[]byte(req.NewPayload),
		jetstream.WithMsgID(replayMsgID),
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to publish replayed event: %v", err), http.StatusInternalServerError)
		return
	}

	if req.DLQSequence > 0 {
		if dlqStream, err := s.js.Stream(r.Context(), streamDepositsDLQ); err == nil {
			_ = dlqStream.DeleteMsg(r.Context(), req.DLQSequence)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"replayed":       true,
		"dlq_sequence":   req.DLQSequence,
		"new_stream_seq": ack.Sequence,
	})
}

func (s *DashboardServer) handleInjectInvalidEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.js == nil {
		http.Error(w, "jetstream not available", http.StatusServiceUnavailable)
		return
	}

	badEventID := fmt.Sprintf("evt-poison-%d", time.Now().UnixNano()%100000)
	poisonPayload := fmt.Sprintf(
		`{"event_id":%q,"user_id":7,"currency":"DOGE","amount":"250.00","created_at":%q}`,
		badEventID,
		time.Now().UTC().Format(time.RFC3339),
	)

	_, err := s.js.Publish(r.Context(), subjectDepositEvents, []byte(poisonPayload), jetstream.WithMsgID(badEventID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"injected_event_id": badEventID})
}

func (s *DashboardServer) handleTogglePublisher(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	newState := !s.publishEnabled.Load()
	s.publishEnabled.Store(newState)

	payload, _ := json.Marshal(map[string]bool{"publish_enabled": newState})
	_ = s.nc.Publish("control.publisher", payload)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"publish_enabled": newState})
}

func (s *DashboardServer) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Enterprise Cluster Dashboard — Deposit Processor</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
<style>
  body { font-family: 'Vazirmatn', sans-serif; background: #0b0f19; color: #f8fafc; }
  .ltr-num { direction: ltr; display: inline-block; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
  .card-glow { background: linear-gradient(135deg, rgba(30,41,59,0.7) 0%, rgba(15,23,42,0.9) 100%); }
</style>
</head>
<body class="min-h-screen p-4 md:p-6">
<div class="max-w-7xl mx-auto space-y-6">

  <!-- Header -->
  <div class="flex flex-col lg:flex-row items-center justify-between card-glow border border-slate-800/80 rounded-2xl p-5 shadow-2xl gap-4">
    <div class="flex items-center gap-4">
      <div class="w-14 h-14 rounded-2xl bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center text-3xl shadow-inner">⚡</div>
      <div>
        <h1 class="text-xl font-extrabold text-white">مرکز کنترل مانیتورینگ خوشه‌ای (Cluster Control Center)</h1>
        <p class="text-xs text-slate-400 mt-1">NATS Consumer Grouping &bull; Automated Ledger Reconciliation &bull; DLQ Inspector & Replay</p>
      </div>
    </div>
    <div class="flex flex-wrap items-center gap-3">
      <button onclick="injectPoisonMessage()" class="px-3.5 py-2.5 rounded-xl text-xs font-bold transition shadow-md bg-amber-600/90 text-white hover:bg-amber-500 cursor-pointer">
        🧪 تزریق پیام خراب تستی (Poison Pill)
      </button>
      <button id="pubBtn" class="px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-md flex items-center gap-2 cursor-pointer bg-emerald-600 text-white hover:bg-emerald-500" onclick="togglePublisher()">
        <span>Publisher: Running</span>
      </button>
      <div class="bg-slate-950 border border-slate-800 px-4 py-2 rounded-xl text-center">
        <span class="text-[10px] text-slate-400 block">کل سرعت خوشه (Cluster TPS)</span>
        <span id="tps" class="text-base font-bold text-emerald-400 ltr-num">0 TPS</span>
      </div>
    </div>
  </div>

  <!-- Global Cluster Metric Cards -->
  <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Active Replicas</h4>
      <div class="text-2xl font-black text-sky-400 ltr-num" id="replicas">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Total Workers</h4>
      <div class="text-2xl font-black text-indigo-400 ltr-num" id="workers">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Global Processed</h4>
      <div class="text-2xl font-black text-emerald-400 ltr-num" id="processed">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Duplicates Blocked</h4>
      <div class="text-2xl font-black text-amber-400 ltr-num" id="duplicates">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">DLQ Messages</h4>
      <div class="text-2xl font-black text-rose-400 ltr-num" id="dlqCount">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">DB Total Records</h4>
      <div class="text-2xl font-black text-teal-400 ltr-num" id="dbTotal">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Ledger Audit</h4>
      <div class="text-sm font-bold text-emerald-400 mt-2" id="reconBadge">Verified ✅</div>
    </div>
  </div>

  <!-- Automated Ledger Reconciliation Panel (Invariant I-3) -->
  <div class="card-glow border border-slate-800 rounded-2xl p-5 shadow-xl">
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-2">
      <div>
        <h2 class="font-bold text-white text-base">موتور مغایرت‌گیری خودکار حسابداری (Automated Ledger Reconciliation — Invariant I-3)</h2>
        <p class="text-xs text-slate-400 mt-0.5">بررسی مستمر انطباق <code class="text-sky-400">SUM(deposits.amount)</code> با <code class="text-sky-400">balances.balance</code> از طریق <code class="text-amber-300">FULL OUTER JOIN</code></p>
      </div>
      <div id="reconStatusBox" class="px-4 py-1.5 rounded-xl text-xs font-bold bg-emerald-500/10 border border-emerald-500/30 text-emerald-400">
        انطباق ۱۰۰٪ دفتر کل و کیف‌پول‌ها (۰ مغایرت)
      </div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-3">
      <div class="bg-slate-950/70 border border-slate-800/80 p-3 rounded-xl flex justify-between items-center">
        <span class="text-xs text-slate-400">مجموع کل واریزهای USDT در دفتر کل:</span>
        <span id="ledgerUSDT" class="text-sm font-bold text-emerald-400 ltr-num">0 USDT</span>
      </div>
      <div class="bg-slate-950/70 border border-slate-800/80 p-3 rounded-xl flex justify-between items-center">
        <span class="text-xs text-slate-400">مجموع کل واریزهای BTC در دفتر کل:</span>
        <span id="ledgerBTC" class="text-sm font-bold text-amber-400 ltr-num">0 BTC</span>
      </div>
      <div class="bg-slate-950/70 border border-slate-800/80 p-3 rounded-xl flex justify-between items-center">
        <span class="text-xs text-slate-400">مجموع کل واریزهای IRT در دفتر کل:</span>
        <span id="ledgerIRT" class="text-sm font-bold text-cyan-400 ltr-num">0 IRT</span>
      </div>
    </div>
    <div id="discrepancyContainer" class="hidden overflow-x-auto border border-rose-800/80 rounded-xl mt-3">
      <table class="w-full text-right text-xs">
        <thead class="text-rose-300 bg-rose-950/60 border-b border-rose-800">
          <tr>
            <th class="py-2 px-4">کاربر</th>
            <th class="py-2 px-4">ارز</th>
            <th class="py-2 px-4">موجودی مورد انتظار (SUM Deposits)</th>
            <th class="py-2 px-4">موجودی فعلی ( جدول Balances)</th>
            <th class="py-2 px-4">میزان مغایرت</th>
          </tr>
        </thead>
        <tbody id="discrepancyRows" class="divide-y divide-rose-900/40 bg-rose-950/20"></tbody>
      </table>
    </div>
  </div>

  <!-- DLQ Inspector & Deterministic Replay Panel -->
  <div class="card-glow border border-slate-800 rounded-2xl p-5 shadow-xl">
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-4 gap-2">
      <div>
        <h2 class="font-bold text-white text-base">بازرس صف پیام‌های مرده و بازپخش (DLQ Inspector & Replay — Stream: DEPOSITS_DLQ)</h2>
        <p class="text-xs text-slate-400 mt-0.5">ثبت متادیتای کامل استریم مبدا (<code class="text-rose-400">source_stream, subject, stream_sequence</code>) با شناسه قطعی ضدتکرار</p>
      </div>
      <span class="text-xs text-slate-400">می‌توانید Payload پیام خراب را اصلاح کرده و مجدداً به چرخه پردازش بازگردانید</span>
    </div>
    <div class="overflow-x-auto border border-slate-800 rounded-xl">
      <table class="w-full text-right text-xs">
        <thead class="text-slate-400 bg-slate-950 border-b border-slate-800">
          <tr>
            <th class="py-3 px-3">DLQ Seq</th>
            <th class="py-3 px-3">متادیتای مبدا (Stream / Subject / Seq)</th>
            <th class="py-3 px-3">شناسه رویداد</th>
            <th class="py-3 px-3">علت خطا (Reason)</th>
            <th class="py-3 px-3">Payload اصلی</th>
            <th class="py-3 px-3">عملیات بازپخش</th>
          </tr>
        </thead>
        <tbody id="dlqRows" class="divide-y divide-slate-800/60"></tbody>
      </table>
    </div>
  </div>

  <!-- Replicas Live Telemetry Table -->
  <div class="card-glow border border-slate-800 rounded-2xl p-5 shadow-xl">
    <div class="flex justify-between items-center mb-4">
      <h2 class="font-bold text-white text-base">وضعیت لحظه‌ای نمونه‌های مقیاس‌شده (Active Depositor Replicas)</h2>
      <span class="text-xs text-slate-400">تله‌متری زنده از طریق NATS Telemetry</span>
    </div>
    <div class="overflow-x-auto border border-slate-800 rounded-xl">
      <table class="w-full text-right text-xs">
        <thead class="text-slate-400 bg-slate-950 border-b border-slate-800">
          <tr>
            <th class="py-3 px-4">شناسه یکتای نمونه (Container ID / Hostname)</th>
            <th class="py-3 px-4">تعداد Worker</th>
            <th class="py-3 px-4">سرعت نمونه (TPS)</th>
            <th class="py-3 px-4">کل پردازش‌شده</th>
            <th class="py-3 px-4">تکراری خنثی‌شده</th>
            <th class="py-3 px-4">خطا / تلاش مجدد</th>
          </tr>
        </thead>
        <tbody id="replicaRows" class="divide-y divide-slate-800/60"></tbody>
      </table>
    </div>
  </div>

  <!-- Chart & Recent Activity Grid -->
  <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
    <div class="lg:col-span-7 card-glow border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col">
      <h2 class="font-bold text-white text-base mb-3">نمودار زنده سرعت خوشه (Global TPS Stream)</h2>
      <div class="relative flex-1 w-full min-h-[240px]">
        <canvas id="clusterChart"></canvas>
      </div>
    </div>

    <div class="lg:col-span-5 card-glow border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col">
      <h2 class="font-bold text-white text-base mb-3">آخرین تراکنش‌های ثبت‌شده در دیتابیس</h2>
      <div class="overflow-y-auto max-h-64 border border-slate-800 rounded-xl flex-1">
        <table class="w-full text-right text-xs">
          <thead class="text-slate-400 bg-slate-950 sticky top-0 border-b border-slate-800">
            <tr>
              <th class="py-2 px-3">شناسه پیام</th>
              <th class="py-2 px-3">کاربر</th>
              <th class="py-2 px-3">مبلغ</th>
              <th class="py-2 px-3">زمان</th>
            </tr>
          </thead>
          <tbody id="recentDepositsRows" class="divide-y divide-slate-800/60"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Bottom Row: PostgreSQL User Balances Table -->
  <div class="card-glow border border-slate-800 rounded-2xl p-5 shadow-xl">
    <div class="flex justify-between items-center mb-4">
      <h2 class="font-bold text-white text-base">موجودی لحظه‌ای کیف‌پول کاربران در PostgreSQL (جدول balances)</h2>
      <span class="text-xs text-slate-400">محاسبات با دقت بالا (Shopspring Decimal)</span>
    </div>
    <div class="overflow-x-auto border border-slate-800 rounded-xl">
      <table class="w-full text-right text-sm">
        <thead class="text-xs text-slate-400 border-b border-slate-800 bg-slate-950/60">
          <tr>
            <th class="py-3 px-4">شناسه کاربر</th>
            <th class="py-3 px-4">موجودی تتر (USDT)</th>
            <th class="py-3 px-4">موجودی بیت‌کوین (BTC)</th>
            <th class="py-3 px-4">موجودی تومان (IRT)</th>
            <th class="py-3 px-4">آخرین به‌روزرسانی</th>
          </tr>
        </thead>
        <tbody id="balancesRows" class="divide-y divide-slate-800/60"></tbody>
      </table>
    </div>
  </div>

</div>

<!-- Replay Modal -->
<div id="replayModal" class="fixed inset-0 bg-black/75 hidden items-center justify-center p-4 z-50">
  <div class="bg-slate-900 border border-slate-700 rounded-2xl max-w-xl w-full p-6 space-y-4 shadow-2xl">
    <div class="flex justify-between items-center">
      <h3 class="text-base font-bold text-white">اصلاح و بازپخش پیام DLQ (Edit & Replay to DEPOSITS)</h3>
      <button onclick="closeReplayModal()" class="text-slate-400 hover:text-white text-lg">&times;</button>
    </div>
    <p class="text-xs text-slate-400">می‌توانید خطای موجود در JSON (مثلاً تغییر ارز <code class="text-amber-400">DOGE</code> به <code class="text-emerald-400">USDT</code>) را اصلاح کرده و پیام را مجدداً به استریم اصلی ارسال کنید:</p>
    <input type="hidden" id="replaySeq" value="0" />
    <textarea id="replayPayload" rows="5" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-xs text-emerald-300 ltr-num focus:outline-none focus:border-sky-500"></textarea>
    <div class="flex justify-end gap-3">
      <button onclick="closeReplayModal()" class="px-4 py-2 rounded-xl text-xs bg-slate-800 text-slate-300 hover:bg-slate-700">انصراف</button>
      <button onclick="submitReplay()" class="px-4 py-2 rounded-xl text-xs font-bold bg-emerald-600 text-white hover:bg-emerald-500">ارسال به استریم اصلی و حذف از DLQ</button>
    </div>
  </div>
</div>

<script>
let lastGlobalProc = null;
let lastTick = Date.now();
let dlqCache = {};

const ctx = document.getElementById('clusterChart').getContext('2d');
const chartData = {
    labels: [],
    datasets: [{
        label: 'Global TPS',
        data: [],
        borderColor: '#38bdf8',
        backgroundColor: 'rgba(56, 189, 248, 0.1)',
        fill: true,
        tension: 0.35
    }]
};

const clusterChart = new Chart(ctx, {
    type: 'line',
    data: chartData,
    options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
            x: { ticks: { color: '#64748b' }, grid: { color: '#1e293b' } },
            y: { beginAtZero: true, ticks: { color: '#64748b' }, grid: { color: '#1e293b' } }
        },
        plugins: { legend: { display: false } }
    }
});

function formatDec(str) {
    if (!str || str === '0') return '0';
    return Number(str).toLocaleString('en-US', { maximumFractionDigits: 8 });
}

async function refresh() {
  try {
    const res = await fetch('/api/stats');
    const d = await res.json();
    
    document.getElementById('replicas').innerText = d.active_replicas;
    document.getElementById('workers').innerText = d.total_workers;
    document.getElementById('tps').innerText = d.global_tps.toFixed(1) + ' TPS';
    document.getElementById('processed').innerText = d.global_processed.toLocaleString();
    document.getElementById('duplicates').innerText = d.global_duplicates.toLocaleString();
    document.getElementById('dlqCount').innerText = d.dlq_total_count.toLocaleString();
    document.getElementById('dbTotal').innerText = d.db_total_deposits.toLocaleString();

    // Update Reconciliation UI
    if (d.reconciliation) {
        document.getElementById('ledgerUSDT').innerText = formatDec(d.reconciliation.total_ledger_usdt) + ' USDT';
        document.getElementById('ledgerBTC').innerText = formatDec(d.reconciliation.total_ledger_btc) + ' BTC';
        document.getElementById('ledgerIRT').innerText = formatDec(d.reconciliation.total_ledger_irt) + ' IRT';

        const badge = document.getElementById('reconBadge');
        const statusBox = document.getElementById('reconStatusBox');
        const discContainer = document.getElementById('discrepancyContainer');
        const discRows = document.getElementById('discrepancyRows');

        if (d.reconciliation.is_balanced) {
            badge.className = 'text-sm font-bold text-emerald-400 mt-2';
            badge.innerText = 'Verified ✅';
            statusBox.className = 'px-4 py-1.5 rounded-xl text-xs font-bold bg-emerald-500/10 border border-emerald-500/30 text-emerald-400';
            statusBox.innerText = 'انطباق ۱۰۰٪ دفتر کل و کیف‌پول‌ها (آخرین بررسی: ' + d.reconciliation.checked_at + ')';
            discContainer.classList.add('hidden');
        } else {
            badge.className = 'text-sm font-bold text-rose-400 mt-2 animate-pulse';
            badge.innerText = 'Discrepancy ❌ (' + d.reconciliation.discrepancy_count + ')';
            statusBox.className = 'px-4 py-1.5 rounded-xl text-xs font-bold bg-rose-500/20 border border-rose-500/40 text-rose-300';
            statusBox.innerText = 'هشدار مغایرت حسابداری! تعداد مغایرت: ' + d.reconciliation.discrepancy_count;
            discContainer.classList.remove('hidden');
            discRows.innerHTML = d.reconciliation.discrepancies.map(item =>
                '<tr>' +
                '<td class="py-2 px-4 font-bold text-white">کاربر #' + item.user_id + '</td>' +
                '<td class="py-2 px-4 text-amber-300">' + item.currency + '</td>' +
                '<td class="py-2 px-4 ltr-num text-emerald-300">' + formatDec(item.expected_balance) + '</td>' +
                '<td class="py-2 px-4 ltr-num text-rose-300">' + formatDec(item.actual_balance) + '</td>' +
                '<td class="py-2 px-4 ltr-num font-bold text-rose-400">' + formatDec(item.difference) + '</td>' +
                '</tr>'
            ).join('');
        }
    }

    // Update DLQ Inspector Table
    const dlqTbody = document.getElementById('dlqRows');
    if (!d.dlq_messages || d.dlq_messages.length === 0) {
        dlqTbody.innerHTML = '<tr><td colspan="6" class="py-5 text-center text-slate-500">صف DLQ خالی است (هیچ پیام خرابی وجود ندارد).</td></tr>';
    } else {
        dlqCache = {};
        dlqTbody.innerHTML = d.dlq_messages.map(m => {
            dlqCache[m.dlq_sequence] = m.original_event;
            const srcMeta = (m.source_stream || 'DEPOSITS') + ' / ' + (m.subject || 'deposits.events') + ' #' + m.stream_sequence;
            return '<tr class="hover:bg-slate-800/40 transition">' +
                '<td class="py-2.5 px-3 ltr-num font-bold text-rose-400">#' + m.dlq_sequence + '</td>' +
                '<td class="py-2.5 px-3 ltr-num text-sky-300 text-[11px]">' + srcMeta + '</td>' +
                '<td class="py-2.5 px-3 ltr-num text-indigo-300">' + (m.event_id || 'N/A') + '</td>' +
                '<td class="py-2.5 px-3 text-amber-300">' + m.reason + '</td>' +
                '<td class="py-2.5 px-3 ltr-num text-slate-400 max-w-xs truncate">' + m.original_event + '</td>' +
                '<td class="py-2.5 px-3">' +
                '<button onclick="openReplayModal(' + m.dlq_sequence + ')" class="px-3 py-1 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-bold text-[11px] cursor-pointer">اصلاح و بازپخش ♻️</button>' +
                '</td>' +
                '</tr>';
        }).join('');
    }

    const btn = document.getElementById('pubBtn');
    if (d.publish_enabled) {
        btn.className = 'px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-md flex items-center gap-2 cursor-pointer bg-emerald-600 text-white hover:bg-emerald-500';
        btn.innerHTML = '<span>Publisher: Running (Click to Pause)</span>';
    } else {
        btn.className = 'px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-md flex items-center gap-2 cursor-pointer bg-rose-600 text-white hover:bg-rose-500';
        btn.innerHTML = '<span>Publisher: Paused (Click to Start)</span>';
    }

    const tbody = document.getElementById('replicaRows');
    if (!d.replicas || d.replicas.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-6 text-center text-slate-500">هیچ نمونه‌ای (Replica) فعالی متصل نیست...</td></tr>';
    } else {
        tbody.innerHTML = d.replicas.map(r => 
          '<tr class="hover:bg-slate-800/40 transition">' +
          '<td class="py-3 px-4 font-mono text-sky-300 font-bold"><span class="bg-sky-950/80 border border-sky-800/60 px-2 py-1 rounded">' + r.instance_id + '</span></td>' +
          '<td class="py-3 px-4 text-slate-300">' + r.worker_count + '</td>' +
          '<td class="py-3 px-4 text-emerald-400 font-bold ltr-num">' + r.current_tps.toFixed(1) + ' /s</td>' +
          '<td class="py-3 px-4 text-white ltr-num">' + r.processed_total.toLocaleString() + '</td>' +
          '<td class="py-3 px-4 text-amber-400 ltr-num">' + r.duplicates_total.toLocaleString() + '</td>' +
          '<td class="py-3 px-4 text-rose-400 ltr-num">' + r.failed_total.toLocaleString() + '</td>' +
          '</tr>'
        ).join('');
    }

    const recTbody = document.getElementById('recentDepositsRows');
    if (!d.recent_deposits || d.recent_deposits.length === 0) {
        recTbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-500">هنوز تراکنشی ثبت نشده...</td></tr>';
    } else {
        recTbody.innerHTML = d.recent_deposits.map(item => 
            '<tr class="hover:bg-slate-800/40 transition">' +
            '<td class="py-2 px-3 font-mono text-indigo-300">' + item.event_id.substring(0, 12) + '...</td>' +
            '<td class="py-2 px-3 text-slate-300">#' + item.user_id + '</td>' +
            '<td class="py-2 px-3 text-emerald-400 ltr-num font-medium">' + item.amount + ' ' + item.currency + '</td>' +
            '<td class="py-2 px-3 text-slate-400 ltr-num text-[11px]">' + item.processed_at + '</td>' +
            '</tr>'
        ).join('');
    }

    const balTbody = document.getElementById('balancesRows');
    if (!d.balances || d.balances.length === 0) {
        balTbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-slate-500">کیف‌پولی یافت نشد...</td></tr>';
    } else {
        balTbody.innerHTML = d.balances.map(b => 
            '<tr class="hover:bg-slate-800/40 transition">' +
            '<td class="py-3 px-4 font-bold text-white">کاربر #' + b.user_id + '</td>' +
            '<td class="py-3 px-4 text-emerald-400 ltr-num font-medium">' + formatDec(b.usdt) + ' USDT</td>' +
            '<td class="py-3 px-4 text-amber-400 ltr-num font-medium">' + formatDec(b.btc) + ' BTC</td>' +
            '<td class="py-3 px-4 text-cyan-400 ltr-num font-medium">' + formatDec(b.irt) + ' IRT</td>' +
            '<td class="py-3 px-4 text-slate-400 text-xs ltr-num">' + b.updated_at + '</td>' +
            '</tr>'
        ).join('');
    }

    const now = Date.now();
    const dt = (now - lastTick) / 1000;
    if (lastGlobalProc !== null && dt >= 0.9) {
        const timeLabel = new Date().toLocaleTimeString('fa-IR');
        chartData.labels.push(timeLabel);
        chartData.datasets[0].data.push(d.global_tps);

        if (chartData.labels.length > 25) {
            chartData.labels.shift();
            chartData.datasets[0].data.shift();
        }
        clusterChart.update();
        lastTick = now;
    } else if (lastGlobalProc === null) {
        lastTick = now;
        lastGlobalProc = d.global_processed;
    }

  } catch (e) {}
}

async function togglePublisher() {
  await fetch('/api/publisher/toggle', { method: 'POST' });
  refresh();
}

async function injectPoisonMessage() {
  await fetch('/api/dlq/inject', { method: 'POST' });
  setTimeout(refresh, 400);
}

function openReplayModal(seq) {
  document.getElementById('replaySeq').value = seq;
  document.getElementById('replayPayload').value = dlqCache[seq] || '';
  const modal = document.getElementById('replayModal');
  modal.classList.remove('hidden');
  modal.classList.add('flex');
}

function closeReplayModal() {
  const modal = document.getElementById('replayModal');
  modal.classList.add('hidden');
  modal.classList.remove('flex');
}

async function submitReplay() {
  const seq = Number(document.getElementById('replaySeq').value);
  const payload = document.getElementById('replayPayload').value;
  await fetch('/api/dlq/replay', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dlq_sequence: seq, new_payload: payload })
  });
  closeReplayModal();
  setTimeout(refresh, 400);
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>`
