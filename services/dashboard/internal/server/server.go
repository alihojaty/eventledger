package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
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

type GatewayTelemetry struct {
	Online           bool    `json:"online"`
	Ready            bool    `json:"ready"`
	MTLSEnabled      bool    `json:"mtls_enabled"`
	RateLimitRPS     float64 `json:"rate_limit_rps"`
	RateLimitBurst   int     `json:"rate_limit_burst"`
	TotalRequests    uint64  `json:"total_requests"`
	Accepted202      uint64  `json:"accepted_202"`
	Duplicates       uint64  `json:"duplicates"`
	BadRequest400    uint64  `json:"bad_request_400"`
	Unauthorized401  uint64  `json:"unauthorized_401"`
	PayloadLarge413  uint64  `json:"payload_large_413"`
	RateLimited429   uint64  `json:"rate_limited_429"`
	UpstreamError5xx uint64  `json:"upstream_error_5xx"`
	GRPCRetries      uint64  `json:"grpc_retries"`
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
	Gateway         GatewayTelemetry           `json:"gateway"`
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
	gatewayURL     string
	apiKey         string
	httpClient     *http.Client
	publishEnabled atomic.Bool
}

func NewDashboardServer(nc *nats.Conn, db *sql.DB, extra ...string) *DashboardServer {
	js, _ := jetstream.New(nc)
	gwURL := "http://localhost:8081"
	key := "dev-secret-api-key"
	if len(extra) > 0 && extra[0] != "" {
		gwURL = extra[0]
	} else if env := os.Getenv("GATEWAY_URL"); env != "" {
		gwURL = env
	}
	if len(extra) > 1 && extra[1] != "" {
		key = extra[1]
	} else if env := os.Getenv("API_KEY"); env != "" {
		key = env
	}

	s := &DashboardServer{
		replicas:   make(map[string]DepositorTelemetry),
		nc:         nc,
		js:         js,
		db:         db,
		gatewayURL: strings.TrimRight(gwURL, "/"),
		apiKey:     key,
		httpClient: &http.Client{Timeout: 3 * time.Second},
	}
	initialPub := strings.EqualFold(os.Getenv("PUBLISH_ENABLED"), "true")
	s.publishEnabled.Store(initialPub)
	return s
}

func (s *DashboardServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleUI)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/publisher/toggle", s.handleTogglePublisher)
	mux.HandleFunc("/api/dlq/replay", s.handleReplayDLQ)
	mux.HandleFunc("/api/dlq/inject", s.handleInjectInvalidEvent)
	mux.HandleFunc("/api/gateway/send", s.handleGatewaySendSingle)
	mux.HandleFunc("/api/gateway/loadgen", s.handleGatewayLoadGen)
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

func (s *DashboardServer) fetchGatewayTelemetry(ctx context.Context) GatewayTelemetry {
	var gt GatewayTelemetry
	reqCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, s.gatewayURL+"/stats", nil)
	if err != nil {
		return gt
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return gt
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&gt); err == nil {
			gt.Online = true
		}
	}
	return gt
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
			defer func() { _ = rows.Close() }()
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
			defer func() { _ = balRows.Close() }()
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

	gwTelemetry := s.fetchGatewayTelemetry(r.Context())

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
		Gateway:         gwTelemetry,
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
	defer func() { _ = rows.Close() }()

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

func (s *DashboardServer) handleGatewaySendSingle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input struct {
		UserID         int    `json:"user_id"`
		Currency       string `json:"currency"`
		Amount         string `json:"amount"`
		IdempotencyKey string `json:"idempotency_key"`
		UseWrongAPIKey bool   `json:"use_wrong_api_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	bodyStr := fmt.Sprintf(`{"user_id":%d,"currency":%q,"amount":%q}`, input.UserID, input.Currency, input.Amount)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.gatewayURL+"/v1/deposits", strings.NewReader(bodyStr))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if input.UseWrongAPIKey {
		req.Header.Set("X-API-Key", "invalid-key-for-test")
	} else {
		req.Header.Set("X-API-Key", s.apiKey)
	}
	req.Header.Set("Idempotency-Key", input.IdempotencyKey)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"http_status": 503, "error": err.Error()})
		return
	}
	defer func() { _ = resp.Body.Close() }()

	var rawBody any
	_ = json.NewDecoder(resp.Body).Decode(&rawBody)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"http_status": resp.StatusCode,
		"response":    rawBody,
	})
}

func (s *DashboardServer) handleGatewayLoadGen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var cfg struct {
		Requests    int    `json:"requests"`
		Concurrency int    `json:"concurrency"`
		Mode        string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if cfg.Requests <= 0 || cfg.Requests > 5000 {
		cfg.Requests = 200
	}
	if cfg.Concurrency <= 0 || cfg.Concurrency > 300 {
		cfg.Concurrency = 50
	}

	sharedKey := fmt.Sprintf("storm-key-%d", time.Now().UnixNano())
	jobs := make(chan int, cfg.Requests)
	for i := 1; i <= cfg.Requests; i++ {
		jobs <- i
	}
	close(jobs)

	var s202, sDup, s429, s4xx, s5xx atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup

	currencies := []string{"USDT", "BTC", "IRT"}
	amounts := map[string]string{"USDT": "25.50", "BTC": "0.0025", "IRT": "500000"}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range jobs {
				idemKey := fmt.Sprintf("ui-loadgen-%d-%d", time.Now().UnixNano(), seq)
				userID := (seq % 20) + 1
				cur := currencies[seq%len(currencies)]
				amt := amounts[cur]

				if cfg.Mode == "same_key" {
					idemKey = sharedKey
					userID = 7
					cur = "USDT"
					amt = "125.50"
				}

				bodyStr := fmt.Sprintf(`{"user_id":%d,"currency":%q,"amount":%q}`, userID, cur, amt)
				req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.gatewayURL+"/v1/deposits", strings.NewReader(bodyStr))
				if err != nil {
					s5xx.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-API-Key", s.apiKey)
				req.Header.Set("Idempotency-Key", idemKey)

				resp, err := client.Do(req)
				if err != nil {
					s5xx.Add(1)
					continue
				}
				var parsed struct {
					Duplicate bool `json:"duplicate"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&parsed)
				_ = resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusAccepted:
					s202.Add(1)
					if parsed.Duplicate {
						sDup.Add(1)
					}
				case http.StatusTooManyRequests:
					s429.Add(1)
				case http.StatusBadRequest, http.StatusUnauthorized, http.StatusRequestEntityTooLarge:
					s4xx.Add(1)
				default:
					s5xx.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	elapsedSec := elapsed.Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 0.001
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"requests":     cfg.Requests,
		"elapsed_ms":   elapsed.Milliseconds(),
		"rps":          float64(cfg.Requests) / elapsedSec,
		"accepted_202": s202.Load(),
		"duplicates":   sDup.Load(),
		"limited_429":  s429.Load(),
		"client_4xx":   s4xx.Load(),
		"server_5xx":   s5xx.Load(),
	})
}

func (s *DashboardServer) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

//nolint:staticcheck // ST1018: Persian ZWNJ characters are intentionally used in the embedded UI template.
const dashboardHTML = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Enterprise Cluster Dashboard — Deposit Processor</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://unpkg.com/lucide@latest"></script>
<link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    color-scheme: light;
  }
  body {
    font-family: 'Vazirmatn', -apple-system, BlinkMacSystemFont, sans-serif;
    background-color: #f4f7fb;
    background-image:
      radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.09) 0px, transparent 45%),
      radial-gradient(at 100% 0%, rgba(99, 102, 241, 0.08) 0px, transparent 45%),
      radial-gradient(at 50% 100%, rgba(16, 185, 129, 0.07) 0px, transparent 50%);
    background-attachment: fixed;
    color: #0f172a;
  }
  .ltr-num {
    direction: ltr;
    display: inline-block;
    font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  }
  .glass-card {
    background: rgba(255, 255, 255, 0.88);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid rgba(226, 232, 240, 0.9);
    box-shadow: 0 10px 30px -10px rgba(15, 23, 42, 0.05), 0 4px 6px -4px rgba(15, 23, 42, 0.03);
    transition: all 0.28s cubic-bezier(0.16, 1, 0.3, 1);
  }
  .glass-card:hover {
    box-shadow: 0 20px 35px -12px rgba(15, 23, 42, 0.08), 0 6px 12px -6px rgba(15, 23, 42, 0.04);
    border-color: rgba(203, 213, 225, 1);
  }
  .metric-card {
    position: relative;
    overflow: hidden;
  }
  .metric-card:hover {
    transform: translateY(-3px);
  }
  @keyframes fadeInUp {
    from { opacity: 0; transform: translateY(14px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .animate-section {
    animation: fadeInUp 0.5s cubic-bezier(0.16, 1, 0.3, 1) forwards;
  }
  .delay-1 { animation-delay: 0.05s; }
  .delay-2 { animation-delay: 0.1s; }
  .delay-3 { animation-delay: 0.15s; }
  .delay-4 { animation-delay: 0.2s; }
  .delay-5 { animation-delay: 0.25s; }

  @keyframes pulseRing {
    0% { transform: scale(0.85); opacity: 0.8; }
    70% { transform: scale(2.1); opacity: 0; }
    100% { transform: scale(2.1); opacity: 0; }
  }
  .pulse-dot::before {
    content: '';
    position: absolute;
    inset: 0;
    border-radius: 9999px;
    background-color: inherit;
    animation: pulseRing 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
  }

  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #f1f5f9; border-radius: 8px; }
  ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 8px; }
  ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
</style>
</head>
<body class="min-h-screen p-4 md:p-6 lg:p-8 selection:bg-indigo-100 selection:text-indigo-900">

<div id="toastContainer" class="fixed bottom-5 left-5 z-50 flex flex-col gap-2.5 pointer-events-none"></div>

<div class="max-w-[1440px] mx-auto space-y-6">

  <!-- Top Header Banner -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section relative overflow-hidden">
    <div class="absolute top-0 right-0 left-0 h-1.5 bg-gradient-to-r from-indigo-500 via-sky-500 to-emerald-500"></div>
    <div class="flex flex-col xl:flex-row items-start xl:items-center justify-between gap-5">
      
      <div class="flex items-center gap-4">
        <div class="relative w-14 h-14 rounded-2xl bg-gradient-to-br from-indigo-600 to-sky-500 flex items-center justify-center text-white shadow-lg shadow-indigo-500/25 shrink-0">
          <i data-lucide="cpu" class="w-7 h-7"></i>
          <span class="absolute -bottom-1 -right-1 w-4 h-4 rounded-full bg-emerald-500 border-2 border-white"></span>
        </div>
        <div>
          <div class="flex flex-wrap items-center gap-2.5">
            <h1 class="text-lg md:text-xl font-black text-slate-900 tracking-tight">مرکز کنترل مانیتورینگ خوشه‌ای</h1>
            <span class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-indigo-50 text-indigo-700 border border-indigo-200/80">
              <span class="relative w-2 h-2 rounded-full bg-indigo-600 pulse-dot"></span>
              Cluster Control Center
            </span>
          </div>
          <p class="text-xs text-slate-500 mt-1.5 flex flex-wrap items-center gap-2 font-medium">
            <span class="inline-flex items-center gap-1 text-slate-600"><i data-lucide="shield" class="w-3.5 h-3.5 text-indigo-500"></i> REST Gateway & gRPC</span>
            <span class="text-slate-300">&bull;</span>
            <span class="inline-flex items-center gap-1 text-slate-600"><i data-lucide="network" class="w-3.5 h-3.5 text-sky-500"></i> NATS JetStream</span>
            <span class="text-slate-300">&bull;</span>
            <span class="inline-flex items-center gap-1 text-slate-600"><i data-lucide="shield-check" class="w-3.5 h-3.5 text-emerald-500"></i> Automated Ledger Audit</span>
            <span class="text-slate-300">&bull;</span>
            <span class="inline-flex items-center gap-1 text-slate-600"><i data-lucide="rotate-ccw" class="w-3.5 h-3.5 text-amber-500"></i> DLQ Replay</span>
          </p>
        </div>
      </div>

      <div class="flex flex-wrap items-center gap-3 w-full xl:w-auto justify-end">
        <button onclick="injectPoisonMessage()" class="group px-4 py-2.5 rounded-2xl text-xs font-bold transition-all duration-200 bg-amber-50 text-amber-800 border border-amber-200 hover:bg-amber-500 hover:text-white hover:border-amber-500 hover:shadow-lg hover:shadow-amber-500/20 flex items-center gap-2 cursor-pointer active:scale-95">
          <i data-lucide="flask-conical" class="w-4 h-4 text-amber-600 group-hover:text-white transition-colors"></i>
          <span>تزریق پیام خراب (Poison Pill)</span>
        </button>

        <button id="pubBtn" onclick="togglePublisher()" class="px-4 py-2.5 rounded-2xl text-xs font-bold transition-all duration-200 shadow-sm flex items-center gap-2 cursor-pointer bg-emerald-600 text-white hover:bg-emerald-700 hover:shadow-lg hover:shadow-emerald-600/20 active:scale-95">
          <i data-lucide="play" class="w-4 h-4"></i>
          <span>Publisher: Running</span>
        </button>

        <div class="bg-gradient-to-br from-slate-900 to-slate-800 text-white px-5 py-2.5 rounded-2xl flex items-center gap-3 shadow-md shadow-slate-900/10 border border-slate-700">
          <div class="w-9 h-9 rounded-xl bg-emerald-500/20 border border-emerald-400/30 flex items-center justify-center text-emerald-400">
            <i data-lucide="gauge" class="w-5 h-5"></i>
          </div>
          <div class="text-right">
            <span class="text-[10px] text-slate-300 block font-medium">سرعت خوشه (Cluster TPS)</span>
            <span id="tps" class="text-base font-extrabold text-emerald-400 ltr-num tracking-tight">0 TPS</span>
          </div>
        </div>
      </div>

    </div>
  </div>

  <!-- Gateway & gRPC Control Panel -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section delay-1">
    <div class="flex flex-col lg:flex-row justify-between items-start lg:items-center mb-5 gap-3 pb-4 border-b border-slate-100">
      <div class="flex items-start gap-3.5">
        <div class="w-11 h-11 rounded-2xl bg-indigo-50 border border-indigo-200/80 text-indigo-600 flex items-center justify-center shrink-0 mt-0.5">
          <i data-lucide="shield" class="w-5 h-5"></i>
        </div>
        <div>
          <div class="flex flex-wrap items-center gap-2">
            <h2 class="font-extrabold text-slate-900 text-base">درگاه ورودی و امنیت (REST API Gateway ──gRPC──▶ Publisher)</h2>
            <span id="gwReadyBadge" class="px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-200 ltr-num">
              /readyz: Checking...
            </span>
            <span id="gwMtlsBadge" class="px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-sky-50 text-sky-700 border border-sky-200 ltr-num">
              mTLS: Configured
            </span>
            <span id="gwRateBadge" class="px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-purple-50 text-purple-700 border border-purple-200 ltr-num">
              Rate Limit: Active
            </span>
          </div>
          <p class="text-xs text-slate-500 mt-1">
            مانیتورینگ زنده درخواست‌های HTTP، محدودکننده نرخ توکن بر اساس X-API-Key، تلاش مجدد خودکار gRPC و ابزار Loadgen
          </p>
        </div>
      </div>

      <div class="flex flex-wrap gap-2">
        <button onclick="runGatewayLoadgen(200, 50, 'normal')" class="px-3.5 py-2 rounded-xl text-xs font-bold bg-indigo-600 text-white hover:bg-indigo-700 transition cursor-pointer shadow-sm">
          اجرای Loadgen (۲۰۰ واریز موازی)
        </button>
        <button onclick="runGatewayLoadgen(50, 50, 'same_key')" class="px-3.5 py-2 rounded-xl text-xs font-bold bg-amber-500 text-white hover:bg-amber-600 transition cursor-pointer shadow-sm">
          تست ۵۰ کلید تکراری هم‌زمان (Idempotency)
        </button>
        <button onclick="runGatewayLoadgen(350, 100, 'rate_limit_burst')" class="px-3.5 py-2 rounded-xl text-xs font-bold bg-rose-600 text-white hover:bg-rose-700 transition cursor-pointer shadow-sm">
          تست رگباری Rate Limit (429)
        </button>
      </div>
    </div>

    <!-- Gateway Counters -->
    <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3 mb-2">
      <div class="bg-slate-50 border border-slate-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-slate-500 block font-semibold">کل درخواست‌ها</span>
        <span id="gwTotal" class="text-lg font-black text-slate-800 ltr-num">0</span>
      </div>
      <div class="bg-emerald-50/60 border border-emerald-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-emerald-700 block font-semibold">202 Accepted</span>
        <span id="gw202" class="text-lg font-black text-emerald-700 ltr-num">0</span>
      </div>
      <div class="bg-amber-50/60 border border-amber-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-amber-700 block font-semibold">تکراری خنثی‌شده</span>
        <span id="gwDup" class="text-lg font-black text-amber-700 ltr-num">0</span>
      </div>
      <div class="bg-purple-50/60 border border-purple-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-purple-700 block font-semibold">429 Rate Limited</span>
        <span id="gw429" class="text-lg font-black text-purple-700 ltr-num">0</span>
      </div>
      <div class="bg-sky-50/60 border border-sky-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-sky-700 block font-semibold">gRPC Auto-Retry</span>
        <span id="gwRetry" class="text-lg font-black text-sky-700 ltr-num">0</span>
      </div>
      <div class="bg-rose-50/60 border border-rose-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-rose-700 block font-semibold">401 Unauthorized</span>
        <span id="gw401" class="text-lg font-black text-rose-700 ltr-num">0</span>
      </div>
      <div class="bg-orange-50/60 border border-orange-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-orange-700 block font-semibold">400 / 413 Invalid</span>
        <span id="gw400" class="text-lg font-black text-orange-700 ltr-num">0</span>
      </div>
      <div class="bg-red-50/60 border border-red-200/80 p-3 rounded-2xl">
        <span class="text-[11px] text-red-700 block font-semibold">503 / 504 Timeout</span>
        <span id="gw5xx" class="text-lg font-black text-red-700 ltr-num">0</span>
      </div>
    </div>

    <div id="loadgenResultBanner" class="hidden bg-slate-900 text-white rounded-2xl p-3.5 text-xs font-mono ltr-num mt-3"></div>
  </div>

  <!-- Global Cluster Metric Cards -->
  <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-4 animate-section delay-1">
    
    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">Active Replicas</span>
        <div class="w-9 h-9 rounded-xl bg-sky-50 border border-sky-100 text-sky-600 flex items-center justify-center">
          <i data-lucide="server" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-slate-800 ltr-num" id="replicas">0</div>
      <div class="mt-1 text-[11px] text-sky-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-sky-500"></span> نودهای فعال مصرف‌کننده
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">Total Workers</span>
        <div class="w-9 h-9 rounded-xl bg-indigo-50 border border-indigo-100 text-indigo-600 flex items-center justify-center">
          <i data-lucide="workflow" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-slate-800 ltr-num" id="workers">0</div>
      <div class="mt-1 text-[11px] text-indigo-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-indigo-500"></span> گوروتین‌های موازی
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">Global Processed</span>
        <div class="w-9 h-9 rounded-xl bg-emerald-50 border border-emerald-100 text-emerald-600 flex items-center justify-center">
          <i data-lucide="check-circle-2" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-emerald-600 ltr-num" id="processed">0</div>
      <div class="mt-1 text-[11px] text-emerald-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span> پردازش موفق کل
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">Duplicates Blocked</span>
        <div class="w-9 h-9 rounded-xl bg-amber-50 border border-amber-100 text-amber-600 flex items-center justify-center">
          <i data-lucide="shield-ban" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-amber-600 ltr-num" id="duplicates">0</div>
      <div class="mt-1 text-[11px] text-amber-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span> مهار Idempotency
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">DLQ Messages</span>
        <div class="w-9 h-9 rounded-xl bg-rose-50 border border-rose-100 text-rose-600 flex items-center justify-center">
          <i data-lucide="alert-octagon" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-rose-600 ltr-num" id="dlqCount">0</div>
      <div class="mt-1 text-[11px] text-rose-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-rose-500"></span> صف پیام‌های معیوب
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">DB Total Records</span>
        <div class="w-9 h-9 rounded-xl bg-teal-50 border border-teal-100 text-teal-600 flex items-center justify-center">
          <i data-lucide="database" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-2xl font-black text-teal-600 ltr-num" id="dbTotal">0</div>
      <div class="mt-1 text-[11px] text-teal-600 font-medium flex items-center gap-1">
        <span class="w-1.5 h-1.5 rounded-full bg-teal-500"></span> رکوردهای PostgreSQL
      </div>
    </div>

    <div class="glass-card metric-card p-4 rounded-2xl bg-gradient-to-br from-white to-emerald-50/40">
      <div class="flex items-center justify-between mb-3">
        <span class="text-xs font-semibold text-slate-500">Ledger Audit</span>
        <div class="w-9 h-9 rounded-xl bg-emerald-100/80 border border-emerald-200 text-emerald-700 flex items-center justify-center">
          <i data-lucide="scale" class="w-4 h-4"></i>
        </div>
      </div>
      <div class="text-sm font-extrabold text-emerald-600 mt-1" id="reconBadge">Verified OK</div>
      <div class="mt-1.5 text-[11px] text-slate-500 font-medium">تراز حسابداری لحظه‌ای</div>
    </div>

  </div>

  <!-- Automated Ledger Reconciliation Panel -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section delay-2">
    <div class="flex flex-col lg:flex-row justify-between items-start lg:items-center mb-5 gap-3 pb-4 border-b border-slate-100">
      <div class="flex items-start gap-3.5">
        <div class="w-11 h-11 rounded-2xl bg-emerald-50 border border-emerald-200/80 text-emerald-600 flex items-center justify-center shrink-0 mt-0.5">
          <i data-lucide="file-check-2" class="w-5 h-5"></i>
        </div>
        <div>
          <h2 class="font-extrabold text-slate-900 text-base">موتور مغایرت‌گیری خودکار حسابداری (Automated Ledger Reconciliation)</h2>
          <p class="text-xs text-slate-500 mt-1">
            بررسی مستمر انطباق SUM(deposits.amount) با balances.balance از طریق FULL OUTER JOIN
          </p>
        </div>
      </div>
      <div id="reconStatusBox" class="px-4 py-2 rounded-2xl text-xs font-bold bg-emerald-50 border border-emerald-200 text-emerald-700 shadow-sm">
        انطباق ۱۰۰٪ دفتر کل و کیف‌پول‌ها (۰ مغایرت)
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div class="bg-gradient-to-l from-emerald-50/70 to-white border border-emerald-200/70 p-4 rounded-2xl flex justify-between items-center shadow-sm">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-emerald-500 text-white flex items-center justify-center font-black text-sm shadow-sm shadow-emerald-500/20">USDT</div>
          <div>
            <span class="text-xs font-bold text-slate-700 block">تتر (Tether USD)</span>
            <span class="text-[11px] text-slate-400">مجموع کل واریزهای دفتر کل</span>
          </div>
        </div>
        <span id="ledgerUSDT" class="text-base font-extrabold text-emerald-700 ltr-num bg-white px-3 py-1.5 rounded-xl border border-emerald-100">0 USDT</span>
      </div>

      <div class="bg-gradient-to-l from-amber-50/70 to-white border border-amber-200/70 p-4 rounded-2xl flex justify-between items-center shadow-sm">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-amber-500 text-white flex items-center justify-center font-black text-sm shadow-sm shadow-amber-500/20">BTC</div>
          <div>
            <span class="text-xs font-bold text-slate-700 block">بیت‌کوین (Bitcoin)</span>
            <span class="text-[11px] text-slate-400">مجموع کل واریزهای دفتر کل</span>
          </div>
        </div>
        <span id="ledgerBTC" class="text-base font-extrabold text-amber-700 ltr-num bg-white px-3 py-1.5 rounded-xl border border-amber-100">0 BTC</span>
      </div>

      <div class="bg-gradient-to-l from-sky-50/70 to-white border border-sky-200/70 p-4 rounded-2xl flex justify-between items-center shadow-sm">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-sky-500 text-white flex items-center justify-center font-black text-xs shadow-sm shadow-sky-500/20">IRT</div>
          <div>
            <span class="text-xs font-bold text-slate-700 block">تومان ایران (IRT)</span>
            <span class="text-[11px] text-slate-400">مجموع کل واریزهای دفتر کل</span>
          </div>
        </div>
        <span id="ledgerIRT" class="text-base font-extrabold text-sky-700 ltr-num bg-white px-3 py-1.5 rounded-xl border border-sky-100">0 IRT</span>
      </div>
    </div>

    <div id="discrepancyContainer" class="hidden overflow-x-auto border border-rose-200 rounded-2xl mt-4 shadow-sm">
      <table class="w-full text-right text-xs">
        <thead class="text-rose-800 bg-rose-50 border-b border-rose-200 font-bold">
          <tr>
            <th class="py-3 px-4">کاربر</th>
            <th class="py-3 px-4">ارز</th>
            <th class="py-3 px-4">موجودی مورد انتظار</th>
            <th class="py-3 px-4">موجودی فعلی</th>
            <th class="py-3 px-4">میزان مغایرت</th>
          </tr>
        </thead>
        <tbody id="discrepancyRows" class="divide-y divide-rose-100 bg-white"></tbody>
      </table>
    </div>
  </div>

  <!-- DLQ Inspector & Replay Panel -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section delay-3">
    <div class="flex flex-col lg:flex-row justify-between items-start lg:items-center mb-5 gap-3">
      <div class="flex items-start gap-3.5">
        <div class="w-11 h-11 rounded-2xl bg-rose-50 border border-rose-200/80 text-rose-600 flex items-center justify-center shrink-0 mt-0.5">
          <i data-lucide="bug" class="w-5 h-5"></i>
        </div>
        <div>
          <h2 class="font-extrabold text-slate-900 text-base">بازرس صف پیام‌های مرده و بازپخش (DLQ Inspector & Replay)</h2>
          <p class="text-xs text-slate-500 mt-1">
            ثبت متادیتای کامل استریم مبدا با شناسه قطعی ضدتکرار
          </p>
        </div>
      </div>
    </div>

    <div class="overflow-x-auto border border-slate-200/90 rounded-2xl bg-white shadow-inner">
      <table class="w-full text-right text-xs">
        <thead class="text-slate-600 bg-slate-50/90 border-b border-slate-200 font-bold">
          <tr>
            <th class="py-3.5 px-4">DLQ Seq</th>
            <th class="py-3.5 px-4">متادیتای مبدا</th>
            <th class="py-3.5 px-4">شناسه رویداد</th>
            <th class="py-3.5 px-4">علت خطا</th>
            <th class="py-3.5 px-4">Payload اصلی</th>
            <th class="py-3.5 px-4 text-left">عملیات بازپخش</th>
          </tr>
        </thead>
        <tbody id="dlqRows" class="divide-y divide-slate-100"></tbody>
      </table>
    </div>
  </div>

  <!-- Replicas Live Telemetry Table -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section delay-3">
    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-5 gap-3">
      <div class="flex items-center gap-3.5">
        <div class="w-11 h-11 rounded-2xl bg-sky-50 border border-sky-200/80 text-sky-600 flex items-center justify-center shrink-0">
          <i data-lucide="radio" class="w-5 h-5"></i>
        </div>
        <div>
          <h2 class="font-extrabold text-slate-900 text-base">وضعیت لحظه‌ای نمونه‌های مقیاس‌شده (Active Depositor Replicas)</h2>
          <p class="text-xs text-slate-500 mt-0.5">دریافت تله‌متری زنده از طریق NATS</p>
        </div>
      </div>
    </div>

    <div class="overflow-x-auto border border-slate-200/90 rounded-2xl bg-white">
      <table class="w-full text-right text-xs">
        <thead class="text-slate-600 bg-slate-50/90 border-b border-slate-200 font-bold">
          <tr>
            <th class="py-3.5 px-4">شناسه نمونه</th>
            <th class="py-3.5 px-4">تعداد Worker</th>
            <th class="py-3.5 px-4">سرعت (TPS)</th>
            <th class="py-3.5 px-4">پردازش‌شده</th>
            <th class="py-3.5 px-4">تکراری خنثی‌شده</th>
            <th class="py-3.5 px-4">خطا / تلاش مجدد</th>
          </tr>
        </thead>
        <tbody id="replicaRows" class="divide-y divide-slate-100"></tbody>
      </table>
    </div>
  </div>

  <!-- Chart & Recent Activity Grid -->
  <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 animate-section delay-4">
    
    <div class="lg:col-span-7 glass-card rounded-3xl p-5 md:p-6 flex flex-col">
      <div class="flex items-center justify-between mb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-2xl bg-indigo-50 border border-indigo-100 text-indigo-600 flex items-center justify-center">
            <i data-lucide="activity" class="w-5 h-5"></i>
          </div>
          <div>
            <h2 class="font-extrabold text-slate-900 text-base">نمودار زنده سرعت خوشه (Global TPS Stream)</h2>
            <p class="text-xs text-slate-500">نرخ تراکنش‌های پردازش‌شده در ثانیه</p>
          </div>
        </div>
        <span class="text-[11px] font-bold text-indigo-600 bg-indigo-50 px-2.5 py-1 rounded-lg border border-indigo-100 ltr-num">Real-time 1s</span>
      </div>
      <div class="relative flex-1 w-full min-h-[260px] bg-slate-50/50 rounded-2xl p-3 border border-slate-100">
        <canvas id="clusterChart"></canvas>
      </div>
    </div>

    <div class="lg:col-span-5 glass-card rounded-3xl p-5 md:p-6 flex flex-col">
      <div class="flex items-center justify-between mb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-2xl bg-emerald-50 border border-emerald-100 text-emerald-600 flex items-center justify-center">
            <i data-lucide="arrow-down-left" class="w-5 h-5"></i>
          </div>
          <div>
            <h2 class="font-extrabold text-slate-900 text-base">آخرین تراکنش‌های ثبت‌شده</h2>
            <p class="text-xs text-slate-500">۱۵ واریز آخر در دیتابیس</p>
          </div>
        </div>
      </div>
      <div class="overflow-y-auto max-h-[275px] border border-slate-200/90 rounded-2xl flex-1 bg-white">
        <table class="w-full text-right text-xs">
          <thead class="text-slate-600 bg-slate-50/95 sticky top-0 border-b border-slate-200 font-bold z-10 backdrop-blur-sm">
            <tr>
              <th class="py-3 px-3.5">شناسه پیام</th>
              <th class="py-3 px-3.5">کاربر</th>
              <th class="py-3 px-3.5">مبلغ</th>
              <th class="py-3 px-3.5">زمان</th>
            </tr>
          </thead>
          <tbody id="recentDepositsRows" class="divide-y divide-slate-100"></tbody>
        </table>
      </div>
    </div>

  </div>

  <!-- PostgreSQL Balances Table -->
  <div class="glass-card rounded-3xl p-5 md:p-6 animate-section delay-5">
    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-5 gap-3">
      <div class="flex items-center gap-3.5">
        <div class="w-11 h-11 rounded-2xl bg-teal-50 border border-teal-200/80 text-teal-600 flex items-center justify-center shrink-0">
          <i data-lucide="wallet" class="w-5 h-5"></i>
        </div>
        <div>
          <h2 class="font-extrabold text-slate-900 text-base">موجودی لحظه‌ای کیف‌پول کاربران در PostgreSQL (جدول balances)</h2>
          <p class="text-xs text-slate-500 mt-0.5">به‌روزرسانی اتمیک تراز مالی کاربران در تراکنش‌های دیتابیس</p>
        </div>
      </div>
    </div>

    <div class="overflow-x-auto border border-slate-200/90 rounded-2xl bg-white">
      <table class="w-full text-right text-sm">
        <thead class="text-xs text-slate-600 border-b border-slate-200 bg-slate-50/90 font-bold">
          <tr>
            <th class="py-3.5 px-4">شناسه کاربر</th>
            <th class="py-3.5 px-4">موجودی تتر (USDT)</th>
            <th class="py-3.5 px-4">موجودی بیت‌کوین (BTC)</th>
            <th class="py-3.5 px-4">موجودی تومان (IRT)</th>
            <th class="py-3.5 px-4">آخرین به‌روزرسانی</th>
          </tr>
        </thead>
        <tbody id="balancesRows" class="divide-y divide-slate-100"></tbody>
      </table>
    </div>
  </div>

</div>

<!-- Replay Modal -->
<div id="replayModal" class="fixed inset-0 bg-slate-900/40 backdrop-blur-sm hidden items-center justify-center p-4 z-50 transition-opacity">
  <div class="bg-white border border-slate-200 rounded-3xl max-w-xl w-full p-6 space-y-5 shadow-2xl animate-section">
    <div class="flex justify-between items-center border-b border-slate-100 pb-4">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-2xl bg-sky-50 border border-sky-200 text-sky-600 flex items-center justify-center">
          <i data-lucide="rotate-ccw" class="w-5 h-5"></i>
        </div>
        <div>
          <h3 class="text-base font-extrabold text-slate-900">اصلاح و بازپخش پیام DLQ</h3>
        </div>
      </div>
      <button onclick="closeReplayModal()" class="w-8 h-8 rounded-xl bg-slate-100 text-slate-500 hover:bg-rose-50 hover:text-rose-600 flex items-center justify-center text-lg font-bold transition cursor-pointer">&times;</button>
    </div>

    <input type="hidden" id="replaySeq" value="0" />
    <div>
      <label class="block text-xs font-bold text-slate-700 mb-2">محتوای پیام (JSON Payload):</label>
      <textarea id="replayPayload" rows="5" class="w-full bg-slate-900 border border-slate-800 rounded-2xl p-4 text-xs text-emerald-300 ltr-num focus:outline-none focus:ring-2 focus:ring-sky-500 shadow-inner leading-relaxed"></textarea>
    </div>

    <div class="flex justify-end gap-3 pt-2">
      <button onclick="closeReplayModal()" class="px-5 py-2.5 rounded-xl text-xs font-bold bg-slate-100 text-slate-700 hover:bg-slate-200 transition cursor-pointer">انصراف</button>
      <button onclick="submitReplay()" class="px-5 py-2.5 rounded-xl text-xs font-bold bg-gradient-to-r from-emerald-600 to-teal-600 text-white hover:from-emerald-500 hover:to-teal-500 shadow-md shadow-emerald-600/20 transition cursor-pointer">
        ارسال به استریم اصلی و حذف از DLQ
      </button>
    </div>
  </div>
</div>

<script>
lucide.createIcons();

let lastGlobalProc = null;
let lastTick = Date.now();
let dlqCache = {};

const ctx = document.getElementById('clusterChart').getContext('2d');
const chartGradient = ctx.createLinearGradient(0, 0, 0, 240);
chartGradient.addColorStop(0, 'rgba(79, 70, 229, 0.24)');
chartGradient.addColorStop(1, 'rgba(56, 189, 248, 0.01)');

const chartData = {
    labels: [],
    datasets: [{
        label: 'Global TPS',
        data: [],
        borderColor: '#4f46e5',
        borderWidth: 2.5,
        backgroundColor: chartGradient,
        pointBackgroundColor: '#ffffff',
        pointBorderColor: '#4f46e5',
        pointBorderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: true,
        tension: 0.38
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
            x: {
                ticks: { color: '#64748b', font: { family: 'Vazirmatn', size: 11 } },
                grid: { color: '#e2e8f0', borderDash: [4, 4] }
            },
            y: {
                beginAtZero: true,
                ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 11 } },
                grid: { color: '#e2e8f0', borderDash: [4, 4] }
            }
        },
        plugins: {
            legend: { display: false }
        }
    }
});

function showToast(message, type) {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    const colorClasses = type === 'warn'
        ? 'bg-amber-600 text-white shadow-amber-600/20'
        : 'bg-slate-900 text-white shadow-slate-900/20';
    toast.className = 'px-4 py-3 rounded-2xl text-xs font-bold shadow-xl flex items-center gap-2.5 transition-all duration-300 ' + colorClasses;
    toast.innerText = message;
    container.appendChild(toast);
    setTimeout(function() {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(8px)';
        setTimeout(function() { toast.remove(); }, 300);
    }, 2600);
}

function formatDec(str) {
    if (!str || str === '0') return '0';
    return Number(str).toLocaleString('en-US', { maximumFractionDigits: 8 });
}

function getCurrencyBadge(cur, amount) {
    if (cur === 'USDT') {
        return '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-emerald-50 text-emerald-700 border border-emerald-200/70 ltr-num font-bold">' + amount + ' USDT</span>';
    }
    if (cur === 'BTC') {
        return '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-amber-50 text-amber-700 border border-amber-200/70 ltr-num font-bold">' + amount + ' BTC</span>';
    }
    return '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-sky-50 text-sky-700 border border-sky-200/70 ltr-num font-bold">' + amount + ' ' + cur + '</span>';
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

    // Gateway stats
    if (d.gateway) {
        document.getElementById('gwTotal').innerText = d.gateway.total_requests.toLocaleString();
        document.getElementById('gw202').innerText = d.gateway.accepted_202.toLocaleString();
        document.getElementById('gwDup').innerText = d.gateway.duplicates.toLocaleString();
        document.getElementById('gw429').innerText = d.gateway.rate_limited_429.toLocaleString();
        document.getElementById('gwRetry').innerText = d.gateway.grpc_retries.toLocaleString();
        document.getElementById('gw401').innerText = d.gateway.unauthorized_401.toLocaleString();
        document.getElementById('gw400').innerText = (d.gateway.bad_request_400 + d.gateway.payload_large_413).toLocaleString();
        document.getElementById('gw5xx').innerText = d.gateway.upstream_error_5xx.toLocaleString();

        const readyBadge = document.getElementById('gwReadyBadge');
        if (d.gateway.online && d.gateway.ready) {
            readyBadge.className = 'px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-200 ltr-num';
            readyBadge.innerText = '/readyz: 200 OK';
        } else {
            readyBadge.className = 'px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-rose-50 text-rose-700 border border-rose-200 ltr-num animate-pulse';
            readyBadge.innerText = '/readyz: 503 Unavailable';
        }

        document.getElementById('gwMtlsBadge').innerText = d.gateway.mtls_enabled ? 'mTLS: Active' : 'mTLS: Inactive (Env Off)';
        document.getElementById('gwRateBadge').innerText = 'Rate Limit: ' + d.gateway.rate_limit_rps + ' RPS (Burst ' + d.gateway.rate_limit_burst + ')';
    }

    if (d.reconciliation) {
        document.getElementById('ledgerUSDT').innerText = formatDec(d.reconciliation.total_ledger_usdt) + ' USDT';
        document.getElementById('ledgerBTC').innerText = formatDec(d.reconciliation.total_ledger_btc) + ' BTC';
        document.getElementById('ledgerIRT').innerText = formatDec(d.reconciliation.total_ledger_irt) + ' IRT';

        const badge = document.getElementById('reconBadge');
        const statusBox = document.getElementById('reconStatusBox');
        const discContainer = document.getElementById('discrepancyContainer');
        const discRows = document.getElementById('discrepancyRows');

        if (d.reconciliation.is_balanced) {
            badge.className = 'text-sm font-extrabold text-emerald-600 mt-1';
            badge.innerText = 'Verified OK';
            statusBox.className = 'px-4 py-2 rounded-2xl text-xs font-bold bg-emerald-50 border border-emerald-200 text-emerald-700 shadow-sm';
            statusBox.innerText = 'انطباق ۱۰۰٪ دفتر کل و کیف‌پول‌ها (آخرین بررسی: ' + d.reconciliation.checked_at + ')';
            discContainer.classList.add('hidden');
        } else {
            badge.className = 'text-sm font-extrabold text-rose-600 mt-1 animate-pulse';
            badge.innerText = 'Discrepancy (' + d.reconciliation.discrepancy_count + ')';
            statusBox.className = 'px-4 py-2 rounded-2xl text-xs font-bold bg-rose-50 border border-rose-200 text-rose-700 shadow-sm';
            statusBox.innerText = 'هشدار مغایرت حسابداری! تعداد: ' + d.reconciliation.discrepancy_count;
            discContainer.classList.remove('hidden');
            discRows.innerHTML = d.reconciliation.discrepancies.map(function(item) {
                return '<tr class="hover:bg-rose-50/40 transition">' +
                '<td class="py-2.5 px-4 font-bold text-slate-800">کاربر #' + item.user_id + '</td>' +
                '<td class="py-2.5 px-4 font-bold text-amber-700">' + item.currency + '</td>' +
                '<td class="py-2.5 px-4 ltr-num text-emerald-700 font-semibold">' + formatDec(item.expected_balance) + '</td>' +
                '<td class="py-2.5 px-4 ltr-num text-rose-700 font-semibold">' + formatDec(item.actual_balance) + '</td>' +
                '<td class="py-2.5 px-4 ltr-num font-extrabold text-rose-600">' + formatDec(item.difference) + '</td>' +
                '</tr>';
            }).join('');
        }
    }

    const dlqTbody = document.getElementById('dlqRows');
    if (!d.dlq_messages || d.dlq_messages.length === 0) {
        dlqTbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-slate-400 font-medium">صف DLQ خالی است.</td></tr>';
    } else {
        dlqCache = {};
        dlqTbody.innerHTML = d.dlq_messages.map(function(m) {
            dlqCache[m.dlq_sequence] = m.original_event;
            const srcMeta = (m.source_stream || 'DEPOSITS') + ' #' + m.stream_sequence;
            return '<tr class="hover:bg-slate-50/90 transition">' +
                '<td class="py-3 px-4 ltr-num font-extrabold text-rose-600"><span class="bg-rose-50 border border-rose-200 px-2 py-0.5 rounded-lg">#' + m.dlq_sequence + '</span></td>' +
                '<td class="py-3 px-4 ltr-num text-sky-700 text-[11px] font-semibold">' + srcMeta + '</td>' +
                '<td class="py-3 px-4 ltr-num text-indigo-700 font-semibold">' + (m.event_id || 'N/A') + '</td>' +
                '<td class="py-3 px-4"><span class="px-2.5 py-1 rounded-lg bg-amber-50 border border-amber-200/70 text-amber-800 font-semibold">' + m.reason + '</span></td>' +
                '<td class="py-3 px-4 ltr-num text-slate-600 max-w-xs truncate font-mono bg-slate-50/70 rounded">' + m.original_event + '</td>' +
                '<td class="py-3 px-4 text-left">' +
                '<button onclick="openReplayModal(' + m.dlq_sequence + ')" class="px-3.5 py-1.5 rounded-xl bg-sky-600 hover:bg-sky-700 text-white font-bold text-[11px] shadow-sm transition cursor-pointer">اصلاح و بازپخش</button>' +
                '</td>' +
                '</tr>';
        }).join('');
    }

    const btn = document.getElementById('pubBtn');
    if (d.publish_enabled) {
        btn.className = 'px-4 py-2.5 rounded-2xl text-xs font-bold transition-all duration-200 shadow-sm flex items-center gap-2 cursor-pointer bg-emerald-600 text-white hover:bg-emerald-700 active:scale-95';
        btn.innerHTML = '<span class="w-2 h-2 rounded-full bg-white animate-ping"></span><span>Publisher: Running</span>';
    } else {
        btn.className = 'px-4 py-2.5 rounded-2xl text-xs font-bold transition-all duration-200 shadow-sm flex items-center gap-2 cursor-pointer bg-rose-600 text-white hover:bg-rose-700 active:scale-95';
        btn.innerHTML = '<span class="w-2 h-2 rounded-full bg-rose-200"></span><span>Publisher: Paused</span>';
    }

    const tbody = document.getElementById('replicaRows');
    if (!d.replicas || d.replicas.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-slate-400 font-medium">هیچ نمونه فعالی متصل نیست...</td></tr>';
    } else {
        tbody.innerHTML = d.replicas.map(function(r) { 
          return '<tr class="hover:bg-slate-50/90 transition">' +
'<td class="py-3.5 px-4 font-mono text-sky-800 font-bold">' + r.instance_id + '</td>' +
          '<td class="py-3.5 px-4 text-slate-700 font-bold">' + r.worker_count + ' Workers</td>' +
          '<td class="py-3.5 px-4 text-emerald-600 font-extrabold ltr-num">' + r.current_tps.toFixed(1) + ' /s</td>' +
          '<td class="py-3.5 px-4 text-slate-900 font-bold ltr-num">' + r.processed_total.toLocaleString() + '</td>' +
          '<td class="py-3.5 px-4 text-amber-600 font-bold ltr-num">' + r.duplicates_total.toLocaleString() + '</td>' +
          '<td class="py-3.5 px-4 text-rose-600 font-bold ltr-num">' + r.failed_total.toLocaleString() + '</td>' +
          '</tr>';
        }).join('');
    }

    const recTbody = document.getElementById('recentDepositsRows');
    if (!d.recent_deposits || d.recent_deposits.length === 0) {
        recTbody.innerHTML = '<tr><td colspan="4" class="py-8 text-center text-slate-400 font-medium">هنوز تراکنشی ثبت نشده...</td></tr>';
    } else {
        recTbody.innerHTML = d.recent_deposits.map(function(item) { 
            return '<tr class="hover:bg-slate-50/90 transition">' +
            '<td class="py-2.5 px-3.5 font-mono text-indigo-600 font-semibold ltr-num">' + item.event_id.substring(0, 12) + '...</td>' +
            '<td class="py-2.5 px-3.5 text-slate-700 font-bold">#' + item.user_id + '</td>' +
            '<td class="py-2.5 px-3.5">' + getCurrencyBadge(item.currency, item.amount) + '</td>' +
            '<td class="py-2.5 px-3.5 text-slate-400 ltr-num text-[11px] font-medium">' + item.processed_at + '</td>' +
            '</tr>';
        }).join('');
    }

    const balTbody = document.getElementById('balancesRows');
    if (!d.balances || d.balances.length === 0) {
        balTbody.innerHTML = '<tr><td colspan="5" class="py-8 text-center text-slate-400 font-medium">کیف‌پولی یافت نشد...</td></tr>';
    } else {
        balTbody.innerHTML = d.balances.map(function(b) { 
            return '<tr class="hover:bg-slate-50/90 transition">' +
            '<td class="py-3.5 px-4 font-extrabold text-slate-800">کاربر #' + b.user_id + '</td>' +
            '<td class="py-3.5 px-4"><span class="text-emerald-700 font-bold text-xs ltr-num">' + formatDec(b.usdt) + ' USDT</span></td>' +
            '<td class="py-3.5 px-4"><span class="text-amber-700 font-bold text-xs ltr-num">' + formatDec(b.btc) + ' BTC</span></td>' +
            '<td class="py-3.5 px-4"><span class="text-sky-700 font-bold text-xs ltr-num">' + formatDec(b.irt) + ' IRT</span></td>' +
            '<td class="py-3.5 px-4 text-slate-400 text-xs ltr-num font-medium">' + b.updated_at + '</td>' +
            '</tr>';
        }).join('');
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
  showToast('وضعیت انتشار پیام تغییر کرد', 'info');
  refresh();
}

async function injectPoisonMessage() {
  await fetch('/api/dlq/inject', { method: 'POST' });
  showToast('پیام خراب تستی به استریم تزریق شد', 'warn');
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
  showToast('پیام بازپخش و از DLQ حذف شد', 'info');
  setTimeout(refresh, 400);
}

async function runGatewayLoadgen(requests, concurrency, mode) {
  showToast('در حال اجرای عملیات بر روی Gateway...', 'info');
  try {
    const res = await fetch('/api/gateway/loadgen', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ requests: requests, concurrency: concurrency, mode: mode })
    });
    const data = await res.json();
    const banner = document.getElementById('loadgenResultBanner');
    banner.classList.remove('hidden');
    banner.innerText = '[Loadgen Report in ' + data.elapsed_ms + 'ms | ' + data.rps.toFixed(1) + ' req/s] 202 Accepted: ' + data.accepted_202 + ' | Duplicates: ' + data.duplicates + ' | 429 RateLimit: ' + data.limited_429 + ' | 5xx Errors: ' + data.server_5xx;
    showToast('تست بار Gateway کامل شد', 'info');
    refresh();
  } catch(e) {
    showToast('خطا در برقراری ارتباط با Gateway', 'warn');
  }
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>`
