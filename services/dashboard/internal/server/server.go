package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
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

type AggregatedResponse struct {
	ActiveReplicas  int                  `json:"active_replicas"`
	TotalWorkers    int                  `json:"total_workers"`
	GlobalTPS       float64              `json:"global_tps"`
	GlobalProcessed uint64               `json:"global_processed"`
	GlobalFailed    uint64               `json:"global_failed"`
	GlobalDuplicate uint64               `json:"global_duplicates"`
	DBTotalDeposits int64                `json:"db_total_deposits"`
	PublishEnabled  bool                 `json:"publish_enabled"`
	Replicas        []DepositorTelemetry `json:"replicas"`
	RecentDeposits  []RecentDepositItem  `json:"recent_deposits"`
	Balances        []UserBalanceRow     `json:"balances"`
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
	db             *sql.DB
	publishEnabled atomic.Bool
}

func NewDashboardServer(nc *nats.Conn, db *sql.DB) *DashboardServer {
	s := &DashboardServer{
		replicas: make(map[string]DepositorTelemetry),
		nc:       nc,
		db:       db,
	}
	s.publishEnabled.Store(true)
	return s
}

func (s *DashboardServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleUI)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/publisher/toggle", s.handleTogglePublisher)
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

	if s.db != nil {
		_ = s.db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM deposits").Scan(&dbCount)

		rows, err := s.db.QueryContext(r.Context(), `
			SELECT ROW_NUMBER() OVER (ORDER BY processed_at DESC) as rn, event_id, user_id, currency, amount, TO_CHAR(processed_at, 'YYYY-MM-DD HH24:MI:SS') 
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
	}

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
		PublishEnabled:  s.publishEnabled.Load(),
		Replicas:        replicasList,
		RecentDeposits:  recentDeposits,
		Balances:        balances,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
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
        <p class="text-xs text-slate-400 mt-1">NATS Consumer Grouping &bull; Horizontal Scaled Replicas &bull; PostgreSQL ACID</p>
      </div>
    </div>
    <div class="flex flex-wrap items-center gap-3">
      <button id="pubBtn" class="px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-md flex items-center gap-2 cursor-pointer bg-emerald-600 text-white hover:bg-emerald-500" onclick="togglePublisher()">
        <span>Publisher: Running</span>
      </button>
      <div class="bg-slate-950 border border-slate-800 px-4 py-2 rounded-xl text-center">
        <span class="text-[10px] text-slate-400 block">کل سرعت خوشه (Cluster TPS)</span>
        <span id="tps" class="text-base font-bold text-emerald-400 ltr-num">0 TPS</span>
      </div>
      <div class="flex items-center gap-2 bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 px-4 py-2.5 rounded-xl text-xs font-medium">
        <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
        <span>Cluster Healthy</span>
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
      <h4 class="text-xs text-slate-400 mb-1">Failed / Retried</h4>
      <div class="text-2xl font-black text-rose-400 ltr-num" id="failed">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">DB Total Records</h4>
      <div class="text-2xl font-black text-teal-400 ltr-num" id="dbTotal">0</div>
    </div>
    <div class="card-glow border border-slate-800 p-4 rounded-2xl shadow-lg">
      <h4 class="text-xs text-slate-400 mb-1">Cluster Status</h4>
      <div class="text-lg font-bold text-emerald-400 mt-1">Online 🟢</div>
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

<script>
let lastGlobalProc = null;
let lastTick = Date.now();

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
    document.getElementById('failed').innerText = d.global_failed.toLocaleString();
    document.getElementById('dbTotal').innerText = d.db_total_deposits.toLocaleString();

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

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>`
