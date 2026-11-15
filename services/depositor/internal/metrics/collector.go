package metrics

import (
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/metricsx"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ResultProcessed       = "processed"
	ResultDuplicate       = "duplicate"
	ResultDLQ             = "dlq"
	ResultRetry           = "retry"
	ResultUnackedShutdown = "unacked_shutdown"

	SettlementResultSuccess = "success"
	SettlementResultError   = "error"
)

var (
	MessageProcessingBuckets = []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
	}
	EventAgeBuckets = []float64{
		0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
	}
	IngestBatchSizeBuckets = []float64{
		1, 2, 4, 8, 16, 32, 64, 128, 192, 256,
	}
	SettlementDurationBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0,
	}
)

// Collector holds all Prometheus metrics for the depositor service.
// Counters always start at 0 on process start (no database state restoration) and serve as the single source of counting.
type Collector struct {
	MessagesTotal                 *prometheus.CounterVec
	MessageProcessingDuration     prometheus.Histogram
	EventAgeSeconds               prometheus.Histogram
	IngestBatchSize               prometheus.Histogram
	IngestBatchDuration           prometheus.Histogram
	IngestBatchErrorsTotal        prometheus.Counter
	SettlementRunsTotal           *prometheus.CounterVec
	SettlementDuration            prometheus.Histogram
	SettledDepositsTotal          prometheus.Counter
	SettlementLastSuccessUnixTime prometheus.Gauge
	WorkersActive                 prometheus.Gauge
	NATSConnected                 prometheus.Gauge
}

type Snapshot struct {
	MessagesReceived  uint64 `json:"messages_received"`
	MessagesProcessed uint64 `json:"messages_processed"`
	SessionProcessed  uint64 `json:"session_processed"`
	MessagesDuplicate uint64 `json:"messages_duplicate"`
	MessagesInvalid   uint64 `json:"messages_invalid"`
	MessagesDLQ       uint64 `json:"messages_dlq"`
	MessagesRetried   uint64 `json:"messages_retried"`
	RestoredFromDB    uint64 `json:"restored_from_db"`
}

func NewCollector(reg ...prometheus.Registerer) *Collector {
	var r prometheus.Registerer
	if len(reg) > 0 && reg[0] != nil {
		r = reg[0]
	} else {
		r = prometheus.NewRegistry()
	}

	c := &Collector{
		MessagesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "depositor",
				Name:      "messages_total",
				Help:      "Total number of JetStream messages handled by the depositor, partitioned by result (processed, duplicate, dlq, retry, unacked_shutdown).",
			},
			[]string{"result"},
		),
		MessageProcessingDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:                   "depositor",
				Name:                        "message_processing_duration_seconds",
				Help:                        "Time in seconds from receiving a JetStream message in a worker until ACK/NAK completion.",
				Buckets:                     MessageProcessingBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
		),
		EventAgeSeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:                   "depositor",
				Name:                        "event_age_seconds",
				Help:                        "End-to-end pipeline lag in seconds from the deposit event's created_at timestamp to PostgreSQL ingestion commit.",
				Buckets:                     EventAgeBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
		),
		IngestBatchSize: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "depositor",
				Name:      "ingest_batch_size",
				Help:      "Distribution of the number of deposit events coalesced into a single SQL UNNEST ingestion batch.",
				Buckets:   IngestBatchSizeBuckets,
			},
		),
		IngestBatchDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:                   "depositor",
				Name:                        "ingest_batch_duration_seconds",
				Help:                        "Duration in seconds of executing a batch SQL UNNEST insert into PostgreSQL.",
				Buckets:                     MessageProcessingBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
		),
		IngestBatchErrorsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "depositor",
				Name:      "ingest_batch_errors_total",
				Help:      "Total number of SQL UNNEST batch ingestion executions that failed with a database error.",
			},
		),
		SettlementRunsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "depositor",
				Name:      "settlement_runs_total",
				Help:      "Total number of periodic PL/pgSQL batch settlement cycles executed, partitioned by result (success, error).",
			},
			[]string{"result"},
		),
		SettlementDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:                   "depositor",
				Name:                        "settlement_duration_seconds",
				Help:                        "Duration in seconds of each periodic settle_pending_deposits PL/pgSQL execution.",
				Buckets:                     SettlementDurationBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
		),
		SettledDepositsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "depositor",
				Name:      "settled_deposits_total",
				Help:      "Total number of deposit rows settled into user balances by this depositor instance.",
			},
		),
		SettlementLastSuccessUnixTime: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "depositor",
				Name:      "settlement_last_success_timestamp_seconds",
				Help:      "Unix timestamp in seconds of the most recent successful periodic batch settlement run.",
			},
		),
		WorkersActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "depositor",
				Name:      "workers_active",
				Help:      "Current number of active JetStream consumer workers in this depositor replica.",
			},
		),
		NATSConnected: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "depositor",
				Name:      "nats_connected",
				Help:      "Whether this depositor replica is currently connected to NATS (1 = connected, 0 = disconnected).",
			},
		),
	}

	r.MustRegister(
		c.MessagesTotal,
		c.MessageProcessingDuration,
		c.EventAgeSeconds,
		c.IngestBatchSize,
		c.IngestBatchDuration,
		c.IngestBatchErrorsTotal,
		c.SettlementRunsTotal,
		c.SettlementDuration,
		c.SettledDepositsTotal,
		c.SettlementLastSuccessUnixTime,
		c.WorkersActive,
		c.NATSConnected,
	)

	for _, res := range []string{ResultProcessed, ResultDuplicate, ResultDLQ, ResultRetry, ResultUnackedShutdown} {
		c.MessagesTotal.WithLabelValues(res).Add(0)
	}
	for _, res := range []string{SettlementResultSuccess, SettlementResultError} {
		c.SettlementRunsTotal.WithLabelValues(res).Add(0)
	}

	return c
}

func (c *Collector) IncReceived() {}

func (c *Collector) IncProcessed() {
	c.MessagesTotal.WithLabelValues(ResultProcessed).Inc()
}

func (c *Collector) IncDuplicate() {
	c.MessagesTotal.WithLabelValues(ResultDuplicate).Inc()
}

func (c *Collector) IncInvalid() {}

func (c *Collector) IncDLQ() {
	c.MessagesTotal.WithLabelValues(ResultDLQ).Inc()
}

func (c *Collector) IncRetried() {
	c.MessagesTotal.WithLabelValues(ResultRetry).Inc()
}

func (c *Collector) IncUnackedShutdown() {
	c.MessagesTotal.WithLabelValues(ResultUnackedShutdown).Inc()
}

func (c *Collector) ObserveProcessingDuration(d time.Duration) {
	c.MessageProcessingDuration.Observe(d.Seconds())
}

func (c *Collector) ObserveEventAge(createdAt time.Time) {
	if createdAt.IsZero() {
		return
	}
	age := time.Since(createdAt).Seconds()
	if age < 0 {
		age = 0
	}
	c.EventAgeSeconds.Observe(age)
}

func (c *Collector) RecordIngestBatch(size int, duration time.Duration, failed bool) {
	if size > 0 {
		c.IngestBatchSize.Observe(float64(size))
	}
	c.IngestBatchDuration.Observe(duration.Seconds())
	if failed {
		c.IngestBatchErrorsTotal.Inc()
	}
}

func (c *Collector) RecordSettlementRun(settledCount int64, duration time.Duration, err error) {
	c.SettlementDuration.Observe(duration.Seconds())
	if err != nil {
		c.SettlementRunsTotal.WithLabelValues(SettlementResultError).Inc()
		return
	}
	c.SettlementRunsTotal.WithLabelValues(SettlementResultSuccess).Inc()
	if settledCount > 0 {
		c.SettledDepositsTotal.Add(float64(settledCount))
	}
	c.SettlementLastSuccessUnixTime.SetToCurrentTime()
}

func (c *Collector) CountByResult(result string) uint64 {
	if c == nil {
		return 0
	}
	counter, err := c.MessagesTotal.GetMetricWithLabelValues(result)
	if err != nil {
		return 0
	}
	return metricsx.ReadCounterValue(counter)
}

// GetSnapshot reads all counters directly from the Prometheus metrics (single source of truth).
func (c *Collector) GetSnapshot() Snapshot {
	proc := c.CountByResult(ResultProcessed)
	dup := c.CountByResult(ResultDuplicate)
	dlq := c.CountByResult(ResultDLQ)
	retried := c.CountByResult(ResultRetry)
	unacked := c.CountByResult(ResultUnackedShutdown)

	return Snapshot{
		MessagesReceived:  proc + dup + dlq + unacked,
		MessagesProcessed: proc,
		SessionProcessed:  proc,
		MessagesDuplicate: dup,
		MessagesInvalid:   dlq,
		MessagesDLQ:       dlq,
		MessagesRetried:   retried,
		RestoredFromDB:    0,
	}
}
