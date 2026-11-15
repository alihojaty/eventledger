package metrics

import (
	"github.com/alihojaty/reliable-deposit-processor/pkg/metricsx"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	PublishResultOK        = "ok"
	PublishResultDuplicate = "duplicate"
	PublishResultError     = "error"
)

// JetStreamPublishBuckets covers fast local NATS acks (0.25ms) up to 5s broker timeouts.
var JetStreamPublishBuckets = []float64{
	0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
}

// HealthStateReader provides read-only access to the unified HealthManager state.
type HealthStateReader interface {
	IsReady() bool
	IsDraining() bool
	IsNATSConnected() bool
}

type PublisherMetrics struct {
	GRPCServerMetrics        *grpcprom.ServerMetrics
	JetStreamPublishTotal    *prometheus.CounterVec
	JetStreamPublishDuration prometheus.Histogram
	RequestsInFlight         prometheus.Gauge
	IdempotencyConflicts     prometheus.Counter
	ReadyGauge               prometheus.GaugeFunc
	DrainingGauge            prometheus.GaugeFunc
	NATSConnectedGauge       prometheus.GaugeFunc
}

func NewPublisherMetrics(reg prometheus.Registerer, health HealthStateReader) *PublisherMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	grpcServerMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets(JetStreamPublishBuckets),
		),
	)

	m := &PublisherMetrics{
		GRPCServerMetrics: grpcServerMetrics,
		JetStreamPublishTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "publisher",
				Name:      "jetstream_publish_total",
				Help:      "Total number of deposit events published to NATS JetStream by result (ok, duplicate, error).",
			},
			[]string{"result"},
		),
		JetStreamPublishDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:                   "publisher",
				Name:                        "jetstream_publish_duration_seconds",
				Help:                        "Time taken in seconds to publish a deposit event to NATS JetStream and receive PubAck.",
				Buckets:                     JetStreamPublishBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
		),
		RequestsInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "publisher",
				Name:      "requests_in_flight",
				Help:      "Current number of PublishDeposit gRPC requests actively being processed.",
			},
		),
		IdempotencyConflicts: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "publisher",
				Name:      "idempotency_conflicts_total",
				Help:      "Total number of PublishDeposit requests rejected due to conflicting payload parameters for the same event_id.",
			},
		),
	}

	reg.MustRegister(
		m.GRPCServerMetrics,
		m.JetStreamPublishTotal,
		m.JetStreamPublishDuration,
		m.RequestsInFlight,
		m.IdempotencyConflicts,
	)

	for _, res := range []string{PublishResultOK, PublishResultDuplicate, PublishResultError} {
		m.JetStreamPublishTotal.WithLabelValues(res).Add(0)
	}

	if health != nil {
		m.ReadyGauge = prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "publisher",
				Name:      "ready",
				Help:      "Whether the publisher instance is ready to serve traffic (1 = ready, 0 = not ready), read from unified healthmgr.",
			},
			func() float64 {
				if health.IsReady() {
					return 1
				}
				return 0
			},
		)
		m.DrainingGauge = prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "publisher",
				Name:      "draining",
				Help:      "Whether the publisher instance is currently draining for shutdown (1 = draining, 0 = normal), read from unified healthmgr.",
			},
			func() float64 {
				if health.IsDraining() {
					return 1
				}
				return 0
			},
		)
		m.NATSConnectedGauge = prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "publisher",
				Name:      "nats_connected",
				Help:      "Whether the publisher instance has an active NATS connection (1 = connected, 0 = disconnected), read from unified healthmgr.",
			},
			func() float64 {
				if health.IsNATSConnected() {
					return 1
				}
				return 0
			},
		)
		reg.MustRegister(m.ReadyGauge, m.DrainingGauge, m.NATSConnectedGauge)
	}

	return m
}

func (m *PublisherMetrics) ProcessedCount() uint64 {
	if m == nil {
		return 0
	}
	okC, _ := m.JetStreamPublishTotal.GetMetricWithLabelValues(PublishResultOK)
	dupC, _ := m.JetStreamPublishTotal.GetMetricWithLabelValues(PublishResultDuplicate)
	return metricsx.ReadCounterValue(okC) + metricsx.ReadCounterValue(dupC)
}

func (m *PublisherMetrics) InFlightCount() int64 {
	if m == nil {
		return 0
	}
	return int64(metricsx.ReadGaugeValue(m.RequestsInFlight))
}
