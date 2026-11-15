package metrics

import (
	"github.com/alihojaty/reliable-deposit-processor/pkg/metricsx"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ResultAccepted            = "accepted"
	ResultDuplicate           = "duplicate"
	ResultInvalid             = "invalid"
	ResultUnauthorized        = "unauthorized"
	ResultRateLimited         = "rate_limited"
	ResultPayloadTooLarge     = "payload_too_large"
	ResultUpstreamUnavailable = "upstream_unavailable"
	ResultUpstreamTimeout     = "upstream_timeout"
	ResultInternalError       = "internal_error"
)

var AllDepositResults = []string{
	ResultAccepted,
	ResultDuplicate,
	ResultInvalid,
	ResultUnauthorized,
	ResultRateLimited,
	ResultPayloadTooLarge,
	ResultUpstreamUnavailable,
	ResultUpstreamTimeout,
	ResultInternalError,
}

// HTTPDurationBuckets is tailored for fast API gateway responses (0.5ms to 5s).
var HTTPDurationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
}

type GatewayMetrics struct {
	HTTPRequestsTotal    *prometheus.CounterVec
	HTTPRequestDuration  *prometheus.HistogramVec
	HTTPRequestsInFlight prometheus.Gauge
	DepositRequestsTotal *prometheus.CounterVec
	GRPCRetriesTotal     prometheus.Counter
	PublisherEndpoints   prometheus.Gauge
	GRPCClientMetrics    *grpcprom.ClientMetrics
}

func NewGatewayMetrics(reg prometheus.Registerer) *GatewayMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	grpcClientMetrics := grpcprom.NewClientMetrics(
		grpcprom.WithClientHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets(HTTPDurationBuckets),
		),
	)

	m := &GatewayMetrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gateway",
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests processed by the gateway, partitioned by method, route pattern, and status code.",
			},
			[]string{"method", "route", "code"},
		),
		HTTPRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:                   "gateway",
				Name:                        "http_request_duration_seconds",
				Help:                        "HTTP request latency distribution in seconds on the gateway.",
				Buckets:                     HTTPDurationBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
			[]string{"method", "route"},
		),
		HTTPRequestsInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "gateway",
				Name:      "http_requests_in_flight",
				Help:      "Current number of HTTP requests actively being processed by the gateway.",
			},
		),
		DepositRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gateway",
				Name:      "deposit_requests_total",
				Help:      "Total number of deposit requests processed by business outcome result.",
			},
			[]string{"result"},
		),
		GRPCRetriesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "gateway",
				Name:      "grpc_retries_total",
				Help:      "Total number of automatic client-side gRPC retry attempts triggered on Unavailable errors.",
			},
		),
		PublisherEndpoints: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "gateway",
				Name:      "publisher_endpoints",
				Help:      "Current number of active publisher gRPC endpoints discovered by the swarmdns resolver.",
			},
		),
		GRPCClientMetrics: grpcClientMetrics,
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.HTTPRequestsInFlight,
		m.DepositRequestsTotal,
		m.GRPCRetriesTotal,
		m.PublisherEndpoints,
		m.GRPCClientMetrics,
	)

	for _, res := range AllDepositResults {
		m.DepositRequestsTotal.WithLabelValues(res).Add(0)
	}

	return m
}

func (m *GatewayMetrics) RecordDepositResult(result string) {
	if m == nil {
		return
	}
	m.DepositRequestsTotal.WithLabelValues(result).Inc()
}

func (m *GatewayMetrics) CountByResult(result string) uint64 {
	if m == nil {
		return 0
	}
	c, err := m.DepositRequestsTotal.GetMetricWithLabelValues(result)
	if err != nil {
		return 0
	}
	return metricsx.ReadCounterValue(c)
}

func (m *GatewayMetrics) TotalDepositRequests() uint64 {
	if m == nil {
		return 0
	}
	var sum uint64
	for _, res := range AllDepositResults {
		sum += m.CountByResult(res)
	}
	return sum
}

func (m *GatewayMetrics) RetriesCount() uint64 {
	if m == nil {
		return 0
	}
	return metricsx.ReadCounterValue(m.GRPCRetriesTotal)
}
