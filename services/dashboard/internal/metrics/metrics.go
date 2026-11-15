package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var HTTPDurationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
}

type DashboardMetrics struct {
	HTTPRequestsTotal    *prometheus.CounterVec
	HTTPRequestDuration  *prometheus.HistogramVec
	HTTPRequestsInFlight prometheus.Gauge
}

func NewDashboardMetrics(reg prometheus.Registerer) *DashboardMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &DashboardMetrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dashboard",
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests processed by the dashboard, partitioned by method, route pattern, and status code.",
			},
			[]string{"method", "route", "code"},
		),
		HTTPRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:                   "dashboard",
				Name:                        "http_request_duration_seconds",
				Help:                        "HTTP request latency distribution in seconds on the dashboard service.",
				Buckets:                     HTTPDurationBuckets,
				NativeHistogramBucketFactor: 1.1,
			},
			[]string{"method", "route"},
		),
		HTTPRequestsInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dashboard",
				Name:      "http_requests_in_flight",
				Help:      "Current number of HTTP requests actively being processed by the dashboard service.",
			},
		),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.HTTPRequestsInFlight,
	)

	return m
}
