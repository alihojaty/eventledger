package metricsx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

var (
	// Version and Revision can be overridden at build time via -ldflags.
	Version  = "1.0.0"
	Revision = "dev"
)

// DefaultDurationBuckets covers sub-millisecond in-memory operations up to 10s timeouts.
var DefaultDurationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// ResolveRevision returns the git SHA revision from ldflags or REVISION / IMAGE_TAG environment variables.
func ResolveRevision() string {
	if envRev := strings.TrimSpace(os.Getenv("REVISION")); envRev != "" {
		return envRev
	}
	if envTag := strings.TrimSpace(os.Getenv("IMAGE_TAG")); envTag != "" {
		return envTag
	}
	if strings.TrimSpace(Revision) != "" {
		return Revision
	}
	return "dev"
}

// ResolveVersion returns the service version from ldflags or SERVICE_VERSION environment variable.
func ResolveVersion() string {
	if envVer := strings.TrimSpace(os.Getenv("SERVICE_VERSION")); envVer != "" {
		return envVer
	}
	if strings.TrimSpace(Version) != "" {
		return Version
	}
	return "1.0.0"
}

// NewRegistry creates an explicit, isolated Prometheus registry (no global state or init()),
// and registers standard Go, Process, optional sql.DB collectors, and <service>_build_info.
func NewRegistry(servicePrefix string, db *sql.DB, dbName string) *prometheus.Registry {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	if db != nil {
		if strings.TrimSpace(dbName) == "" {
			dbName = "deposits_db"
		}
		reg.MustRegister(collectors.NewDBStatsCollector(db, dbName))
	}

	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: servicePrefix,
			Name:      "build_info",
			Help:      fmt.Sprintf("Build information for the %s service (constant value 1).", servicePrefix),
		},
		[]string{"version", "revision", "go_version"},
	)
	reg.MustRegister(buildInfo)
	buildInfo.WithLabelValues(ResolveVersion(), ResolveRevision(), runtime.Version()).Set(1)

	return reg
}

// Handler returns a lightweight HTTP handler for /metrics with OpenMetrics exemplar support enabled.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		Registry:          reg,
		EnableOpenMetrics: true,
		Timeout:           5 * time.Second,
	})
}

// Server wraps a dedicated internal HTTP server exposing only /metrics.
type Server struct {
	httpSrv *http.Server
	port    int
	logger  *zap.Logger
}

// StartServer starts a dedicated HTTP server for /metrics on the given port.
// This server is meant to be shut down as the very last step during process termination.
func StartServer(port int, reg *prometheus.Registry, logger *zap.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	s := &Server{
		httpSrv: srv,
		port:    port,
		logger:  logger,
	}

	go func() {
		if logger != nil {
			logger.Info("Prometheus metrics HTTP server listening", zap.Int("metrics_port", port))
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger != nil {
				logger.Error("Prometheus metrics HTTP server stopped with error", zap.Error(err))
			}
		}
	}()

	return s
}

// Shutdown cleanly closes the dedicated /metrics HTTP server.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil || s.httpSrv == nil {
		return
	}
	if err := s.httpSrv.Shutdown(ctx); err != nil && s.logger != nil {
		s.logger.Warn("Prometheus metrics HTTP server shutdown error", zap.Error(err))
	} else if s.logger != nil {
		s.logger.Info("Prometheus metrics HTTP server stopped", zap.Int("metrics_port", s.port))
	}
}

// ReadCounterValue reads the current float64 value of a Prometheus Counter directly from its internal state
// so auxiliary telemetry endpoints (/stats) never maintain duplicate atomic counters.
func ReadCounterValue(c prometheus.Counter) uint64 {
	if c == nil {
		return 0
	}
	var m dto.Metric
	if err := c.Write(&m); err != nil || m.Counter == nil {
		return 0
	}
	val := m.Counter.GetValue()
	if val < 0 {
		return 0
	}
	return uint64(val)
}

// ReadGaugeValue reads the current float64 value of a Prometheus Gauge directly.
func ReadGaugeValue(g prometheus.Gauge) float64 {
	if g == nil {
		return 0
	}
	var m dto.Metric
	if err := g.Write(&m); err != nil || m.Gauge == nil {
		return 0
	}
	return m.Gauge.GetValue()
}
