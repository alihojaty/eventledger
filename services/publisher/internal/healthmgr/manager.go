package healthmgr

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type StateCode int32

const (
	StateStarting     StateCode = 0 // Initial boot before NATS ready
	StateHealthy      StateCode = 1 // Connected to NATS and serving traffic
	StateDegradedNATS StateCode = 2 // NATS temporarily disconnected (process alive, not ready)
	StateUnhealthy    StateCode = 3 // Shutting down (SIGTERM) or FORCE_UNHEALTHY=true
)

func (s StateCode) String() string {
	switch s {
	case StateStarting:
		return "STARTING"
	case StateHealthy:
		return "SERVING"
	case StateDegradedNATS:
		return "DEGRADED_NATS_DISCONNECTED"
	case StateUnhealthy:
		return "NOT_SERVING_UNHEALTHY"
	default:
		return "UNKNOWN"
	}
}

type StatsSnapshot struct {
	InstanceID             string  `json:"instance_id"`
	Status                 string  `json:"status"`
	Live                   bool    `json:"live"`
	Ready                  bool    `json:"ready"`
	Draining               bool    `json:"draining"`
	NATSConnected          bool    `json:"nats_connected"`
	ProcessedRequests      uint64  `json:"processed_requests"`
	InFlightRequests       int64   `json:"in_flight_requests"`
	LastRequestReceivedAt  string  `json:"last_request_received_at,omitempty"`
	LastRequestCompletedAt string  `json:"last_request_completed_at,omitempty"`
	UptimeSeconds          float64 `json:"uptime_seconds"`
}

// Manager is the single source of truth for Publisher health (/livez, /readyz, and gRPC Health)
// as well as per-instance request counters (/stats).
type Manager struct {
	grpc_health_v1.UnimplementedHealthServer

	instanceID     string
	forceUnhealthy bool
	startedAt      time.Time
	logger         *zap.Logger

	// Single atomic health state read by /livez, /readyz, and gRPC Health Check/Watch
	state         atomic.Int32
	draining      atomic.Bool
	natsConnected atomic.Bool

	// Request metrics
	processedReqs      atomic.Uint64
	inFlightReqs       atomic.Int64
	lastReqReceivedNs  atomic.Int64
	lastReqCompletedNs atomic.Int64

	// gRPC Health Watch subscribers
	mu           sync.Mutex
	nextWatchID  uint64
	watchers     map[uint64]chan grpc_health_v1.HealthCheckResponse_ServingStatus
	watchCloseCh chan struct{}
	watchClosed  bool
}

func NewManager(instanceID string, forceUnhealthy bool, logger *zap.Logger) *Manager {
	m := &Manager{
		instanceID:     instanceID,
		forceUnhealthy: forceUnhealthy,
		startedAt:      time.Now().UTC(),
		logger:         logger,
		watchers:       make(map[uint64]chan grpc_health_v1.HealthCheckResponse_ServingStatus),
		watchCloseCh:   make(chan struct{}),
	}

	if forceUnhealthy {
		m.state.Store(int32(StateUnhealthy))
		logger.Warn("Publisher initialized in FORCE_UNHEALTHY mode; all health checks will fail")
	} else {
		m.state.Store(int32(StateStarting))
	}

	return m
}

// SetNATSConnected updates the unified health state based on NATS connectivity.
// If the instance is already draining (SIGTERM) or forced unhealthy, it stays StateUnhealthy.
func (m *Manager) SetNATSConnected(connected bool) {
	m.natsConnected.Store(connected)

	if m.forceUnhealthy || m.draining.Load() {
		m.transitionTo(StateUnhealthy)
		return
	}

	if connected {
		m.transitionTo(StateHealthy)
	} else {
		m.transitionTo(StateDegradedNATS)
	}
}

// SetDraining transitions the unified health state to StateUnhealthy upon SIGTERM.
// Both HTTP (/livez, /readyz) and gRPC health immediately become unhealthy.
func (m *Manager) SetDraining() {
	m.draining.Store(true)
	m.transitionTo(StateUnhealthy)
}

func (m *Manager) transitionTo(next StateCode) {
	prev := StateCode(m.state.Swap(int32(next)))
	if prev == next {
		return
	}

	grpcStatus := m.grpcStatusForState(next)
	m.logger.Info("Unified health state transition",
		zap.String("previous_state", prev.String()),
		zap.String("new_state", next.String()),
		zap.String("grpc_serving_status", grpcStatus.String()),
		zap.Bool("http_live", m.isLiveState(next)),
		zap.Bool("http_ready", m.isReadyState(next)),
	)

	m.mu.Lock()
	for _, ch := range m.watchers {
		select {
		case ch <- grpcStatus:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- grpcStatus:
			default:
			}
		}
	}
	m.mu.Unlock()
}

func (m *Manager) CurrentState() StateCode {
	return StateCode(m.state.Load())
}

func (m *Manager) IsDraining() bool {
	return m.draining.Load()
}

func (m *Manager) isLiveState(s StateCode) bool {
	return s == StateStarting || s == StateHealthy || s == StateDegradedNATS
}

func (m *Manager) isReadyState(s StateCode) bool {
	return s == StateHealthy
}

func (m *Manager) grpcStatusForState(s StateCode) grpc_health_v1.HealthCheckResponse_ServingStatus {
	if s == StateHealthy {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}
	return grpc_health_v1.HealthCheckResponse_NOT_SERVING
}

// Check implements grpc_health_v1.HealthServer reading from the single state.
func (m *Manager) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	st := m.CurrentState()
	return &grpc_health_v1.HealthCheckResponse{
		Status: m.grpcStatusForState(st),
	}, nil
}

// Watch implements grpc_health_v1.HealthServer streaming updates from the single state.
func (m *Manager) Watch(_ *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	ch := make(chan grpc_health_v1.HealthCheckResponse_ServingStatus, 4)

	m.mu.Lock()
	if m.watchClosed {
		m.mu.Unlock()
		_ = stream.Send(&grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		})
		return status.Error(codes.Unavailable, "health watch closed for graceful shutdown")
	}
	m.nextWatchID++
	id := m.nextWatchID
	m.watchers[id] = ch
	initialStatus := m.grpcStatusForState(m.CurrentState())
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.watchers, id)
		m.mu.Unlock()
	}()

	if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: initialStatus}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-m.watchCloseCh:
			return nil
		case newStatus := <-ch:
			if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: newStatus}); err != nil {
				return err
			}
		}
	}
}

// CloseWatchers cleanly terminates open gRPC Health.Watch streams right before grpcSrv.GracefulStop()
// so GracefulStop only waits for in-flight PublishDeposit RPCs.
func (m *Manager) CloseWatchers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.watchClosed {
		m.watchClosed = true
		close(m.watchCloseCh)
	}
}

func (m *Manager) RecordRequestStart() time.Time {
	now := time.Now().UTC()
	m.inFlightReqs.Add(1)
	m.lastReqReceivedNs.Store(now.UnixNano())
	return now
}

func (m *Manager) RecordRequestEnd(succeeded bool) time.Time {
	now := time.Now().UTC()
	m.inFlightReqs.Add(-1)
	if succeeded {
		m.processedReqs.Add(1)
	}
	m.lastReqCompletedNs.Store(now.UnixNano())
	return now
}

func (m *Manager) GetSnapshot() StatsSnapshot {
	st := m.CurrentState()
	snap := StatsSnapshot{
		InstanceID:        m.instanceID,
		Status:            st.String(),
		Live:              m.isLiveState(st),
		Ready:             m.isReadyState(st),
		Draining:          m.draining.Load(),
		NATSConnected:     m.natsConnected.Load(),
		ProcessedRequests: m.processedReqs.Load(),
		InFlightRequests:  m.inFlightReqs.Load(),
		UptimeSeconds:     time.Since(m.startedAt).Seconds(),
	}

	if recvNs := m.lastReqReceivedNs.Load(); recvNs > 0 {
		snap.LastRequestReceivedAt = time.Unix(0, recvNs).UTC().Format(time.RFC3339Nano)
	}
	if compNs := m.lastReqCompletedNs.Load(); compNs > 0 {
		snap.LastRequestCompletedAt = time.Unix(0, compNs).UTC().Format(time.RFC3339Nano)
	}
	return snap
}

func (m *Manager) NewHTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", m.handleLivez)
	mux.HandleFunc("/readyz", m.handleReadyz)
	mux.HandleFunc("/stats", m.handleStats)
	return mux
}

func (m *Manager) handleLivez(w http.ResponseWriter, _ *http.Request) {
	st := m.CurrentState()
	w.Header().Set("Content-Type", "application/json")
	if !m.isLiveState(st) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "unhealthy",
			"state":       st.String(),
			"instance_id": m.instanceID,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      "ok",
		"state":       st.String(),
		"instance_id": m.instanceID,
	})
}

func (m *Manager) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	st := m.CurrentState()
	w.Header().Set("Content-Type", "application/json")
	if !m.isReadyState(st) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "not_ready",
			"state":       st.String(),
			"instance_id": m.instanceID,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      "ready",
		"state":       st.String(),
		"instance_id": m.instanceID,
	})
}

func (m *Manager) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(m.GetSnapshot())
}
