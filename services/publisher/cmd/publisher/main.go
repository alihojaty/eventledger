package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/pkg/tlscfg"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/generator"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/grpcserver"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/healthmgr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	baseLog, err := logger.New(cfg.LogLevel)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "publisher-local"
	}
	log := baseLog.With(
		zap.String("service", "publisher"),
		zap.String("instance_id", hostname),
	)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	// Single source of truth for /livez, /readyz, gRPC Health, and /stats
	healthMgr := healthmgr.NewManager(hostname, cfg.ForceUnhealthy, log)

	httpHealthSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.PublisherHealthPort),
		Handler:           healthMgr.NewHTTPHandler(),
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		log.Info("Publisher HTTP health & stats server listening",
			zap.Int("health_port", cfg.PublisherHealthPort),
		)
		if err := httpHealthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Publisher HTTP health server stopped with error", zap.Error(err))
		}
	}()

	natsBroker, err := broker.NewNATSBroker(sigCtx, cfg.NatsURL, log)
	if err != nil {
		log.Fatal("Failed to initialize NATS JetStream broker", zap.Error(err))
	}
	healthMgr.SetNATSConnected(natsBroker.IsConnected())

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PublisherGRPCPort))
	if err != nil {
		natsBroker.Close()
		log.Fatal("Failed to listen on gRPC port", zap.Int("port", cfg.PublisherGRPCPort), zap.Error(err))
	}

	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts,
		grpcserver.UnaryInterceptors(log),
		grpc.InitialWindowSize(4*1024*1024),
		grpc.InitialConnWindowSize(16*1024*1024),
		grpc.WriteBufferSize(256*1024),
		grpc.ReadBufferSize(256*1024),
		grpc.MaxConcurrentStreams(4096),
	)

	if cfg.MTLSEnabled {
		creds, err := tlscfg.ServerTransportCredentials(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if err != nil {
			natsBroker.Close()
			log.Fatal("Failed to configure publisher mTLS credentials", zap.Error(err))
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	grpcSrv := grpc.NewServer(serverOpts...)
	pubGRPC := grpcserver.NewServer(natsBroker, log, healthMgr)
	depositv1.RegisterDepositPublisherServiceServer(grpcSrv, pubGRPC)

	// Register unified HealthManager as the gRPC HealthServer
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthMgr)

	// Background NATS connectivity monitor updating the single HealthManager
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				return
			case <-ticker.C:
				healthMgr.SetNATSConnected(natsBroker.IsConnected())
			}
		}
	}()

	// Periodic 10-second stats log line with instance_id and processed_requests
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				return
			case <-ticker.C:
				snap := healthMgr.GetSnapshot()
				log.Info("Publisher instance periodic stats report",
					zap.Uint64("processed_requests", snap.ProcessedRequests),
					zap.Int64("in_flight_requests", snap.InFlightRequests),
					zap.String("health_status", snap.Status),
				)
			}
		}
	}()

	go func() {
		log.Info("Publisher gRPC server listening",
			zap.Int("grpc_port", cfg.PublisherGRPCPort),
			zap.Int("health_port", cfg.PublisherHealthPort),
			zap.Duration("drain_delay", cfg.DrainDelay),
			zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
			zap.Bool("mtls_enabled", cfg.MTLSEnabled),
		)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("gRPC server stopped with error", zap.Error(err))
		}
	}()

	pub := generator.NewPublisher(natsBroker, cfg.PublishInterval, cfg.PublishEnabled, log)
	pub.StartControlListener(sigCtx, cfg.NatsURL)
	go pub.Start(sigCtx)

	// =========================================================================
	// EXACT 6-STEP ORDERED GRACEFUL SHUTDOWN
	// =========================================================================

	// Step 1: SIGTERM arrives and is logged
	<-sigCtx.Done()
	sigTime := time.Now().UTC()
	log.Info("STEP 1/6: SIGTERM received; initiating ordered graceful shutdown",
		zap.Time("sigterm_at", sigTime),
		zap.Duration("drain_delay", cfg.DrainDelay),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
	)

	// Step 2: Health status becomes unhealthy (both HTTP /livez, /readyz and gRPC health)
	healthMgr.SetDraining()
	unhealthyTime := time.Now().UTC()
	log.Info("STEP 2/6: Health status set to UNHEALTHY across HTTP (/livez, /readyz -> 503) and gRPC (NOT_SERVING)",
		zap.Time("unhealthy_at", unhealthyTime),
	)

	// Step 3: Continue normal operation for DRAIN_DELAY (incoming requests are still accepted & processed)
	if cfg.DrainDelay > 0 {
		log.Info("STEP 3/6: Entering DRAIN_DELAY window; continuing to accept and process in-flight/arriving requests",
			zap.Duration("drain_delay", cfg.DrainDelay),
		)
		time.Sleep(cfg.DrainDelay)
	}
	snapAfterDrain := healthMgr.GetSnapshot()
	log.Info("STEP 3/6 COMPLETE: DRAIN_DELAY elapsed",
		zap.String("last_request_received_at", snapAfterDrain.LastRequestReceivedAt),
		zap.String("last_request_completed_at", snapAfterDrain.LastRequestCompletedAt),
		zap.Int64("in_flight_requests", snapAfterDrain.InFlightRequests),
		zap.Uint64("processed_requests_total", snapAfterDrain.ProcessedRequests),
	)

	// Step 4: GracefulStop — stop accepting new requests and wait up to SHUTDOWN_TIMEOUT for in-flight RPCs
	gracefulStart := time.Now().UTC()
	log.Info("STEP 4/6: Starting gRPC GracefulStop; rejecting new connections and draining in-flight RPCs",
		zap.Time("graceful_stop_started_at", gracefulStart),
		zap.Int64("in_flight_at_start", snapAfterDrain.InFlightRequests),
	)
	healthMgr.CloseWatchers()

	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		snapAfterStop := healthMgr.GetSnapshot()
		log.Info("STEP 4/6 COMPLETE: gRPC server stopped gracefully; all in-flight requests finished",
			zap.Time("graceful_stop_completed_at", time.Now().UTC()),
			zap.String("last_request_received_at", snapAfterStop.LastRequestReceivedAt),
			zap.String("last_request_completed_at", snapAfterStop.LastRequestCompletedAt),
			zap.Uint64("processed_requests_total", snapAfterStop.ProcessedRequests),
		)
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("STEP 4/6 TIMEOUT: gRPC GracefulStop timed out; forcing Stop()",
			zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
		)
		grpcSrv.Stop()
	}

	// Step 5: Close NATS connection
	log.Info("STEP 5/6: Closing NATS JetStream broker connection")
	natsBroker.Close()
	log.Info("STEP 5/6 COMPLETE: NATS connection closed",
		zap.Time("nats_closed_at", time.Now().UTC()),
	)

	// Stop auxiliary HTTP health server right before process exit
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = httpHealthSrv.Shutdown(httpCtx)
	httpCancel()

	// Step 6: Process exits cleanly
	finalSnap := healthMgr.GetSnapshot()
	log.Info("STEP 6/6: Publisher process exiting cleanly",
		zap.Time("exit_at", time.Now().UTC()),
		zap.Uint64("final_processed_requests", finalSnap.ProcessedRequests),
		zap.String("last_request_received_at", finalSnap.LastRequestReceivedAt),
		zap.String("last_request_completed_at", finalSnap.LastRequestCompletedAt),
	)
	_ = log.Sync()
}
