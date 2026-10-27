package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/generator"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/grpcserver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
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

	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()

	natsBroker, err := broker.NewNATSBroker(initCtx, cfg.NatsURL, log)
	if err != nil {
		log.Fatal("Failed to initialize NATS JetStream broker", zap.Error(err))
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.PublisherGRPCPort))
	if err != nil {
		natsBroker.Close()
		log.Fatal("Failed to listen on gRPC port", zap.Int("port", cfg.PublisherGRPCPort), zap.Error(err))
	}

	grpcSrv := grpc.NewServer(grpcserver.UnaryInterceptors(log))
	pubGRPC := grpcserver.NewServer(natsBroker, log)
	depositv1.RegisterDepositPublisherServiceServer(grpcSrv, pubGRPC)

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Monitor NATS connection status for readiness checks
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				return
			case <-ticker.C:
				if natsBroker.IsConnected() {
					healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
				} else {
					healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
				}
			}
		}
	}()

	go func() {
		log.Info("Publisher gRPC server listening", zap.Int("grpc_port", cfg.PublisherGRPCPort))
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("gRPC server stopped with error", zap.Error(err))
		}
	}()

	// Keep background simulator (disabled by default, controllable via Dashboard)
	pub := generator.NewPublisher(natsBroker, cfg.PublishInterval, cfg.PublishEnabled, log)
	pub.StartControlListener(sigCtx, cfg.NatsURL)
	go pub.Start(sigCtx)

	<-sigCtx.Done()
	log.Info("Shutdown signal received; stopping publisher gRPC server gracefully")

	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("Publisher gRPC server stopped gracefully")
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("Publisher gRPC graceful stop timed out; forcing stop")
		grpcSrv.Stop()
	}

	natsBroker.Close()
	log.Info("Publisher service stopped")
	_ = log.Sync()
}
