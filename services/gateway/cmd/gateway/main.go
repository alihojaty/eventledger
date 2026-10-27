package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/handler"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		hostname = "gateway-local"
	}
	log := baseLog.With(
		zap.String("service", "gateway"),
		zap.String("instance_id", hostname),
	)

	// Non-blocking gRPC client initialization; reconnects automatically when publisher recovers
	grpcConn, err := grpc.NewClient(
		cfg.PublisherGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal("Failed to create gRPC client", zap.Error(err))
	}
	defer func() { _ = grpcConn.Close() }()

	pubClient := depositv1.NewDepositPublisherServiceClient(grpcConn)
	healthClient := grpc_health_v1.NewHealthClient(grpcConn)

	gwHandler := handler.NewGatewayHandler(
		pubClient,
		healthClient,
		cfg.APIKey,
		cfg.RequestTimeout,
		log,
	)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.GatewayPort),
		Handler:           gwHandler.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	go func() {
		log.Info("Gateway HTTP server started",
			zap.Int("port", cfg.GatewayPort),
			zap.String("publisher_grpc_addr", cfg.PublisherGRPCAddr),
			zap.Duration("request_timeout", cfg.RequestTimeout),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("Gateway HTTP server error", zap.Error(err))
		}
	}()

	<-sigCtx.Done()
	log.Info("Shutdown signal received; draining active HTTP requests on gateway")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("Gateway HTTP server forced to shutdown", zap.Error(err))
	}

	log.Info("Gateway service stopped")
	_ = log.Sync()
}
