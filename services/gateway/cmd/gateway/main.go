package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	depositv1 "github.com/alihojaty/reliable-deposit-processor/pkg/pb/deposit/v1"
	"github.com/alihojaty/reliable-deposit-processor/pkg/tlscfg"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/discovery"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/handler"
	"github.com/alihojaty/reliable-deposit-processor/services/gateway/internal/ratelimit"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // Enables gRPC client-side Health.Watch load balancing
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
)

const grpcServiceConfigJSON = `{
	"loadBalancingConfig": [{"round_robin": {}}],
	"healthCheckConfig": {"serviceName": ""}
}`

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	if cfg.APIKey == "" {
		_, _ = fmt.Fprintln(os.Stderr, "API_KEY or API_KEY_FILE is required and must not be empty")
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

	dnsBuilder := discovery.NewBuilder(cfg.DNSRefreshInterval, log)
	resolver.Register(dnsBuilder)

	var gwHandler *handler.GatewayHandler

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(grpcServiceConfigJSON),
		grpc.WithInitialWindowSize(4 * 1024 * 1024),
		grpc.WithInitialConnWindowSize(16 * 1024 * 1024),
		grpc.WithWriteBufferSize(256 * 1024),
		grpc.WithReadBufferSize(256 * 1024),
		grpc.WithUnaryInterceptor(
			handler.RetryUnavailableInterceptor(
				cfg.GRPCMaxAttempts,
				cfg.GRPCRetryBackoff,
				func() {
					if gwHandler != nil {
						gwHandler.IncGRPCRetry()
					}
				},
				log,
			),
		),
	}

	if cfg.MTLSEnabled {
		creds, err := tlscfg.ClientTransportCredentials(
			cfg.TLSCertFile,
			cfg.TLSKeyFile,
			cfg.TLSCAFile,
		)
		if err != nil {
			log.Fatal("Failed to configure gateway mTLS credentials", zap.Error(err))
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	targetAddr := cfg.PublisherGRPCAddr
	if !strings.Contains(targetAddr, "://") {
		targetAddr = fmt.Sprintf("swarmdns:///%s", targetAddr)
	}

	grpcConn, err := grpc.NewClient(targetAddr, dialOpts...)
	if err != nil {
		log.Fatal("Failed to create gRPC client", zap.Error(err))
	}
	defer func() { _ = grpcConn.Close() }()

	pubClient := depositv1.NewDepositPublisherServiceClient(grpcConn)
	healthClient := grpc_health_v1.NewHealthClient(grpcConn)
	limiter := ratelimit.NewKeyRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst)

	gwHandler = handler.NewGatewayHandler(
		pubClient,
		healthClient,
		cfg.APIKey,
		cfg.RequestTimeout,
		log,
		handler.WithRateLimiter(limiter),
		handler.WithMTLSEnabled(cfg.MTLSEnabled),
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
			zap.String("publisher_grpc_target", targetAddr),
			zap.Duration("dns_refresh_interval", cfg.DNSRefreshInterval),
			zap.Duration("request_timeout", cfg.RequestTimeout),
			zap.Int("grpc_max_attempts", cfg.GRPCMaxAttempts),
			zap.Bool("mtls_enabled", cfg.MTLSEnabled),
			zap.Float64("rate_limit_rps", cfg.RateLimitRPS),
			zap.Int("rate_limit_burst", cfg.RateLimitBurst),
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
