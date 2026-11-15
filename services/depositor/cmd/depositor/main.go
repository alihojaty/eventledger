package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	"github.com/alihojaty/reliable-deposit-processor/pkg/metricsx"
	"github.com/alihojaty/reliable-deposit-processor/services/depositor/internal/metrics"
	"github.com/alihojaty/reliable-deposit-processor/services/depositor/internal/repository"
	"github.com/alihojaty/reliable-deposit-processor/services/depositor/internal/worker"
	"go.uber.org/zap"
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
		hostname = "depositor-local"
	}
	log := baseLog.With(
		zap.String("service", "depositor"),
		zap.String("instance_id", hostname),
	)

	log.Info("Depositor service starting",
		zap.Int("worker_count", cfg.WorkerCount),
		zap.Duration("retry_interval", cfg.RetryInterval),
		zap.Duration("settlement_interval", cfg.SettlementInterval),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
		zap.Int("metrics_port", cfg.MetricsPort),
	)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	workCtx, workCancel := context.WithCancel(context.Background())
	defer workCancel()

	repo, err := repository.NewPostgresRepository(sigCtx, cfg.DatabaseURL, cfg.MigrationsPath, log)
	if err != nil {
		log.Fatal("Failed to initialize PostgreSQL repository", zap.Error(err))
	}

	// Explicit isolated Prometheus registry with Go, Process, build_info, and sql.DB pool stats collectors
	promReg := metricsx.NewRegistry("depositor", repo.DB(), cfg.DBName)
	metricsCollector := metrics.NewCollector(promReg)
	repo.SetBatchObserver(metricsCollector)
	metricsSrv := metricsx.StartServer(cfg.MetricsPort, promReg, log)

	natsBroker, err := broker.NewNATSBroker(sigCtx, cfg.NatsURL, log)
	if err != nil {
		_ = repo.Close()
		log.Fatal("Failed to initialize NATS JetStream broker", zap.Error(err))
	}

	maxAckPending := max(cfg.WorkerCount*256, 8192)
	consumer, err := natsBroker.CreateOrUpdateDepositConsumer(sigCtx, maxAckPending)
	if err != nil {
		natsBroker.Close()
		_ = repo.Close()
		log.Fatal("Failed to initialize NATS durable consumer", zap.Error(err))
	}

	var inFlightWg sync.WaitGroup
	proc := worker.NewProcessor(
		workCtx,
		&inFlightWg,
		repo,
		natsBroker,
		cfg.RetryInterval,
		log,
		metricsCollector,
	)

	consumerGroup := worker.NewConsumerGroup(cfg.WorkerCount, consumer, proc, log)
	if err := consumerGroup.Start(); err != nil {
		natsBroker.Close()
		_ = repo.Close()
		log.Fatal("Failed to start worker consumer group", zap.Error(err))
	}

	metricsCollector.WorkersActive.Set(float64(consumerGroup.ActiveWorkerCount()))
	if natsBroker.IsConnected() {
		metricsCollector.NATSConnected.Set(1)
	}

	// Background gauge updater for depositor_workers_active and depositor_nats_connected
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				return
			case <-ticker.C:
				metricsCollector.WorkersActive.Set(float64(consumerGroup.ActiveWorkerCount()))
				if natsBroker.IsConnected() {
					metricsCollector.NATSConnected.Set(1)
				} else {
					metricsCollector.NATSConnected.Set(0)
				}
			}
		}
	}()

	settlementWorker := worker.NewSettlementWorker(repo, cfg.SettlementInterval, log, metricsCollector)
	settlementWorker.Start(workCtx)

	natsBroker.OnReconnect(func(reconnCtx context.Context) {
		newConsumer, err := natsBroker.CreateOrUpdateDepositConsumer(reconnCtx, maxAckPending)
		if err != nil {
			log.Error("Failed to recreate consumer after NATS reconnect", zap.Error(err))
			return
		}
		consumerGroup.RebindConsumer(newConsumer)
	})

	metrics.StartTelemetryReporter(
		sigCtx,
		cfg.NatsURL,
		cfg.WorkerCount,
		metricsCollector.GetSnapshot,
		consumerGroup.ActiveWorkerCount,
	)

	termCtrl := worker.NewTerminalController(consumerGroup, os.Stdin, os.Stdout, log)
	go termCtrl.Start(sigCtx)

	<-sigCtx.Done()

	log.Info("Shutdown signal received")
	consumerGroup.DrainAll()
	metricsCollector.WorkersActive.Set(0)

	waitDone := make(chan struct{})
	go func() {
		inFlightWg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		log.Info("Workers stopped")
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("Shutdown timeout exceeded; canceling in-flight database transactions",
			zap.Duration("timeout", cfg.ShutdownTimeout),
			zap.Int64("in_flight_remaining", proc.InFlightCount()),
		)
		workCancel()
		<-waitDone
		log.Info("Workers stopped")
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	settlementWorker.StopAndFlush(flushCtx)
	flushCancel()

	natsBroker.Close()
	metricsCollector.NATSConnected.Set(0)
	_ = repo.Close()

	// Shut down Prometheus metrics HTTP server LAST
	metricsCtx, metricsCancel := context.WithTimeout(context.Background(), 3*time.Second)
	metricsSrv.Shutdown(metricsCtx)
	metricsCancel()

	log.Info("Depositor service stopped")
	_ = log.Sync()
}
