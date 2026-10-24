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

	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log.Info("Depositor service started",
		zap.Int("worker_count", cfg.WorkerCount),
		zap.Duration("retry_interval", cfg.RetryInterval),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
		zap.Int("metrics_port", cfg.MetricsPort),
	)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	workCtx, workCancel := context.WithCancel(context.Background())
	defer workCancel()

	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()

	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()

	repo, err := repository.NewPostgresRepository(initCtx, cfg.DatabaseURL, cfg.MigrationsPath, log)
	if err != nil {
		log.Fatal("Failed to initialize PostgreSQL repository", zap.Error(err))
	}

	natsBroker, err := broker.NewNATSBroker(initCtx, cfg.NatsURL, log)
	if err != nil {
		_ = repo.Close()
		log.Fatal("Failed to initialize NATS JetStream broker", zap.Error(err))
	}

	consumer, err := natsBroker.CreateOrUpdateDepositConsumer(initCtx, cfg.WorkerCount*2)
	if err != nil {
		natsBroker.Close()
		_ = repo.Close()
		log.Fatal("Failed to initialize NATS durable consumer", zap.Error(err))
	}

	metricsCollector := metrics.NewCollector()
	prevProcessed, _ := repo.GetTotalProcessedCount(initCtx)
	qStats, _ := natsBroker.GetQueueStats(initCtx)
	metricsCollector.RestoreState(prevProcessed, qStats.DLQTotalMsgs)

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

	metricsCollector.EnableDashboard(consumerGroup, natsBroker, repo, nil)
	go metricsCollector.StartServer(srvCtx, cfg.MetricsPort, log)

	termCtrl := worker.NewTerminalController(consumerGroup, os.Stdin, os.Stdout, log)
	go termCtrl.Start(sigCtx)

	<-sigCtx.Done()

	log.Info("Shutdown signal received")
	consumerGroup.DrainAll()

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
		)
		workCancel()
		<-waitDone
		log.Info("Workers stopped")
	}

	srvCancel()
	natsBroker.Close()
	_ = repo.Close()

	log.Info("Depositor service stopped")
	_ = log.Sync()
}
