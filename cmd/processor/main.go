package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/broker"
	"github.com/alihojaty/reliable-deposit-processor/internal/config"
	"github.com/alihojaty/reliable-deposit-processor/internal/logger"
	"github.com/alihojaty/reliable-deposit-processor/internal/metrics"
	"github.com/alihojaty/reliable-deposit-processor/internal/publisher"
	"github.com/alihojaty/reliable-deposit-processor/internal/repository"
	"github.com/alihojaty/reliable-deposit-processor/internal/worker"
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

	log.Info("Application started",
		zap.Int("worker_count", cfg.WorkerCount),
		zap.Bool("publish_enabled", cfg.PublishEnabled),
		zap.Duration("publish_interval", cfg.PublishInterval),
		zap.Duration("retry_interval", cfg.RetryInterval),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
		zap.Int("metrics_port", cfg.MetricsPort),
	)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	workCtx, workCancel := context.WithCancel(context.Background())
	defer workCancel()

	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()

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

	if prevProcessed > 0 || qStats.ConsumerPending > 0 {
		log.Info("Restored previous state from Database and NATS JetStream",
			zap.Uint64("already_processed_in_db", prevProcessed),
			zap.Uint64("pending_in_nats_queue", qStats.ConsumerPending),
			zap.Uint64("resuming_from_message_number", prevProcessed+1),
		)
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

	pub := publisher.NewPublisher(natsBroker, cfg.PublishInterval, cfg.PublishEnabled, log)

	// اتصال داشبورد به کنترلرهای Worker، Broker، دیتابیس و کلید زنده Publisher
	metricsCollector.EnableDashboard(consumerGroup, natsBroker, repo, pub)
	go metricsCollector.StartServer(pubCtx, cfg.MetricsPort, log)

	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		pub.Start(pubCtx)
	}()

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

	pubCancel()
	<-pubDone

	natsBroker.Close()
	_ = repo.Close()

	log.Info("Application stopped")
	_ = log.Sync()
}
