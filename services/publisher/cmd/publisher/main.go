package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/config"
	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	"github.com/alihojaty/reliable-deposit-processor/services/publisher/internal/generator"
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

	log.Info("Publisher service started",
		zap.Bool("publish_enabled", cfg.PublishEnabled),
		zap.Duration("publish_interval", cfg.PublishInterval),
	)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()

	natsBroker, err := broker.NewNATSBroker(initCtx, cfg.NatsURL, log)
	if err != nil {
		log.Fatal("Failed to initialize NATS JetStream broker", zap.Error(err))
	}
	defer natsBroker.Close()

	pub := generator.NewPublisher(natsBroker, cfg.PublishInterval, cfg.PublishEnabled, log)
	pub.Start(sigCtx)

	log.Info("Publisher service stopped")
	_ = log.Sync()
}
