package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alihojaty/eventledger/internal/config"
	"github.com/alihojaty/eventledger/internal/logger"
	"github.com/alihojaty/eventledger/internal/messaging"
	"github.com/alihojaty/eventledger/internal/publisher"
	"github.com/alihojaty/eventledger/internal/subscriber"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func main() {

	// Initialize logger
	if err := logger.Init(true); err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	// Load config
	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal(
			"failed to load config",
			zap.Error(err),
		)
	}

	natsManager, err := messaging.NewNATSManager(
		messaging.NATSConfig{
			URL: cfg.NATSURL,
		},
	)

	if err != nil {
		zap.L().Fatal(
			"failed to create NATS Manager",
			zap.Error(err),
		)
	}

	if err := natsManager.Connect(
		messaging.NATSConfig{
			URL: cfg.NATSURL,
		},
	); err != nil {

		zap.L().Fatal(
			"failed to connect NATS Manager",
			zap.Error(err),
		)
	}

	js := natsManager.JetStream()

	if js == nil {
		zap.L().Fatal("failed to create NATS JetStream")
	}

	jsManager, err := messaging.NewJetStreamManager(js)

	if err != nil {
		zap.L().Fatal(
			"failed to create JetStream Manager",
			zap.Error(err),
		)
	}

	err = jsManager.EnsureStream(
		messaging.StreamConfig{
			Name:    "MESSAGES",
			Subject: "demo.messages",
			Storage: nats.FileStorage,
		},
	)

	if err != nil {
		zap.L().Fatal(
			"failed to ensure stream",
			zap.Error(err),
		)
	}

	consumerManager, err := messaging.NewConsumerManager(js)

	if err != nil {
		zap.L().Fatal(
			"failed to create consumer manager",
			zap.Error(err),
		)
	}

	err = consumerManager.EnsureConsumer(
		messaging.ConsumerConfig{
			StreamName:    "MESSAGES",
			DurableName:   "message-worker",
			FilterSubject: "demo.messages",
			AckWait:       30 * time.Second,
		},
	)

	if err != nil {
		zap.L().Fatal(
			"failed to ensure consumer",
			zap.Error(err),
		)
	}

	zap.L().Info(
		"EventLedger started",
		zap.String("nats_url", cfg.NATSURL),
		zap.String("nats_port", cfg.NATSPort),
	)

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	defer cancel()

	pub, err := publisher.New(
		natsManager.Connection(),
		"demo.messages",
		3*time.Second,
	)

	if err != nil {
		zap.L().Fatal(
			"failed to create publisher",
			zap.Error(err),
		)
	}

	sub, err := subscriber.New(
		natsManager.JetStream(),
		"MESSAGES",
		"message-worker",
		"demo.messages",
	)

	if err != nil {
		zap.L().Fatal(
			"failed to create subscriber",
			zap.Error(err),
		)
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := pub.Run(ctx); err != nil {
			zap.L().Error(
				"publisher stopped with error",
				zap.Error(err),
			)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := sub.Run(ctx); err != nil {
			zap.L().Error(
				"subscriber stopped with error",
				zap.Error(err),
			)
		}
	}()

	<-ctx.Done()

	zap.L().Info(
		"shutdown signal received",
	)

	cancel()

	wg.Wait()

	zap.L().Info(
		"workers stopped successfully",
	)

	if err := natsManager.Close(); err != nil {

		zap.L().Fatal(
			"failed to close NATS Manager",
			zap.Error(err),
		)
	}

	zap.L().Info(
		"EventLedger shutdown complete",
	)
}
