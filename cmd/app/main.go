package main

import (
	"log"
	"time"

	"github.com/alihojaty/eventledger/internal/config"
	"github.com/alihojaty/eventledger/internal/logger"
	"github.com/alihojaty/eventledger/internal/messaging"
	"go.uber.org/zap"
)

func main() {
	if err := logger.Init(true); err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal("failed to load config",
			zap.Error(err),
		)
	}

	natsManager, err := messaging.NewNATSManager(
		messaging.NATSConfig{
			URL: cfg.NATSURL,
		})
	if err != nil {
		zap.L().Fatal("failed to create NATS Manager", zap.Error(err))
	}

	if err := natsManager.Connect(
		messaging.NATSConfig{
			URL: cfg.NATSURL,
		}); err != nil {

		zap.L().Fatal("failed to connect NATS Manager", zap.Error(err))
	}

	zap.L().Info("NATS Manager connected")

	time.Sleep(10 * time.Second)

	if err := natsManager.Close(); err != nil {
		zap.L().Fatal("failed to close NATS Manager", zap.Error(err))
	}

	zap.L().Info("EventLedger started",
		zap.String("nats_url", cfg.NATSURL),
		zap.String("nats_port", cfg.NATSPort),
	)

}
