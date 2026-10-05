package main

import (
	"log"

	"github.com/alihojaty/eventledger/internal/config"
	"github.com/alihojaty/eventledger/internal/logger"
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

	zap.L().Info("EventLedger started",
		zap.String("nats_url", cfg.NATSURL),
		zap.String("nats_port", cfg.NATSPort),
	)

}
