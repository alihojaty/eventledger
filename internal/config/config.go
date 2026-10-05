package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

type Config struct {
	NATSURL  string
	NATSPort string
}

func Load() (*Config, error) {
	zap.L().Info("loading application config")

	if err := godotenv.Load(); err != nil {
		zap.L().Warn("could not load .env file",
			zap.Error(err),
		)
	}

	cfg := &Config{
		NATSURL:  os.Getenv("NATS_URL"),
		NATSPort: os.Getenv("NATS_PORT"),
	}

	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("NATS_URL not set")
	}
	if cfg.NATSPort == "" {
		return nil, fmt.Errorf("NATS_PORT not set")
	}

	zap.L().Info("application config loaded")
	return cfg, nil
}
