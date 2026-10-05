package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	NATSURL  string
	NATSPort string
}

func Load() (*Config, error) {
	err := godotenv.Load()

	if err != nil {
		return nil, fmt.Errorf("error loading .env file")
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

	return cfg, nil
}
