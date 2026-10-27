package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL     string        `env:"DATABASE_URL" envDefault:"postgres://postgres:postgres@localhost:5432/deposits_db?sslmode=disable"`
	MigrationsPath  string        `env:"MIGRATIONS_PATH" envDefault:"file://services/depositor/migrations"`
	NatsURL         string        `env:"NATS_URL" envDefault:"nats://localhost:4222"`
	WorkerCount     int           `env:"WORKER_COUNT" envDefault:"3"`
	RetryInterval   time.Duration `env:"RETRY_INTERVAL" envDefault:"5s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
	MetricsPort     int           `env:"METRICS_PORT" envDefault:"8080"`
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`

	// Publisher Configuration (Random simulator disabled by default)
	PublishEnabled    bool          `env:"PUBLISH_ENABLED" envDefault:"false"`
	PublishInterval   time.Duration `env:"PUBLISH_INTERVAL" envDefault:"1s"`
	PublisherGRPCPort int           `env:"PUBLISHER_GRPC_PORT" envDefault:"50051"`

	// Gateway Configuration
	GatewayPort       int           `env:"GATEWAY_PORT" envDefault:"8081"`
	PublisherGRPCAddr string        `env:"PUBLISHER_GRPC_ADDR" envDefault:"localhost:50051"`
	APIKey            string        `env:"API_KEY" envDefault:"dev-secret-api-key"`
	RequestTimeout    time.Duration `env:"REQUEST_TIMEOUT" envDefault:"3s"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.WorkerCount <= 0 {
		return fmt.Errorf("WORKER_COUNT must be greater than 0, got %d", c.WorkerCount)
	}
	if c.PublishInterval <= 0 {
		return fmt.Errorf("PUBLISH_INTERVAL must be greater than 0, got %v", c.PublishInterval)
	}
	if c.RetryInterval <= 0 {
		return fmt.Errorf("RETRY_INTERVAL must be greater than 0, got %v", c.RetryInterval)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than 0, got %v", c.ShutdownTimeout)
	}
	return nil
}
