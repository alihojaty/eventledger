package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL      string `env:"DATABASE_URL"`
	DatabaseURLFile  string `env:"DATABASE_URL_FILE"`
	DBHost           string `env:"DB_HOST" envDefault:"postgres"`
	DBPort           int    `env:"DB_PORT" envDefault:"5432"`
	DBUser           string `env:"DB_USER" envDefault:"postgres"`
	DBName           string `env:"DB_NAME" envDefault:"deposits_db"`
	DBSSLMode        string `env:"DB_SSLMODE" envDefault:"disable"`
	PostgresPassword string `env:"POSTGRES_PASSWORD"`
	PostgresPassFile string `env:"POSTGRES_PASSWORD_FILE"`
	MigrationsPath   string `env:"MIGRATIONS_PATH" envDefault:"file://migrations"`

	NatsURL string `env:"NATS_URL" envDefault:"nats://localhost:4222"`

	WorkerCount        int           `env:"WORKER_COUNT" envDefault:"3"`
	RetryInterval      time.Duration `env:"RETRY_INTERVAL" envDefault:"5s"`
	SettlementInterval time.Duration `env:"SETTLEMENT_INTERVAL" envDefault:"5s"`
	ShutdownTimeout    time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
	MetricsPort        int           `env:"METRICS_PORT" envDefault:"8080"`

	PublishInterval     time.Duration `env:"PUBLISH_INTERVAL" envDefault:"1s"`
	PublishEnabled      bool          `env:"PUBLISH_ENABLED" envDefault:"false"`
	PublisherGRPCPort   int           `env:"PUBLISHER_GRPC_PORT" envDefault:"50051"`
	PublisherHealthPort int           `env:"PUBLISHER_HEALTH_PORT" envDefault:"8090"`
	DrainDelay          time.Duration `env:"DRAIN_DELAY" envDefault:"6s"`
	ForceUnhealthy      bool          `env:"FORCE_UNHEALTHY" envDefault:"false"`

	GatewayPort        int           `env:"GATEWAY_PORT" envDefault:"8081"`
	PublisherGRPCAddr  string        `env:"PUBLISHER_GRPC_ADDR" envDefault:"localhost:50051"`
	DNSRefreshInterval time.Duration `env:"DNS_REFRESH_INTERVAL" envDefault:"2s"`
	APIKey             string        `env:"API_KEY"`
	APIKeyFile         string        `env:"API_KEY_FILE"`
	RequestTimeout     time.Duration `env:"REQUEST_TIMEOUT" envDefault:"3s"`
	RateLimitRPS       float64       `env:"RATE_LIMIT_RPS" envDefault:"500"`
	RateLimitBurst     int           `env:"RATE_LIMIT_BURST" envDefault:"200"`
	GRPCMaxAttempts    int           `env:"GRPC_MAX_ATTEMPTS" envDefault:"3"`
	GRPCRetryBackoff   time.Duration `env:"GRPC_RETRY_BACKOFF" envDefault:"50ms"`

	MTLSEnabled bool   `env:"MTLS_ENABLED" envDefault:"false"`
	TLSCertFile string `env:"TLS_CERT_FILE"`
	TLSKeyFile  string `env:"TLS_KEY_FILE"`
	TLSCAFile   string `env:"TLS_CA_FILE"`

	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing environment variables: %w", err)
	}

	if err := cfg.resolveSecrets(); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) resolveSecrets() error {
	if c.APIKeyFile != "" {
		val, err := ReadSecretFile(c.APIKeyFile)
		if err != nil {
			return fmt.Errorf("reading API_KEY_FILE: %w", err)
		}
		c.APIKey = val
	}

	if c.DatabaseURLFile != "" {
		val, err := ReadSecretFile(c.DatabaseURLFile)
		if err != nil {
			return fmt.Errorf("reading DATABASE_URL_FILE: %w", err)
		}
		c.DatabaseURL = val
	}

	if c.PostgresPassFile != "" {
		val, err := ReadSecretFile(c.PostgresPassFile)
		if err != nil {
			return fmt.Errorf("reading POSTGRES_PASSWORD_FILE: %w", err)
		}
		c.PostgresPassword = val
	}

	if c.DatabaseURL == "" && c.PostgresPassword != "" {
		u := &url.URL{
			Scheme:   "postgres",
			User:     url.UserPassword(c.DBUser, c.PostgresPassword),
			Host:     fmt.Sprintf("%s:%d", c.DBHost, c.DBPort),
			Path:     c.DBName,
			RawQuery: "sslmode=" + url.QueryEscape(c.DBSSLMode),
		}
		c.DatabaseURL = u.String()
	} else if c.DatabaseURL != "" && c.PostgresPassword != "" {
		if parsed, err := url.Parse(c.DatabaseURL); err == nil {
			user := c.DBUser
			if parsed.User != nil && parsed.User.Username() != "" {
				user = parsed.User.Username()
			}
			parsed.User = url.UserPassword(user, c.PostgresPassword)
			c.DatabaseURL = parsed.String()
		}
	}

	return nil
}

func ReadSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	val := strings.TrimSpace(string(b))
	if val == "" {
		return "", fmt.Errorf("secret file %s is empty", path)
	}
	return val, nil
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
	if c.SettlementInterval <= 0 {
		c.SettlementInterval = 5 * time.Second
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than 0, got %v", c.ShutdownTimeout)
	}
	if c.DrainDelay < 0 {
		return fmt.Errorf("DRAIN_DELAY cannot be negative, got %v", c.DrainDelay)
	}
	return nil
}
