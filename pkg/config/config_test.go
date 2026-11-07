package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad_OSEnvOverridesDotEnv(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	envContent := "WORKER_COUNT=5\n"

	if err := os.WriteFile(envFile, []byte(envContent), 0600); err != nil {
		t.Fatalf("failed to write temp .env file: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}

	t.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/testdb")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("WORKER_COUNT", "12")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.WorkerCount != 12 {
		t.Errorf("expected OS env WORKER_COUNT=12 to override .env (5), but got %d", cfg.WorkerCount)
	}
}

func TestLoad_Defaults_NoDefaultSecrets(t *testing.T) {
	os.Clearenv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}

	if cfg.GatewayPort != 8081 {
		t.Errorf("expected GatewayPort 8081, got %d", cfg.GatewayPort)
	}
	if cfg.PublisherGRPCPort != 50051 {
		t.Errorf("expected PublisherGRPCPort 50051, got %d", cfg.PublisherGRPCPort)
	}
	if cfg.APIKey != "" {
		t.Errorf("expected empty default APIKey, got %q", cfg.APIKey)
	}
	if cfg.DatabaseURL != "" {
		t.Errorf("expected empty default DatabaseURL when no password is provided, got %q", cfg.DatabaseURL)
	}
	if cfg.RequestTimeout != 3*time.Second {
		t.Errorf("expected RequestTimeout 3s, got %v", cfg.RequestTimeout)
	}
}

func TestLoad_SecretFiles(t *testing.T) {
	os.Clearenv()
	tmpDir := t.TempDir()

	apiKeyFile := filepath.Join(tmpDir, "api_key")
	dbPassFile := filepath.Join(tmpDir, "postgres_password")

	if err := os.WriteFile(apiKeyFile, []byte("swarm-secret-api-key\n"), 0600); err != nil {
		t.Fatalf("failed to write api_key secret file: %v", err)
	}
	if err := os.WriteFile(dbPassFile, []byte("swarm-db-pass-999\n"), 0600); err != nil {
		t.Fatalf("failed to write postgres_password secret file: %v", err)
	}

	t.Setenv("API_KEY_FILE", apiKeyFile)
	t.Setenv("POSTGRES_PASSWORD_FILE", dbPassFile)
	t.Setenv("DB_HOST", "postgres")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "postgres")
	t.Setenv("DB_NAME", "deposits_db")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load config with secret files: %v", err)
	}

	if cfg.APIKey != "swarm-secret-api-key" {
		t.Errorf("expected APIKey 'swarm-secret-api-key', got %q", cfg.APIKey)
	}
	expectedDSN := "postgres://postgres:swarm-db-pass-999@postgres:5432/deposits_db?sslmode=disable"
	if !strings.EqualFold(cfg.DatabaseURL, expectedDSN) {
		t.Errorf("expected DatabaseURL %q, got %q", expectedDSN, cfg.DatabaseURL)
	}
}
