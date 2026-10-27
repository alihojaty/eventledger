package config

import (
	"os"
	"path/filepath"
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

func TestLoad_Defaults(t *testing.T) {
	// پاکسازی متغیرهای محیطی برای اطمینان از اعمال مقادیر پیش‌فرض
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
	if cfg.APIKey != "dev-secret-api-key" {
		t.Errorf("expected default APIKey 'dev-secret-api-key', got %s", cfg.APIKey)
	}
	if cfg.RequestTimeout != 3*time.Second {
		t.Errorf("expected RequestTimeout 3s, got %v", cfg.RequestTimeout)
	}
}
