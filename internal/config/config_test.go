package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_OSEnvOverridesDotEnv(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")
	envContent := "DATABASE_URL=postgres://dotenv:5432/db\nNATS_URL=nats://dotenv:4222\nWORKER_COUNT=5\n"
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

	t.Setenv("WORKER_COUNT", "12")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.WorkerCount != 12 {
		t.Errorf("expected OS env WORKER_COUNT=12 to override .env (5), but got %d", cfg.WorkerCount)
	}
}
