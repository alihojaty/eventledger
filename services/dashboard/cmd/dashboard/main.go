package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/services/dashboard/internal/server"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	natsURL := getEnv("NATS_URL", "nats://localhost:4222")
	dbURL, err := resolveDatabaseURL()
	if err != nil {
		log.Fatalf("Failed to resolve database configuration: %v", err)
	}
	apiKey, err := resolveAPIKey()
	if err != nil {
		log.Fatalf("Failed to resolve API key configuration: %v", err)
	}
	port := getEnv("PORT", "8080")

	nc := connectNATSWithRetry(ctx, natsURL)
	if nc == nil {
		return
	}
	defer nc.Close()

	var db *sql.DB
	if dbURL != "" {
		db, err = sql.Open("postgres", dbURL)
		if err != nil {
			log.Printf("Warning: DB open failed: %v", err)
		} else {
			db.SetMaxOpenConns(15)
			db.SetMaxIdleConns(5)
			defer func() { _ = db.Close() }()
		}
	}

	gatewayURL := getEnv("GATEWAY_URL", "http://localhost:8081")

	srv := server.NewDashboardServer(nc, db, gatewayURL, apiKey)
	if err := srv.StartNATSListener(ctx); err != nil {
		log.Printf("Warning: initial NATS listener registration delayed: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("Enterprise Cluster Dashboard listening on http://0.0.0.0:%s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func connectNATSWithRetry(ctx context.Context, natsURL string) *nats.Conn {
	backoff := 500 * time.Millisecond
	for {
		nc, err := nats.Connect(
			natsURL,
			nats.Name("reliable-deposit-dashboard"),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(1*time.Second),
		)
		if err == nil {
			return nc
		}
		log.Printf("Waiting for NATS at %s: %v", natsURL, err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	}
}

func resolveAPIKey() (string, error) {
	if file := os.Getenv("API_KEY_FILE"); file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("reading API_KEY_FILE: %w", err)
		}
		key := strings.TrimSpace(string(b))
		if key == "" {
			return "", fmt.Errorf("API_KEY_FILE %s is empty", file)
		}
		return key, nil
	}
	if key := strings.TrimSpace(os.Getenv("API_KEY")); key != "" {
		return key, nil
	}
	return "", fmt.Errorf("either API_KEY_FILE or API_KEY must be provided")
}

func resolveDatabaseURL() (string, error) {
	if file := os.Getenv("DATABASE_URL_FILE"); file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}

	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if passFile := os.Getenv("POSTGRES_PASSWORD_FILE"); passFile != "" {
		b, err := os.ReadFile(passFile)
		if err != nil {
			return "", fmt.Errorf("reading POSTGRES_PASSWORD_FILE: %w", err)
		}
		dbPass = strings.TrimSpace(string(b))
	}

	rawURL := os.Getenv("DATABASE_URL")
	if rawURL == "" && dbPass != "" {
		host := getEnv("DB_HOST", "postgres")
		port := getEnv("DB_PORT", "5432")
		user := getEnv("DB_USER", "postgres")
		name := getEnv("DB_NAME", "deposits_db")
		sslMode := getEnv("DB_SSLMODE", "disable")
		u := &url.URL{
			Scheme:   "postgres",
			User:     url.UserPassword(user, dbPass),
			Host:     fmt.Sprintf("%s:%s", host, port),
			Path:     name,
			RawQuery: "sslmode=" + url.QueryEscape(sslMode),
		}
		return u.String(), nil
	}

	if rawURL != "" && dbPass != "" {
		if parsed, err := url.Parse(rawURL); err == nil {
			user := getEnv("DB_USER", "postgres")
			if parsed.User != nil && parsed.User.Username() != "" {
				user = parsed.User.Username()
			}
			parsed.User = url.UserPassword(user, dbPass)
			return parsed.String(), nil
		}
	}

	return rawURL, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
