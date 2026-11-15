package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/logger"
	"github.com/alihojaty/reliable-deposit-processor/pkg/metricsx"
	"github.com/alihojaty/reliable-deposit-processor/services/dashboard/internal/metrics"
	"github.com/alihojaty/reliable-deposit-processor/services/dashboard/internal/server"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logLevel := getEnv("LOG_LEVEL", "info")
	baseLog, err := logger.New(logLevel)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "dashboard-local"
	}
	log := baseLog.With(
		zap.String("service", "dashboard"),
		zap.String("instance_id", hostname),
	)

	natsURL := getEnv("NATS_URL", "nats://localhost:4222")
	dbURL, err := resolveDatabaseURL()
	if err != nil {
		log.Fatal("Failed to resolve database configuration", zap.Error(err))
	}
	apiKey, err := resolveAPIKey()
	if err != nil {
		log.Fatal("Failed to resolve API key configuration", zap.Error(err))
	}
	port := getEnv("PORT", "8080")
	metricsPort, err := strconv.Atoi(getEnv("METRICS_PORT", "9090"))
	if err != nil || metricsPort <= 0 {
		metricsPort = 9090
	}

	nc := connectNATSWithRetry(ctx, natsURL, log)
	if nc == nil {
		return
	}

	var db *sql.DB
	if dbURL != "" {
		db, err = sql.Open("postgres", dbURL)
		if err != nil {
			log.Warn("DB open failed", zap.Error(err))
		} else {
			db.SetMaxOpenConns(15)
			db.SetMaxIdleConns(5)
			db.SetConnMaxLifetime(5 * time.Minute)
			db.SetConnMaxIdleTime(2 * time.Minute)
		}
	}

	dbName := getEnv("DB_NAME", "deposits_db")
	promReg := metricsx.NewRegistry("dashboard", db, dbName)
	dashMetrics := metrics.NewDashboardMetrics(promReg)
	metricsSrv := metricsx.StartServer(metricsPort, promReg, log)

	gatewayURL := getEnv("GATEWAY_URL", "http://localhost:8081")

	srv := server.NewDashboardServer(nc, db, gatewayURL, apiKey)
	srv.SetMetrics(dashMetrics)
	if err := srv.StartNATSListener(ctx); err != nil {
		log.Warn("Initial NATS listener registration delayed", zap.Error(err))
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		log.Info("Enterprise Cluster Dashboard listening",
			zap.String("port", port),
			zap.Int("metrics_port", metricsPort),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	log.Info("Shutdown signal received; stopping dashboard HTTP server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()

	nc.Close()
	if db != nil {
		_ = db.Close()
	}

	// Shut down Prometheus metrics HTTP server LAST
	metricsCtx, metricsCancel := context.WithTimeout(context.Background(), 3*time.Second)
	metricsSrv.Shutdown(metricsCtx)
	metricsCancel()

	log.Info("Dashboard service stopped")
	_ = log.Sync()
}

func connectNATSWithRetry(ctx context.Context, natsURL string, log *zap.Logger) *nats.Conn {
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
		log.Warn("Waiting for NATS", zap.String("nats_url", natsURL), zap.Error(err))
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
