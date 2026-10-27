package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	dbURL := getEnv("DATABASE_URL", "")
	port := getEnv("PORT", "8080")

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	var db *sql.DB
	if dbURL != "" {
		db, err = sql.Open("postgres", dbURL)
		if err != nil {
			log.Printf("Warning: DB connection failed: %v", err)
		} else {
			defer func() { _ = db.Close() }()
		}
	}

	gatewayURL := getEnv("GATEWAY_URL", "http://localhost:8081")
	apiKey := getEnv("API_KEY", "dev-secret-api-key")

	srv := server.NewDashboardServer(nc, db, gatewayURL, apiKey)
	if err := srv.StartNATSListener(ctx); err != nil {
		log.Fatalf("Failed to start NATS listener: %v", err)
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

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
