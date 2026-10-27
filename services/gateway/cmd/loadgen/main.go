package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func main() {
	gatewayURL := flag.String("url", getEnv("GATEWAY_URL", "http://localhost:8081"), "Gateway base URL")
	apiKey := flag.String("api-key", getEnv("API_KEY", "dev-secret-api-key"), "X-API-Key header")
	totalReqs := flag.Int("requests", 2000, "Total number of deposit requests")
	concurrency := flag.Int("concurrency", 200, "Number of concurrent workers")
	sameKey := flag.Bool("same-key", false, "Use identical Idempotency-Key for all requests (idempotency storm test)")
	timeout := flag.Duration("timeout", 30*time.Second, "Overall load test timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			MaxConnsPerHost:     *concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	sharedKey := "loadgen-shared-" + uuid.NewString()
	jobs := make(chan int, *totalReqs)
	for i := 1; i <= *totalReqs; i++ {
		jobs <- i
	}
	close(jobs)

	var (
		accepted202  atomic.Int64
		rateLimited  atomic.Int64
		badReq       atomic.Int64
		serverErr    atomic.Int64
		networkErr   atomic.Int64
		totalLatency atomic.Int64
	)

	start := time.Now()
	var wg sync.WaitGroup

	currencies := []string{"USDT", "BTC", "IRT"}
	amounts := map[string]string{
		"USDT": "10.50",
		"BTC":  "0.00500000",
		"IRT":  "1000000",
	}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*997))

			for seq := range jobs {
				if ctx.Err() != nil {
					return
				}

				idemKey := fmt.Sprintf("loadgen-%s-%d", uuid.NewString()[:8], seq)
				userID := (seq % 20) + 1
				cur := currencies[rng.Intn(len(currencies))]
				amt := amounts[cur]

				if *sameKey {
					idemKey = sharedKey
					userID = 7
					cur = "USDT"
					amt = "125.50"
				}

				body := fmt.Sprintf(`{"user_id":%d,"currency":%q,"amount":%q}`, userID, cur, amt)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, *gatewayURL+"/v1/deposits", bytes.NewBufferString(body))
				if err != nil {
					networkErr.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-API-Key", *apiKey)
				req.Header.Set("Idempotency-Key", idemKey)

				reqStart := time.Now()
				resp, err := client.Do(req)
				totalLatency.Add(time.Since(reqStart).Microseconds())
				if err != nil {
					networkErr.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusAccepted:
					accepted202.Add(1)
				case http.StatusTooManyRequests:
					rateLimited.Add(1)
				case http.StatusBadRequest, http.StatusUnauthorized, http.StatusRequestEntityTooLarge:
					badReq.Add(1)
				default:
					serverErr.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	completedReqs := accepted202.Load() + rateLimited.Load() + badReq.Load() + serverErr.Load() + networkErr.Load()
	rps := float64(completedReqs) / max(elapsed.Seconds(), 0.001)
	avgLatencyMs := float64(totalLatency.Load()) / float64(max(completedReqs, 1)) / 1000.0

	fmt.Printf("\n=== Gateway Load Generator Report ===\n")
	fmt.Printf("Target URL      : %s/v1/deposits\n", *gatewayURL)
	fmt.Printf("Total Requests  : %d (Concurrency: %d, Same-Key: %v)\n", *totalReqs, *concurrency, *sameKey)
	fmt.Printf("Elapsed Time    : %v (Throughput: %.1f req/sec, Avg Latency: %.2f ms)\n", elapsed.Round(time.Millisecond), rps, avgLatencyMs)
	fmt.Printf("202 Accepted    : %d\n", accepted202.Load())
	fmt.Printf("429 RateLimited : %d\n", rateLimited.Load())
	fmt.Printf("4xx Client Err  : %d\n", badReq.Load())
	fmt.Printf("5xx Server Err  : %d\n", serverErr.Load())
	fmt.Printf("Network Errors  : %d\n", networkErr.Load())
}

func getEnv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
