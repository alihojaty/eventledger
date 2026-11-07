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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const (
	numUsers      = 20
	numCurrencies = 3
	numTiers      = 7
	cyclePeriod   = numUsers * numCurrencies * numTiers // 20 * 3 * 7 = 420

	// Histogram: 100,000 buckets of 100 microseconds (0.1ms) covering 0..10s
	histBuckets   = 100_000
	bucketWidthUs = 100
)

var currencies = [numCurrencies]string{"USDT", "BTC", "IRT"}

type LatencyHistogram struct {
	buckets  [histBuckets + 1]atomic.Uint64
	total    atomic.Uint64
	sumMicro atomic.Uint64
}

func (h *LatencyHistogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	idx := int(us / bucketWidthUs)
	if idx >= histBuckets {
		idx = histBuckets
	}
	h.buckets[idx].Add(1)
	h.total.Add(1)
	h.sumMicro.Add(uint64(us))
}

func (h *LatencyHistogram) PercentileMs(p float64) float64 {
	tot := h.total.Load()
	if tot == 0 {
		return 0
	}
	target := uint64(float64(tot)*p + 0.5)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := 0; i <= histBuckets; i++ {
		cum += h.buckets[i].Load()
		if cum >= target {
			return float64(i*bucketWidthUs) / 1000.0
		}
	}
	return float64(histBuckets*bucketWidthUs) / 1000.0
}

func (h *LatencyHistogram) AvgMs() float64 {
	tot := h.total.Load()
	if tot == 0 {
		return 0
	}
	return float64(h.sumMicro.Load()) / float64(tot) / 1000.0
}

// DeterministicDeposit derives (userID, currency, amountStr, amountDec) purely from seq (1-based).
func DeterministicDeposit(seq int64) (int, string, string, decimal.Decimal) {
	idx := seq - 1
	userID := int(idx%numUsers) + 1
	cur := currencies[idx%numCurrencies]
	tier := (idx % numTiers) + 1

	var amountStr string
	switch cur {
	case "USDT":
		// 10.25, 20.25, ..., 70.25
		amountStr = fmt.Sprintf("%d0.25", tier)
	case "BTC":
		// 0.00100000, 0.00200000, ..., 0.00700000
		amountStr = fmt.Sprintf("0.00%d00000", tier)
	case "IRT":
		// 100000, 200000, ..., 700000
		amountStr = fmt.Sprintf("%d00000", tier)
	}

	return userID, cur, amountStr, decimal.RequireFromString(amountStr)
}

// ComputeExpectedBalances calculates the exact expected balance per user and currency
// for sequences 1..totalReqs in O(420) constant time and O(1) memory.
func ComputeExpectedBalances(totalReqs int64) map[int]map[string]decimal.Decimal {
	res := make(map[int]map[string]decimal.Decimal, numUsers)
	for u := 1; u <= numUsers; u++ {
		res[u] = map[string]decimal.Decimal{
			"USDT": decimal.Zero,
			"BTC":  decimal.Zero,
			"IRT":  decimal.Zero,
		}
	}

	if totalReqs <= 0 {
		return res
	}

	fullCycles := totalReqs / cyclePeriod
	remainder := totalReqs % cyclePeriod

	for r := int64(0); r < cyclePeriod; r++ {
		count := fullCycles
		if r < remainder {
			count++
		}
		if count == 0 {
			continue
		}
		seq := r + 1
		u, cur, _, amt := DeterministicDeposit(seq)
		totalForResidue := amt.Mul(decimal.NewFromInt(count))
		res[u][cur] = res[u][cur].Add(totalForResidue)
	}

	return res
}

func main() {
	gatewayURL := flag.String("url", getEnv("GATEWAY_URL", "http://localhost:8081"), "Gateway base URL")
	apiKey := flag.String("api-key", getEnv("API_KEY", ""), "X-API-Key header")
	apiKeyFile := flag.String("api-key-file", getEnv("API_KEY_FILE", ".secrets/api_key"), "Path to file containing X-API-Key")
	runID := flag.String("run-id", getEnv("RUN_ID", fmt.Sprintf("%d", time.Now().UTC().Unix())), "Unique RUN_ID for event_id prefix run-<RUN_ID>-<seq>")
	totalReqs := flag.Int64("requests", 5_000_000, "Total number of deposit requests (target sequence N)")
	fromSeq := flag.Int64("from", 1, "Starting sequence number (for resuming an interrupted run)")
	concurrency := flag.Int("concurrency", 256, "Number of concurrent HTTP worker goroutines")
	sameKey := flag.Bool("same-key", false, "Use identical Idempotency-Key for all requests (idempotency storm test)")
	timeout := flag.Duration("timeout", 2*time.Hour, "Overall load test timeout")
	reqTimeout := flag.Duration("req-timeout", 5*time.Second, "Per-HTTP-request timeout")
	initialBackoff := flag.Duration("initial-backoff", 30*time.Millisecond, "Initial retry backoff on 429/5xx/network errors")
	timelineFile := flag.String("timeline-log", "loadgen-timeline.log", "File path to write timestamped RPS & 5xx timeline")
	flag.Parse()

	if *apiKey == "" && *apiKeyFile != "" {
		if b, err := os.ReadFile(*apiKeyFile); err == nil {
			*apiKey = strings.TrimSpace(string(b))
		}
	}
	if *apiKey == "" {
		_, _ = fmt.Fprintln(os.Stderr, "Error: -api-key / API_KEY or -api-key-file / API_KEY_FILE is required")
		os.Exit(1)
	}
	if *fromSeq < 1 {
		*fromSeq = 1
	}
	if *totalReqs < *fromSeq {
		_, _ = fmt.Fprintf(os.Stderr, "Error: -requests (%d) must be >= -from (%d)\n", *totalReqs, *fromSeq)
		os.Exit(1)
	}

	var tlWriter io.Writer = os.Stdout
	var tlFile *os.File
	if *timelineFile != "" {
		if f, err := os.OpenFile(*timelineFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			tlFile = f
			defer func() { _ = tlFile.Close() }()
			tlWriter = io.MultiWriter(os.Stdout, tlFile)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client := &http.Client{
		Timeout: *reqTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			MaxConnsPerHost:     *concurrency * 2,
			IdleConnTimeout:     60 * time.Second,
			DisableCompression:  true,
		},
	}

	var (
		nextSeq            atomic.Int64
		completedTotal     atomic.Int64
		firstTrySuccess    atomic.Int64
		retriedReqsSuccess atomic.Int64
		totalRetryAttempts atomic.Int64
		finalFailures      atomic.Int64
		rateLimited429     atomic.Int64
		serverErr5xx       atomic.Int64
		networkErr         atomic.Int64
		window5xx          atomic.Int64
		hist               LatencyHistogram
	)
	nextSeq.Store(*fromSeq - 1)

	sharedKey := "loadgen-shared-" + uuid.NewString()
	targetURL := strings.TrimRight(*gatewayURL, "/") + "/v1/deposits"
	expectedToExecute := *totalReqs - *fromSeq + 1

	start := time.Now()
	_, _ = fmt.Fprintf(tlWriter, "[%s] LOADGEN_START run_id=%s from=%d to=%d total_to_send=%d concurrency=%d\n",
		start.UTC().Format(time.RFC3339), *runID, *fromSeq, *totalReqs, expectedToExecute, *concurrency)

	// Background time-series logger (every 5 seconds) for RPS & 5xx correlation with chaos.sh
	doneMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastCompleted := int64(0)
		lastTick := start
		for {
			select {
			case <-doneMonitor:
				return
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				curCompleted := completedTotal.Load()
				dt := now.Sub(lastTick).Seconds()
				if dt <= 0 {
					dt = 1
				}
				instRPS := float64(curCompleted-lastCompleted) / dt
				w5xx := window5xx.Swap(0)
				lastCompleted = curCompleted
				lastTick = now

				_, _ = fmt.Fprintf(tlWriter, "[%s] LOADGEN_PROGRESS completed=%d/%d (%.1f%%) rps=%.1f window_5xx=%d total_5xx=%d total_retries=%d\n",
					now.UTC().Format(time.RFC3339),
					curCompleted,
					expectedToExecute,
					float64(curCompleted)*100.0/float64(expectedToExecute),
					instRPS,
					w5xx,
					serverErr5xx.Load(),
					totalRetryAttempts.Load(),
				)
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*7919))

			for {
				seq := nextSeq.Add(1)
				if seq > *totalReqs {
					return
				}

				userID, cur, amt, _ := DeterministicDeposit(seq)
				idemKey := fmt.Sprintf("run-%s-%d", *runID, seq)

				if *sameKey {
					idemKey = sharedKey
					userID = 7
					cur = "USDT"
					amt = "125.50"
				}

				bodyBytes := []byte(fmt.Sprintf(`{"user_id":%d,"currency":%q,"amount":%q}`, userID, cur, amt))
				backoff := *initialBackoff

				for attempt := 1; ; attempt++ {
					if ctx.Err() != nil {
						finalFailures.Add(1)
						return
					}

					req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
					if err != nil {
						finalFailures.Add(1)
						break
					}
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("X-API-Key", *apiKey)
					req.Header.Set("Idempotency-Key", idemKey)

					reqStart := time.Now()
					resp, err := client.Do(req)
					latency := time.Since(reqStart)
					hist.Record(latency)

					if err != nil {
						networkErr.Add(1)
						totalRetryAttempts.Add(1)
						sleepWithJitter(ctx, backoff, rng)
						backoff = minDuration(backoff*2, time.Second)
						continue
					}

					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()

					if resp.StatusCode == http.StatusAccepted {
						if attempt == 1 {
							firstTrySuccess.Add(1)
						} else {
							retriedReqsSuccess.Add(1)
						}
						completedTotal.Add(1)
						break
					}

					if resp.StatusCode == http.StatusTooManyRequests {
						rateLimited429.Add(1)
						totalRetryAttempts.Add(1)
						sleepWithJitter(ctx, backoff, rng)
						backoff = minDuration(backoff*2, time.Second)
						continue
					}

					if resp.StatusCode >= 500 {
						s5 := serverErr5xx.Add(1)
						w5 := window5xx.Add(1)
						totalRetryAttempts.Add(1)
						// Log first 5 occurrences per 5s window with exact timestamp for timeline correlation
						if w5 <= 5 {
							_, _ = fmt.Fprintf(tlWriter, "[%s] GATEWAY_5XX status=%d key=%s attempt=%d total_5xx=%d\n",
								time.Now().UTC().Format(time.RFC3339Nano), resp.StatusCode, idemKey, attempt, s5)
						}
						sleepWithJitter(ctx, backoff, rng)
						backoff = minDuration(backoff*2, time.Second)
						continue
					}

					// Non-retryable 4xx error
					finalFailures.Add(1)
					break
				}
			}
		}(w)
	}

	wg.Wait()
	close(doneMonitor)

	elapsed := time.Since(start)
	doneCount := completedTotal.Load()
	rps := float64(doneCount) / max(elapsed.Seconds(), 0.001)

	_, _ = fmt.Fprintf(tlWriter, "[%s] LOADGEN_FINISHED run_id=%s completed=%d failed=%d rps=%.1f elapsed=%v\n",
		time.Now().UTC().Format(time.RFC3339), *runID, doneCount, finalFailures.Load(), rps, elapsed.Round(time.Millisecond))

	fmt.Printf("\n====================================================================\n")
	fmt.Printf("                GATEWAY LOAD GENERATOR FINAL REPORT                 \n")
	fmt.Printf("====================================================================\n")
	fmt.Printf("Run ID                       : %s (key pattern: run-%s-<seq>)\n", *runID, *runID)
	fmt.Printf("Sequence Range               : %d .. %d (Total Target: %d)\n", *fromSeq, *totalReqs, *totalReqs)
	fmt.Printf("Requests Executed This Run   : %d\n", expectedToExecute)
	fmt.Printf("Succeeded on First Attempt   : %d\n", firstTrySuccess.Load())
	fmt.Printf("Succeeded After Retry        : %d (Total Retry Attempts: %d)\n", retriedReqsSuccess.Load(), totalRetryAttempts.Load())
	fmt.Printf("  - 429 Rate Limited Retries : %d\n", rateLimited429.Load())
	fmt.Printf("  - 5xx Server Err Retries   : %d\n", serverErr5xx.Load())
	fmt.Printf("  - Network Error Retries    : %d\n", networkErr.Load())
	fmt.Printf("Final Failures (Must be 0)   : %d\n", finalFailures.Load())
	fmt.Printf("--------------------------------------------------------------------\n")
	fmt.Printf("Elapsed Time                 : %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput (RPS)             : %.2f req/sec\n", rps)
	fmt.Printf("Latency p50                  : %.2f ms\n", hist.PercentileMs(0.50))
	fmt.Printf("Latency p95                  : %.2f ms\n", hist.PercentileMs(0.95))
	fmt.Printf("Latency p99                  : %.2f ms\n", hist.PercentileMs(0.99))
	fmt.Printf("Latency Avg                  : %.2f ms\n", hist.AvgMs())
	fmt.Printf("====================================================================\n")

	if !*sameKey {
		expectedMap := ComputeExpectedBalances(*totalReqs)
		fmt.Printf("\n=== Expected Cumulative Balance Per User & Currency (Seq 1..%d) ===\n", *totalReqs)
		fmt.Printf("%-8s | %-20s | %-20s | %-20s\n", "User ID", "USDT", "BTC", "IRT")
		fmt.Printf("---------+----------------------+----------------------+---------------------\n")

		totalUSDT := decimal.Zero
		totalBTC := decimal.Zero
		totalIRT := decimal.Zero

		users := make([]int, 0, numUsers)
		for u := range expectedMap {
			users = append(users, u)
		}
		sort.Ints(users)

		for _, u := range users {
			usdt := expectedMap[u]["USDT"]
			btc := expectedMap[u]["BTC"]
			irt := expectedMap[u]["IRT"]
			totalUSDT = totalUSDT.Add(usdt)
			totalBTC = totalBTC.Add(btc)
			totalIRT = totalIRT.Add(irt)

			fmt.Printf("%-8d | %-20s | %-20s | %-20s\n",
				u, usdt.StringFixed(2), btc.StringFixed(8), irt.StringFixed(0))
		}
		fmt.Printf("---------+----------------------+----------------------+---------------------\n")
		fmt.Printf("%-8s | %-20s | %-20s | %-20s\n",
			"TOTAL", totalUSDT.StringFixed(2), totalBTC.StringFixed(8), totalIRT.StringFixed(0))
		fmt.Printf("====================================================================\n")
	}

	if finalFailures.Load() > 0 {
		os.Exit(1)
	}
}

func sleepWithJitter(ctx context.Context, base time.Duration, rng *rand.Rand) {
	jitter := time.Duration(rng.Int63n(int64(max(base/2, time.Millisecond))))
	wait := base + jitter
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func getEnv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
