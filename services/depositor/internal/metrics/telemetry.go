package metrics

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type DepositorTelemetry struct {
	InstanceID      string    `json:"instance_id"`
	WorkerCount     int       `json:"worker_count"`
	ProcessedTotal  uint64    `json:"processed_total"`
	FailedTotal     uint64    `json:"failed_total"`
	DuplicatesTotal uint64    `json:"duplicates_total"`
	CurrentTPS      float64   `json:"current_tps"`
	Timestamp       time.Time `json:"timestamp"`
}

func StartTelemetryReporter(
	ctx context.Context,
	natsURL string,
	workerCount int,
	getSnapshot func() Snapshot,
	activeWorkersFn ...func() int,
) {
	instanceID, err := os.Hostname()
	if err != nil || instanceID == "" {
		instanceID = "depositor-unknown"
	}

	var getActiveWorkers func() int
	if len(activeWorkersFn) > 0 && activeWorkersFn[0] != nil {
		getActiveWorkers = activeWorkersFn[0]
	}

	go func() {
		nc, err := nats.Connect(
			natsURL,
			nats.Name("reliable-deposit-telemetry-"+instanceID),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err != nil {
			return
		}
		defer func() {
			_ = nc.FlushTimeout(1 * time.Second)
			nc.Close()
		}()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastProcessed uint64
		lastTick := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				snap := getSnapshot()

				proc := snap.SessionProcessed
				if proc == 0 && snap.MessagesProcessed >= snap.RestoredFromDB {
					proc = snap.MessagesProcessed - snap.RestoredFromDB
				}

				fail := snap.MessagesInvalid + snap.MessagesRetried
				dup := snap.MessagesDuplicate

				elapsedSec := now.Sub(lastTick).Seconds()
				if elapsedSec <= 0 {
					elapsedSec = 1.0
				}

				var tps float64
				if proc >= lastProcessed {
					tps = float64(proc-lastProcessed) / elapsedSec
				}
				lastProcessed = proc
				lastTick = now

				currentWorkers := workerCount
				if getActiveWorkers != nil {
					currentWorkers = getActiveWorkers()
				}

				payload := DepositorTelemetry{
					InstanceID:      instanceID,
					WorkerCount:     currentWorkers,
					ProcessedTotal:  proc,
					FailedTotal:     fail,
					DuplicatesTotal: dup,
					CurrentTPS:      tps,
					Timestamp:       now.UTC(),
				}

				data, err := json.Marshal(payload)
				if err == nil && nc.IsConnected() {
					_ = nc.Publish("telemetry.depositor", data)
				}
			}
		}
	}()
}
