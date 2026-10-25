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
) {
	instanceID, err := os.Hostname()
	if err != nil || instanceID == "" {
		instanceID = "depositor-unknown"
	}

	go func() {
		nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
		if err != nil {
			return
		}
		defer nc.Close()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastProcessed uint64
		initialized := false

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				snap := getSnapshot()

				var proc uint64
				if snap.MessagesProcessed >= snap.RestoredFromDB {
					proc = snap.MessagesProcessed - snap.RestoredFromDB
				} else {
					proc = snap.MessagesProcessed
				}

				fail := snap.MessagesInvalid + snap.MessagesRetried
				dup := snap.MessagesDuplicate

				if !initialized {
					lastProcessed = proc
					initialized = true
				}

				tps := float64(proc - lastProcessed)
				if proc < lastProcessed {
					tps = 0
				}
				lastProcessed = proc

				payload := DepositorTelemetry{
					InstanceID:      instanceID,
					WorkerCount:     workerCount,
					ProcessedTotal:  proc,
					FailedTotal:     fail,
					DuplicatesTotal: dup,
					CurrentTPS:      tps,
					Timestamp:       now.UTC(),
				}

				data, err := json.Marshal(payload)
				if err == nil {
					_ = nc.Publish("telemetry.depositor", data)
				}
			}
		}
	}()
}
