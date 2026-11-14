package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

var ErrWorkerAlreadyStopped = fmt.Errorf("worker is already stopped")

var ErrInvalidWorkerID = fmt.Errorf("invalid worker ID")

const workerPullBatchSize = 128

type ConsumerGroup struct {
	workerCount int
	consumer    jetstream.Consumer
	processor   *Processor
	logger      *zap.Logger

	mu      sync.Mutex
	members map[int]jetstream.ConsumeContext
}

func NewConsumerGroup(
	workerCount int,
	consumer jetstream.Consumer,
	processor *Processor,
	logger *zap.Logger,
) *ConsumerGroup {
	return &ConsumerGroup{
		workerCount: workerCount,
		consumer:    consumer,
		processor:   processor,
		logger:      logger,
		members:     make(map[int]jetstream.ConsumeContext, workerCount),
	}
}

func (cg *ConsumerGroup) Start() error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	for id := 1; id <= cg.workerCount; id++ {
		workerID := id

		consumeCtx, err := cg.consumer.Consume(
			func(msg jetstream.Msg) {
				cg.processor.HandleMessage(workerID, msg)
			},
			jetstream.PullMaxMessages(workerPullBatchSize),
		)
		if err != nil {
			for _, activeCtx := range cg.members {
				activeCtx.Drain()
			}
			cg.members = make(map[int]jetstream.ConsumeContext)
			return fmt.Errorf("failed to start worker %d: %w", workerID, err)
		}

		cg.members[workerID] = consumeCtx
		cg.logger.Info("Worker started", zap.Int("worker_id", workerID))
	}

	return nil
}

func (cg *ConsumerGroup) RebindConsumer(newConsumer jetstream.Consumer) {
	cg.mu.Lock()
	cg.consumer = newConsumer
	activeIDs := make([]int, 0, len(cg.members))
	for id := range cg.members {
		activeIDs = append(activeIDs, id)
	}
	cg.mu.Unlock()

	var oldContexts []jetstream.ConsumeContext

	for _, id := range activeIDs {
		workerID := id
		var (
			consumeCtx jetstream.ConsumeContext
			err        error
		)
		for attempt := 1; attempt <= 3; attempt++ {
			consumeCtx, err = newConsumer.Consume(
				func(msg jetstream.Msg) {
					cg.processor.HandleMessage(workerID, msg)
				},
				jetstream.PullMaxMessages(workerPullBatchSize),
			)
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}
		if err != nil {
			cg.logger.Error("Failed to rebind worker after NATS reconnect",
				zap.Int("worker_id", workerID),
				zap.Error(err),
			)
			continue
		}

		cg.mu.Lock()
		if oldCtx, stillActive := cg.members[workerID]; stillActive {
			cg.members[workerID] = consumeCtx
			oldContexts = append(oldContexts, oldCtx)
			cg.logger.Info("Worker rebound to NATS consumer after reconnect", zap.Int("worker_id", workerID))
		} else {
			consumeCtx.Drain()
		}
		cg.mu.Unlock()
	}

	for _, oldCtx := range oldContexts {
		oldCtx.Drain()
	}
}

func (cg *ConsumerGroup) StopWorker(workerID int) error {
	if workerID < 1 || workerID > cg.workerCount {
		return fmt.Errorf("%w: must be between 1 and %d", ErrInvalidWorkerID, cg.workerCount)
	}

	cg.mu.Lock()
	consumeCtx, exists := cg.members[workerID]
	if !exists {
		cg.mu.Unlock()
		return ErrWorkerAlreadyStopped
	}
	delete(cg.members, workerID)
	cg.mu.Unlock()

	consumeCtx.Drain()
	cg.logger.Info("Worker drained via terminal command", zap.Int("worker_id", workerID))
	return nil
}

func (cg *ConsumerGroup) DrainAll() {
	cg.mu.Lock()
	activeContexts := make([]jetstream.ConsumeContext, 0, len(cg.members))
	for id, consumeCtx := range cg.members {
		activeContexts = append(activeContexts, consumeCtx)
		delete(cg.members, id)
	}
	cg.mu.Unlock()

	for _, consumeCtx := range activeContexts {
		consumeCtx.Drain()
	}

	cg.logger.Info("Message consumption stopped")
}

func (cg *ConsumerGroup) WorkerCount() int {
	return cg.workerCount
}

func (cg *ConsumerGroup) ActiveWorkerCount() int {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return len(cg.members)
}

func (cg *ConsumerGroup) StatusMap() map[int]bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	statuses := make(map[int]bool, cg.workerCount)
	for id := 1; id <= cg.workerCount; id++ {
		_, active := cg.members[id]
		statuses[id] = active
	}
	return statuses
}
