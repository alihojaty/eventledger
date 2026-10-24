package worker

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

var ErrWorkerAlreadyStopped = fmt.Errorf("worker is already stopped")

var ErrInvalidWorkerID = fmt.Errorf("invalid worker ID")

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
			jetstream.PullMaxMessages(1),
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
