package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	"github.com/alihojaty/reliable-deposit-processor/services/depositor/internal/repository"
	"go.uber.org/zap"
)

type JetStreamMessage interface {
	Data() []byte
	Ack() error
	NakWithDelay(delay time.Duration) error
}

type MetricsRecorder interface {
	IncReceived()
	IncProcessed()
	IncDuplicate()
	IncInvalid()
	IncDLQ()
	IncRetried()
}

type noopMetrics struct{}

func (noopMetrics) IncReceived()  {}
func (noopMetrics) IncProcessed() {}
func (noopMetrics) IncDuplicate() {}
func (noopMetrics) IncInvalid()   {}
func (noopMetrics) IncDLQ()       {}
func (noopMetrics) IncRetried()   {}

type Processor struct {
	workCtx       context.Context
	inFlightWg    *sync.WaitGroup
	inFlightCount atomic.Int64
	repo          repository.DepositRepository
	broker        broker.Broker
	retryInterval time.Duration
	logger        *zap.Logger
	metrics       MetricsRecorder
}

func NewProcessor(
	workCtx context.Context,
	inFlightWg *sync.WaitGroup,
	repo repository.DepositRepository,
	b broker.Broker,
	retryInterval time.Duration,
	logger *zap.Logger,
	metrics MetricsRecorder,
) *Processor {
	if metrics == nil {
		metrics = noopMetrics{}
	}
	if inFlightWg == nil {
		inFlightWg = &sync.WaitGroup{}
	}

	return &Processor{
		workCtx:       workCtx,
		inFlightWg:    inFlightWg,
		repo:          repo,
		broker:        b,
		retryInterval: retryInterval,
		logger:        logger,
		metrics:       metrics,
	}
}

func (p *Processor) InFlightCount() int64 {
	return p.inFlightCount.Load()
}

func (p *Processor) HandleMessage(workerID int, msg JetStreamMessage) {
	p.inFlightWg.Add(1)
	p.inFlightCount.Add(1)
	defer func() {
		p.inFlightCount.Add(-1)
		p.inFlightWg.Done()
	}()

	rawPayload := msg.Data()
	p.metrics.IncReceived()

	p.logger.Info("Message received",
		zap.Int("worker_id", workerID),
		zap.Int("payload_bytes", len(rawPayload)),
	)

	event, err := domain.ParseAndValidateDepositEvent(rawPayload)
	if err != nil {
		var invalidErr *domain.InvalidMessageError
		if errors.As(err, &invalidErr) {
			p.handleInvalidMessage(workerID, msg, rawPayload, invalidErr)
			return
		}
		p.handleInvalidMessage(workerID, msg, rawPayload, domain.NewInvalidMessageError("", err.Error(), err))
		return
	}

	dbCtx, cancel := context.WithTimeout(p.workCtx, 10*time.Second)
	defer cancel()

	err = p.repo.ProcessDeposit(dbCtx, event)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateEvent) {
			p.metrics.IncDuplicate()
			p.logger.Warn("Duplicate message detected",
				zap.Int("worker_id", workerID),
				zap.String("event_id", event.EventID),
				zap.Int("user_id", event.UserID),
			)
			if ackErr := msg.Ack(); ackErr != nil {
				p.logger.Error("Failed to ACK duplicate message",
					zap.Int("worker_id", workerID),
					zap.String("event_id", event.EventID),
					zap.Error(ackErr),
				)
			}
			return
		}

		p.metrics.IncRetried()
		p.logger.Error("Temporary failure",
			zap.Int("worker_id", workerID),
			zap.String("event_id", event.EventID),
			zap.Error(err),
		)
		p.logger.Info("Retry started",
			zap.Int("worker_id", workerID),
			zap.String("event_id", event.EventID),
			zap.Duration("retry_after", p.retryInterval),
		)

		if nakErr := msg.NakWithDelay(p.retryInterval); nakErr != nil {
			p.logger.Error("Failed to NakWithDelay message after temporary failure",
				zap.Int("worker_id", workerID),
				zap.String("event_id", event.EventID),
				zap.Error(nakErr),
			)
		}
		return
	}

	if err := msg.Ack(); err != nil {
		p.logger.Error("Failed to ACK processed message",
			zap.Int("worker_id", workerID),
			zap.String("event_id", event.EventID),
			zap.Error(err),
		)
		return
	}

	p.metrics.IncProcessed()
	p.logger.Info("Message processed",
		zap.Int("worker_id", workerID),
		zap.String("event_id", event.EventID),
		zap.Int("user_id", event.UserID),
		zap.String("currency", event.Currency),
		zap.String("amount", event.Amount.String()),
	)
}

func (p *Processor) handleInvalidMessage(
	workerID int,
	msg JetStreamMessage,
	rawPayload []byte,
	invalidErr *domain.InvalidMessageError,
) {
	p.metrics.IncInvalid()
	p.logger.Warn("Invalid message",
		zap.Int("worker_id", workerID),
		zap.String("event_id", invalidErr.EventID),
		zap.String("reason", invalidErr.Reason),
	)

	dlqPayload := domain.DLQMessage{
		EventID:       invalidErr.EventID,
		ErrorCode:     "VALIDATION_ERROR",
		OriginalEvent: string(rawPayload),
		Reason:        invalidErr.Reason,
		Timestamp:     time.Now().UTC(),
	}

	dlqCtx, cancel := context.WithTimeout(p.workCtx, 5*time.Second)
	defer cancel()

	if err := p.broker.PublishDLQ(dlqCtx, dlqPayload); err != nil {
		p.metrics.IncRetried()
		p.logger.Error("Temporary failure",
			zap.Int("worker_id", workerID),
			zap.String("reason", "failed to publish invalid message to DLQ"),
			zap.Error(err),
		)
		p.logger.Info("Retry started",
			zap.Int("worker_id", workerID),
			zap.Duration("retry_after", p.retryInterval),
		)
		_ = msg.NakWithDelay(p.retryInterval)
		return
	}

	if err := msg.Ack(); err != nil {
		p.logger.Error("Failed to ACK invalid message after moving to DLQ",
			zap.Int("worker_id", workerID),
			zap.Error(err),
		)
		return
	}

	p.metrics.IncDLQ()
	p.logger.Info("Message moved to DLQ",
		zap.Int("worker_id", workerID),
		zap.String("event_id", invalidErr.EventID),
		zap.String("reason", invalidErr.Reason),
	)
}
