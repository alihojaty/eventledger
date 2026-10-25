package generator

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Publisher struct {
	broker   broker.Broker
	interval time.Duration
	enabled  atomic.Bool
	logger   *zap.Logger
}

func NewPublisher(b broker.Broker, interval time.Duration, enabled bool, logger *zap.Logger) *Publisher {
	p := &Publisher{
		broker:   b,
		interval: interval,
		logger:   logger,
	}
	p.enabled.Store(enabled)
	return p
}

func (p *Publisher) IsEnabled() bool {
	return p.enabled.Load()
}

func (p *Publisher) SetEnabled(enable bool) {
	prev := p.enabled.Swap(enable)
	if prev == enable {
		return
	}
	if enable {
		p.logger.Info("Publisher started", zap.String("trigger", "dashboard_live_toggle"), zap.Duration("interval", p.interval))
	} else {
		p.logger.Info("Publisher stopped", zap.String("trigger", "dashboard_live_toggle"))
	}
}

func (p *Publisher) StartControlListener(ctx context.Context, natsURL string) {
	ctrlConn, err := nats.Connect(natsURL, nats.MaxReconnects(-1))
	if err != nil {
		p.logger.Warn("Failed to connect control listener to NATS", zap.Error(err))
		return
	}

	_, _ = ctrlConn.Subscribe("control.publisher", func(msg *nats.Msg) {
		var cmd struct {
			PublishEnabled bool `json:"publish_enabled"`
		}
		if err := json.Unmarshal(msg.Data, &cmd); err == nil {
			p.SetEnabled(cmd.PublishEnabled)
		}
	})

	go func() {
		<-ctx.Done()
		ctrlConn.Close()
	}()
}

func (p *Publisher) Start(ctx context.Context) {
	if p.enabled.Load() {
		p.logger.Info("Publisher started", zap.Duration("interval", p.interval))
	} else {
		p.logger.Info("Publisher is disabled via configuration (PUBLISH_ENABLED=false)")
	}
	defer p.logger.Info("Publisher stopped")

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.enabled.Load() {
				p.publishNextSimulatedEvent(ctx)
			}
		}
	}
}

func (p *Publisher) publishNextSimulatedEvent(ctx context.Context) {
	event := p.generateValidEvent()
	payload, err := json.Marshal(event)
	if err != nil {
		p.logger.Error("Failed to marshal deposit event", zap.Error(err))
		return
	}

	const maxAttempts = 3
	backoff := 200 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pubCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, pubErr := p.broker.PublishDeposit(pubCtx, event.EventID, payload)
		cancel()

		if pubErr == nil {
			p.logger.Info("Deposit event published successfully",
				zap.String("event_id", event.EventID),
				zap.Int("user_id", event.UserID),
				zap.String("currency", event.Currency),
				zap.String("amount", event.Amount.String()),
				zap.Uint64("sequence", ack.Sequence),
				zap.Bool("duplicate", ack.Duplicate),
			)
			return
		}

		p.logger.Warn("Temporary failure publishing deposit event; retrying with same event_id",
			zap.String("event_id", event.EventID),
			zap.Int("attempt", attempt),
			zap.Error(pubErr),
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	p.logger.Error("Failed to publish deposit event after max retries",
		zap.String("event_id", event.EventID),
	)
}

func (p *Publisher) generateValidEvent() domain.DepositEvent {
	currencies := []string{domain.CurrencyUSDT, domain.CurrencyBTC, domain.CurrencyIRT}
	currency := currencies[rand.Intn(len(currencies))]
	userID := rand.Intn(domain.MaxUserID-domain.MinUserID+1) + domain.MinUserID

	var amount decimal.Decimal
	switch currency {
	case domain.CurrencyUSDT:
		amount = decimal.NewFromInt(int64(rand.Intn(500) + 10)).Add(decimal.RequireFromString("0.50"))
	case domain.CurrencyBTC:
		sats := rand.Intn(50) + 1
		amount = decimal.NewFromInt(int64(sats)).Div(decimal.NewFromInt(1000))
	case domain.CurrencyIRT:
		amount = decimal.NewFromInt(int64(rand.Intn(10)+1) * 1_000_000)
	}

	return domain.DepositEvent{
		EventID:   uuid.NewString(),
		UserID:    userID,
		Currency:  currency,
		Amount:    amount,
		CreatedAt: time.Now().UTC(),
	}
}
