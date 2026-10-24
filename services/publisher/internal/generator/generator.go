package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/pkg/broker"
	"github.com/alihojaty/reliable-deposit-processor/pkg/domain"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Publisher struct {
	broker   broker.Broker
	interval time.Duration
	enabled  atomic.Bool
	logger   *zap.Logger
	seq      atomic.Uint64
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
	count := p.seq.Add(1)

	if count%10 == 0 {
		badID := fmt.Sprintf("evt-invalid-%d-%s", count, uuid.NewString()[:8])
		badPayload := fmt.Sprintf(
			`{"event_id":%q,"user_id":5,"currency":"DOGE","amount":"50.00","created_at":%q}`,
			badID, time.Now().UTC().Format(time.RFC3339),
		)
		if _, err := p.broker.PublishDeposit(ctx, badID, []byte(badPayload)); err != nil {
			if ctx.Err() == nil {
				p.logger.Error("Failed to publish simulated invalid event", zap.Error(err))
			}
		}
		return
	}

	event := p.generateValidEvent()
	payload, err := json.Marshal(event)
	if err != nil {
		p.logger.Error("Failed to marshal deposit event", zap.Error(err))
		return
	}

	if _, err := p.broker.PublishDeposit(ctx, event.EventID, payload); err != nil {
		if ctx.Err() == nil {
			p.logger.Error("Failed to publish deposit event",
				zap.String("event_id", event.EventID),
				zap.Error(err),
			)
		}
		return
	}
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
