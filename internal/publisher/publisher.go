package publisher

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Publisher struct {
	connection *nats.Conn
	subject    string
	interval   time.Duration
}

func New(
	connection *nats.Conn,
	subject string,
	interval time.Duration,
) (*Publisher, error) {

	if connection == nil {
		return nil, fmt.Errorf("NATS connection is nil")
	}

	if subject == "" {
		return nil, fmt.Errorf("NATS subject is required")
	}

	return &Publisher{
		connection: connection,
		subject:    subject,
		interval:   interval,
	}, nil
}

func (p *Publisher) Run(ctx context.Context) error {
	zap.L().Info(
		"starting NATS publisher",
		zap.String("subject", p.subject),
	)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("publisher stopped")
			return nil
		case <-ticker.C:

			message := generateMessage()

			err := p.connection.Publish(
				p.subject,
				[]byte(message),
			)

			if err != nil {
				zap.L().Error("failed to publish message", zap.Error(err))
				continue
			}

			zap.L().Info(
				"published message",
				zap.String("subject", p.subject),
				zap.String("message", message),
			)
		}
	}
	return nil
}

func generateMessage() string {
	messages := []string{
		"Hello event ledger",
		"NATS message",
		"random event",
		"golang worker",
	}

	return messages[rand.Intn(len(messages))]
}
