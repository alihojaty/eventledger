package subscriber

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Subscriber struct {
	connection   *nats.Conn
	subject      string
	subscription *nats.Subscription
}

func New(
	connection *nats.Conn,
	subject string,
) (*Subscriber, error) {

	if connection == nil {
		return nil, fmt.Errorf("NATS connection is nil")
	}

	if subject == "" {
		return nil, fmt.Errorf("NATS subject is required")
	}

	return &Subscriber{
		connection: connection,
		subject:    subject,
	}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {

	zap.L().Info(
		"starting NATS subscriber",
		zap.String("subject", s.subject),
	)

	subscription, err := s.connection.Subscribe(
		s.subject,
		func(msg *nats.Msg) {

			zap.L().Info(
				"message received",
				zap.String("subject", msg.Subject),
				zap.String("payload", string(msg.Data)),
			)

		},
	)

	if err != nil {
		return fmt.Errorf(
			"failed to subscribe: %w",
			err,
		)
	}

	s.subscription = subscription

	zap.L().Info(
		"NATS subscriber ready",
	)

	// Wait for shutdown signal
	<-ctx.Done()

	zap.L().Info(
		"stopping NATS subscriber",
	)

	if err := s.subscription.Unsubscribe(); err != nil {

		zap.L().Error(
			"failed to unsubscribe",
			zap.Error(err),
		)

		return err
	}

	zap.L().Info(
		"NATS subscriber stopped",
	)

	return nil
}
