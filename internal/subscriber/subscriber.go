package subscriber

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Subscriber struct {
	jetStream nats.JetStreamContext
	stream    string
	consumer  string
	subject   string
}

func New(
	js nats.JetStreamContext,
	stream string,
	consumer string,
	subject string,
) (*Subscriber, error) {

	if js == nil {
		return nil, fmt.Errorf("jetstream context is nil")
	}

	if stream == "" {
		return nil, fmt.Errorf("stream name is required")
	}

	if consumer == "" {
		return nil, fmt.Errorf("consumer name is required")
	}

	if subject == "" {
		return nil, fmt.Errorf("subject is required")
	}

	return &Subscriber{
		jetStream: js,
		stream:    stream,
		consumer:  consumer,
		subject:   subject,
	}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {

	zap.L().Info(
		"starting jetstream subscriber",
		zap.String("stream", s.stream),
		zap.String("consumer", s.consumer),
	)

	sub, err := s.jetStream.PullSubscribe(
		s.subject,
		s.consumer,
		nats.Bind(s.stream, s.consumer),
	)

	if err != nil {
		return fmt.Errorf(
			"failed to create pull subscription: %w",
			err,
		)
	}

	zap.L().Info(
		"jetstream subscriber ready",
	)

	for {

		select {

		case <-ctx.Done():

			zap.L().Info(
				"stopping jetstream subscriber",
			)

			return nil

		default:

			msgs, err := sub.Fetch(
				1,
				nats.MaxWait(time.Second),
			)

			if err != nil {

				if err == nats.ErrTimeout {
					continue
				}

				return fmt.Errorf(
					"failed to fetch messages: %w",
					err,
				)
			}

			for _, msg := range msgs {

				zap.L().Info(
					"message received",
					zap.String(
						"subject",
						msg.Subject,
					),
					zap.String(
						"payload",
						string(msg.Data),
					),
				)

				if err := msg.Ack(); err != nil {

					zap.L().Error(
						"failed to acknowledge message",
						zap.Error(err),
					)

					continue
				}

				zap.L().Info(
					"message acknowledged",
				)
			}
		}
	}
}
