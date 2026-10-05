package messaging

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type ConsumerManager struct {
	jetStream nats.JetStreamContext
}

type ConsumerConfig struct {
	StreamName    string
	DurableName   string
	FilterSubject string
	AckWait       time.Duration
}

func NewConsumerManager(
	js nats.JetStreamContext,
) (*ConsumerManager, error) {

	if js == nil {
		return nil, fmt.Errorf("jetstream context is nil")
	}

	return &ConsumerManager{
		jetStream: js,
	}, nil
}

func (cm *ConsumerManager) EnsureConsumer(
	cfg ConsumerConfig,
) error {

	zap.L().Info(
		"checking jetstream consumer",
		zap.String("stream", cfg.StreamName),
		zap.String("consumer", cfg.DurableName),
	)

	_, err := cm.jetStream.ConsumerInfo(
		cfg.StreamName,
		cfg.DurableName,
	)

	if err == nil {

		zap.L().Info(
			"consumer already exists",
			zap.String("consumer", cfg.DurableName),
		)

		return nil
	}

	zap.L().Info(
		"creating durable consumer",
		zap.String("consumer", cfg.DurableName),
	)

	_, err = cm.jetStream.AddConsumer(
		cfg.StreamName,
		&nats.ConsumerConfig{
			Durable:       cfg.DurableName,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       cfg.AckWait,
			FilterSubject: cfg.FilterSubject,
		},
	)

	if err != nil {

		return fmt.Errorf(
			"failed to create consumer: %w",
			err,
		)
	}

	zap.L().Info(
		"jetstream durable consumer created",
		zap.String("consumer", cfg.DurableName),
	)

	return nil
}
