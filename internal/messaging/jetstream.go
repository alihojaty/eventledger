package messaging

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type JetStreamManager struct {
	jetStream nats.JetStreamContext
}

type StreamConfig struct {
	Name    string
	Subject string
	Storage nats.StorageType
}

func NewJetStreamManager(js nats.JetStreamContext) (*JetStreamManager, error) {
	if js == nil {
		return nil, fmt.Errorf("jetstream context is nil")
	}

	return &JetStreamManager{
		jetStream: js,
	}, nil
}

func (m *JetStreamManager) EnsureStream(cfg StreamConfig) error {
	zap.L().Info(
		"checking jetstream stream",
		zap.String("stream", cfg.Name),
	)

	_, err := m.jetStream.StreamInfo(cfg.Name)

	if err == nil {
		zap.L().Info(
			"stream already exists",
			zap.String("stream", cfg.Name),
		)
		return nil
	}

	zap.L().Info(
		"creating jetstream stream",
		zap.String("stream", cfg.Name),
	)

	_, err = m.jetStream.AddStream(&nats.StreamConfig{
		Name:     cfg.Name,
		Subjects: []string{cfg.Subject},
		Storage:  cfg.Storage,
	})

	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	zap.L().Info(
		"jetstream stream created",
		zap.String("stream", cfg.Name),
	)

	return nil
}
