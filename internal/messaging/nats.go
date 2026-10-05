package messaging

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type NATSConfig struct {
	URL string
}
type NATSManager struct {
	connection *nats.Conn
	jetStream  nats.JetStreamContext
}

func NewNATSManager(cfg NATSConfig) (*NATSManager, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("NATS url is required")
	}
	return &NATSManager{}, nil
}

func (m *NATSManager) Connect(cfg NATSConfig) error {
	zap.L().Info(
		"Connecting to NATS",
		zap.String("url", cfg.URL),
	)

	conn, err := nats.Connect(cfg.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	m.connection = conn
	m.jetStream = js

	zap.L().Info("Connected to NATS")

	return nil
}

func (m *NATSManager) Connection() *nats.Conn {
	return m.connection
}

func (m *NATSManager) JetStream() nats.JetStreamContext {
	return m.jetStream
}

func (m *NATSManager) Close() error {
	if m.connection == nil {
		return nil
	}

	zap.L().Info("Closing NATS connection")

	m.connection.Close()

	zap.L().Info("NATS connection closed")

	return nil
}
