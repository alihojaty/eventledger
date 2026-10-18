package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alihojaty/reliable-deposit-processor/internal/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	StreamDeposits       = "DEPOSITS"
	SubjectDepositEvents = "deposits.events"

	StreamDepositsDLQ  = "DEPOSITS_DLQ"
	SubjectDepositsDLQ = "deposits.dlq"

	ConsumerDurableName = "deposits_processor"

	DeduplicationWindow = 10 * time.Minute
	DefaultAckWait      = 3 * time.Second
)

type Broker interface {
	PublishDeposit(ctx context.Context, eventID string, payload []byte) (*jetstream.PubAck, error)
	PublishDepositAsync(eventID string, payload []byte) (jetstream.PubAckFuture, error)
	PublishAsyncComplete() <-chan struct{}
	PublishDLQ(ctx context.Context, dlqMsg domain.DLQMessage) error
	CreateOrUpdateDepositConsumer(ctx context.Context, maxAckPending int) (jetstream.Consumer, error)
	CreateOrUpdateDepositConsumerWithAckWait(ctx context.Context, maxAckPending int, ackWait time.Duration) (jetstream.Consumer, error)
	Close()
}

type NATSBroker struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	logger *zap.Logger
}

func NewNATSBroker(ctx context.Context, natsURL string, logger *zap.Logger) (*NATSBroker, error) {
	b := &NATSBroker{
		logger: logger,
	}

	nc, err := nats.Connect(
		natsURL,
		nats.Name("reliable-deposit-processor"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("Temporary failure",
				zap.String("component", "nats_broker"),
				zap.String("event", "disconnected from NATS"),
				zap.Error(err),
			)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("Reconnected to Message Broker", zap.String("url", nc.ConnectedUrl()))
			reconnCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := b.setupStreams(reconnCtx); err != nil {
				logger.Error("Failed to verify streams after NATS reconnect", zap.Error(err))
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(65536))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream context: %w", err)
	}

	b.nc = nc
	b.js = js

	if err := b.setupStreams(ctx); err != nil {
		nc.Close()
		return nil, err
	}

	logger.Info("Connected to Message Broker")
	return b, nil
}

func (b *NATSBroker) setupStreams(ctx context.Context) error {
	_, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamDeposits,
		Subjects:   []string{SubjectDepositEvents},
		Storage:    jetstream.FileStorage,
		Duplicates: DeduplicationWindow,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update %s stream: %w", StreamDeposits, err)
	}

	_, err = b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamDepositsDLQ,
		Subjects: []string{SubjectDepositsDLQ},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update %s stream: %w", StreamDepositsDLQ, err)
	}

	return nil
}

func (b *NATSBroker) CreateOrUpdateDepositConsumer(ctx context.Context, maxAckPending int) (jetstream.Consumer, error) {
	return b.CreateOrUpdateDepositConsumerWithAckWait(ctx, maxAckPending, DefaultAckWait)
}

func (b *NATSBroker) CreateOrUpdateDepositConsumerWithAckWait(
	ctx context.Context,
	maxAckPending int,
	ackWait time.Duration,
) (jetstream.Consumer, error) {
	if maxAckPending <= 0 {
		maxAckPending = 100
	}
	if ackWait <= 0 {
		ackWait = DefaultAckWait
	}

	consumer, err := b.js.CreateOrUpdateConsumer(ctx, StreamDeposits, jetstream.ConsumerConfig{
		Durable:       ConsumerDurableName,
		FilterSubject: SubjectDepositEvents,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    -1,
		MaxAckPending: maxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update durable consumer %s: %w", ConsumerDurableName, err)
	}

	return consumer, nil
}

func (b *NATSBroker) PublishDeposit(ctx context.Context, eventID string, payload []byte) (*jetstream.PubAck, error) {
	var opts []jetstream.PublishOpt
	if eventID != "" {
		opts = append(opts, jetstream.WithMsgID(eventID))
	}

	ack, err := b.js.Publish(ctx, SubjectDepositEvents, payload, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to publish deposit event (id=%s): %w", eventID, err)
	}

	return ack, nil
}

func (b *NATSBroker) PublishDepositAsync(eventID string, payload []byte) (jetstream.PubAckFuture, error) {
	var opts []jetstream.PublishOpt
	if eventID != "" {
		opts = append(opts, jetstream.WithMsgID(eventID))
	}
	return b.js.PublishAsync(SubjectDepositEvents, payload, opts...)
}

func (b *NATSBroker) PublishAsyncComplete() <-chan struct{} {
	return b.js.PublishAsyncComplete()
}

func (b *NATSBroker) PublishDLQ(ctx context.Context, dlqMsg domain.DLQMessage) error {
	data, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	_, err = b.js.Publish(ctx, SubjectDepositsDLQ, data)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %w", StreamDepositsDLQ, err)
	}

	return nil
}

func (b *NATSBroker) JetStream() jetstream.JetStream {
	return b.js
}

func (b *NATSBroker) Close() {
	if b.nc != nil {
		_ = b.nc.FlushTimeout(2 * time.Second)
		b.nc.Close()
		b.logger.Info("Message Broker connection closed")
	}
}

type QueueStats struct {
	StreamTotalMsgs uint64 `json:"stream_total_msgs"`
	DLQTotalMsgs    uint64 `json:"dlq_total_msgs"`
	ConsumerPending uint64 `json:"consumer_pending"`
	AckFloorSeq     uint64 `json:"ack_floor_seq"`
	NextSequence    uint64 `json:"next_sequence"`
}

func (b *NATSBroker) GetQueueStats(ctx context.Context) (QueueStats, error) {
	var stats QueueStats

	if s, err := b.js.Stream(ctx, StreamDeposits); err == nil {
		if info, err := s.Info(ctx); err == nil {
			stats.StreamTotalMsgs = info.State.Msgs
		}
		if c, err := s.Consumer(ctx, ConsumerDurableName); err == nil {
			if cInfo, err := c.Info(ctx); err == nil {
				stats.ConsumerPending = cInfo.NumPending + uint64(cInfo.NumAckPending)
				stats.AckFloorSeq = cInfo.AckFloor.Stream
				if stats.ConsumerPending > 0 {
					stats.NextSequence = cInfo.AckFloor.Stream + 1
				}
			}
		}
	}

	if dlq, err := b.js.Stream(ctx, StreamDepositsDLQ); err == nil {
		if info, err := dlq.Info(ctx); err == nil {
			stats.DLQTotalMsgs = info.State.Msgs
		}
	}

	return stats, nil
}
