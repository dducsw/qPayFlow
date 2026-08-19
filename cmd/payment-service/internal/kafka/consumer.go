package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID, topic string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          topic,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			CommitInterval: time.Second,
		}),
	}
}

func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, key, value []byte) error) {
	slog.Info("starting kafka consumer", "topic", c.reader.Config().Topic, "group_id", c.reader.Config().GroupID)
	
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("kafka consumer stopped (context canceled)")
				return
			}
			slog.Error("failed to read kafka message", "error", err)
			continue
		}

		slog.Debug("received message from kafka", "key", string(msg.Key), "offset", msg.Offset)

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			slog.Error("failed to process kafka message", "key", string(msg.Key), "error", err)
			// In production, we'd route to a DLQ here
			continue
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
