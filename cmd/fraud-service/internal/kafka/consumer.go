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
			MinBytes:       10e3,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
	}
}

func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, key, value []byte) error) {
	slog.Info("starting fraud kafka consumer", "topic", c.reader.Config().Topic, "group_id", c.reader.Config().GroupID)
	
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("fraud kafka consumer stopped (context canceled)")
				return
			}
			slog.Error("failed to read fraud messages from kafka", "error", err)
			continue
		}

		slog.Debug("fraud consumer received event", "key", string(msg.Key), "offset", msg.Offset)

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			slog.Error("fraud consumer failed to process message", "key", string(msg.Key), "error", err)
			continue
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
