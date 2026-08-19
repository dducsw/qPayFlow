package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"qpayflow/pkg/tracing"

	"github.com/segmentio/kafka-go"
)

// Producer wraps kafka.Writer with tracing support and key-based hashing for ordering.
type Producer struct {
	writer *kafka.Writer
	dlq    *kafka.Writer
}

func NewProducer(brokers []string, topic string, dlqTopic ...string) *Producer {
	p := &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // Hash by key (e.g. account_id) to guarantee partition ordering
			MaxAttempts:  5,
			BatchTimeout: 10 * time.Millisecond,
		},
	}

	if len(dlqTopic) > 0 && dlqTopic[0] != "" {
		p.dlq = &kafka.Writer{
			Addr:        kafka.TCP(brokers...),
			Topic:       dlqTopic[0],
			Balancer:    &kafka.LeastBytes{},
			MaxAttempts: 5,
		}
	}

	return p
}

// Publish sends a message to Kafka with traceparent header injection.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	tp := tracing.FromContext(ctx)
	headers := tracing.InjectToKafkaHeaders(nil, tp)

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
		Time:    time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write kafka message to %s: %w", p.writer.Topic, err)
	}

	slog.Debug("published kafka message", "topic", p.writer.Topic, "key", key, "traceparent", tp)
	return nil
}

// PublishDLQ sends a poisoned message to the Dead Letter Queue.
func (p *Producer) PublishDLQ(ctx context.Context, key string, value []byte, reason string) error {
	if p.dlq == nil {
		slog.Warn("DLQ writer not configured, message dropped from DLQ", "key", key, "reason", reason)
		return nil
	}

	tp := tracing.FromContext(ctx)
	headers := tracing.InjectToKafkaHeaders([]kafka.Header{
		{Key: "dlq-reason", Value: []byte(reason)},
		{Key: "dlq-timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
	}, tp)

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
		Time:    time.Now(),
	}

	return p.dlq.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	if p.dlq != nil {
		_ = p.dlq.Close()
	}
	return p.writer.Close()
}

// MessageHandler is the callback for consumed messages.
type MessageHandler func(ctx context.Context, key, value []byte, headers map[string]string) error

// Consumer wraps kafka.Reader.
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
			StartOffset:    kafka.FirstOffset,
		}),
	}
}

// Start begins the consumer loop and propagates traceparent into the handler context.
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) {
	slog.Info("starting kafka consumer loop", "topic", c.reader.Config().Topic, "group_id", c.reader.Config().GroupID)

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("kafka consumer stopped (context canceled)", "topic", c.reader.Config().Topic)
				return
			}
			slog.Error("error reading kafka message", "topic", c.reader.Config().Topic, "error", err)
			continue
		}

		// Extract traceparent from message headers
		tp := tracing.ExtractFromKafka(msg)
		msgCtx := tracing.WithTraceparent(ctx, tp)

		headersMap := make(map[string]string)
		for _, h := range msg.Headers {
			headersMap[h.Key] = string(h.Value)
		}

		if err := handler(msgCtx, msg.Key, msg.Value, headersMap); err != nil {
			slog.Error("handler failed processing kafka message", "topic", c.reader.Config().Topic, "key", string(msg.Key), "error", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
