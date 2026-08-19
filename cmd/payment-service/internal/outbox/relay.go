package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"qpayflow/cmd/payment-service/internal/payment"
	"qpayflow/pkg/kafka"
)

type RelayWorker struct {
	repo     payment.Repository
	producer *kafka.Producer
	interval time.Duration
	batch    int
}

func NewRelayWorker(repo payment.Repository, producer *kafka.Producer, interval time.Duration, batch int) *RelayWorker {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	if batch <= 0 {
		batch = 50
	}
	return &RelayWorker{
		repo:     repo,
		producer: producer,
		interval: interval,
		batch:    batch,
	}
}

// Start runs the polling loop until the context is canceled.
func (w *RelayWorker) Start(ctx context.Context) {
	slog.Info("starting transactional outbox relay worker", "interval", w.interval, "batch", w.batch)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox relay worker stopped (context canceled)")
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("outbox relay batch execution error", "error", err)
			}
		}
	}
}

func (w *RelayWorker) processBatch(ctx context.Context) error {
	events, err := w.repo.GetPendingOutboxEvents(ctx, w.batch)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	for _, evt := range events {
		var created payment.PaymentCreatedEvent
		partitionKey := evt.AggregateID
		if err := json.Unmarshal(evt.Payload, &created); err == nil && created.SourceAccountID != "" {
			partitionKey = created.SourceAccountID
		}

		if err := w.producer.Publish(ctx, partitionKey, evt.Payload); err != nil {
			slog.Error("failed to publish outbox event to kafka", "event_id", evt.ID, "error", err)
			continue
		}

		if err := w.repo.MarkOutboxEventProcessed(ctx, evt.ID); err != nil {
			slog.Error("failed to mark outbox event processed", "event_id", evt.ID, "error", err)
			continue
		}

		slog.Debug("outbox event published and marked processed", "event_id", evt.ID, "type", evt.EventType)
	}

	return nil
}
