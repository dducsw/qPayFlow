package saga

import (
	"context"
	"encoding/json"
	"log/slog"

	"qpayflow/cmd/payment-service/internal/payment"
	"qpayflow/pkg/kafka"
)

type Consumer struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	service  payment.Service
}

func NewConsumer(brokers []string, groupID, fraudTopic, paymentTopic string, svc payment.Service) *Consumer {
	return &Consumer{
		consumer: kafka.NewConsumer(brokers, groupID, fraudTopic),
		producer: kafka.NewProducer(brokers, paymentTopic),
		service:  svc,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	slog.Info("starting payment saga consumer for fraud results")
	c.consumer.Start(ctx, func(msgCtx context.Context, key, value []byte, headers map[string]string) error {
		var fraudResult payment.FraudCheckedEvent
		if err := json.Unmarshal(value, &fraudResult); err != nil {
			slog.Error("failed to unmarshal fraud checked event", "error", err)
			return nil
		}

		if fraudResult.PaymentID == "" {
			return nil
		}

		slog.Info("saga processing fraud result for payment",
			"payment_id", fraudResult.PaymentID,
			"approved", fraudResult.Approved,
			"reason", fraudResult.Reason,
		)

		settledPayment, err := c.service.ProcessFraudResult(msgCtx, fraudResult.PaymentID, fraudResult.Approved, fraudResult.Reason)
		if err != nil {
			slog.Error("saga failed to process fraud result", "payment_id", fraudResult.PaymentID, "error", err)
			return err
		}

		// Publish PaymentCompleted / PaymentFailed to payment-events for Notification Service
		resultEvent := payment.PaymentResultEvent{
			ID:              settledPayment.ID,
			SourceAccountID: settledPayment.SourceAccountID,
			TargetAccountID: settledPayment.TargetAccountID,
			Amount:          settledPayment.Amount,
			Currency:        settledPayment.Currency,
			Status:          string(settledPayment.Status),
			Reason:          fraudResult.Reason,
		}

		eventPayload, _ := json.Marshal(resultEvent)
		_ = c.producer.Publish(msgCtx, settledPayment.SourceAccountID, eventPayload)

		return nil
	})
}

func (c *Consumer) Close() error {
	_ = c.producer.Close()
	return c.consumer.Close()
}
