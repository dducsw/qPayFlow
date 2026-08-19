package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qpayflow/cmd/notification-service/internal/config"
	"qpayflow/cmd/notification-service/internal/notification"
	"qpayflow/pkg/kafka"
	"qpayflow/pkg/logger"
)

type PaymentEvent struct {
	ID              string  `json:"id"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Status          string  `json:"status"`
	Reason          string  `json:"reason,omitempty"`
}

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting Notification Service...")

	// 2. Load Configuration
	cfg := config.Load()

	// 3. Initialize Services
	notifSvc := notification.NewService()

	// 4. Initialize Kafka Consumer
	consumer := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.NotificationGroupID, cfg.Kafka.PaymentEventsTopic)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 5. Start Kafka consuming loop
	go consumer.Start(ctx, func(msgCtx context.Context, key, value []byte, headers map[string]string) error {
		var event PaymentEvent
		if err := json.Unmarshal(value, &event); err != nil {
			log.Error("failed to unmarshal payment event in notification service", "error", err)
			return nil
		}

		// Only notify for completed or failed payments
		if event.Status != "SUCCESS" && event.Status != "FAILED" {
			return nil
		}

		log.Info("dispatching notifications for payment",
			"payment_id", event.ID,
			"status", event.Status,
			"source_account", event.SourceAccountID,
		)

		accountID := event.SourceAccountID
		if accountID == "" {
			accountID = string(key)
		}

		msg := fmt.Sprintf("Transaction %s of %.2f %s finished with status: %s",
			event.ID, event.Amount, event.Currency, event.Status)
		if event.Reason != "" {
			msg += fmt.Sprintf(" (Reason: %s)", event.Reason)
		}

		// 1. Send SMS Notification (Mock)
		_ = notifSvc.SendSMS(msgCtx, accountID, msg)

		// 2. Send Email Notification (Mock)
		_ = notifSvc.SendEmail(msgCtx, accountID, "Payment Status Update", msg)

		// 3. Send Signed Webhook to Merchant (Mock)
		webhookURL := "https://merchant.example.com/webhooks/payments"
		secretKey := "merchant-shared-secret-key-1234"
		_ = notifSvc.TriggerMerchantWebhook(msgCtx, webhookURL, secretKey, event)

		return nil
	})

	// 6. Graceful Shutdown
	<-ctx.Done()
	log.Info("Shutting down Notification Service gracefully...")
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Notification Service stopped clean")
}
