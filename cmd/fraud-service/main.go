package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qpayflow/cmd/fraud-service/internal/config"
	"qpayflow/cmd/fraud-service/internal/fraud"
	"qpayflow/cmd/fraud-service/internal/redis"
	"qpayflow/pkg/kafka"
	"qpayflow/pkg/logger"
)

type PaymentCreatedEvent struct {
	ID              string  `json:"id"`
	IdempotencyKey  string  `json:"idempotency_key"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Status          string  `json:"status"`
}

type FraudCheckedEvent struct {
	PaymentID string `json:"payment_id"`
	Approved  bool   `json:"approved"`
	Reason    string `json:"reason"`
}

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting Fraud Detection Service...")

	// 2. Load Configuration
	cfg := config.Load()

	// 3. Connect to Redis
	rdb, err := redis.Init(cfg.Redis)
	if err != nil {
		log.Error("failed to initialize redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("Redis connected successfully")

	// 4. Initialize Services
	fraudSvc := fraud.NewService(rdb)

	// 5. Initialize Kafka Producer & Consumer
	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.FraudEventsTopic)
	defer producer.Close()

	consumer := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.FraudGroupID, cfg.Kafka.PaymentEventsTopic)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 6. Start processing loop
	go consumer.Start(ctx, func(msgCtx context.Context, key, value []byte, headers map[string]string) error {
		var event PaymentCreatedEvent
		if err := json.Unmarshal(value, &event); err != nil {
			log.Error("failed to unmarshal payment event", "error", err)
			return nil
		}

		// Only evaluate newly created payments
		if event.Status != "" && event.Status != "PENDING" {
			return nil
		}

		accountID := event.SourceAccountID
		if accountID == "" {
			accountID = string(key)
		}

		log.Info("evaluating fraud rules for transaction",
			"payment_id", event.ID,
			"source_account", accountID,
			"amount", event.Amount,
		)

		approved, reason, err := fraudSvc.CheckPayment(msgCtx, accountID, event.Amount)
		if err != nil {
			log.Error("error evaluating fraud logic", "payment_id", event.ID, "error", err)
			return err
		}

		checkedEvent := FraudCheckedEvent{
			PaymentID: event.ID,
			Approved:  approved,
			Reason:    reason,
		}
		payload, _ := json.Marshal(checkedEvent)

		if err := producer.Publish(msgCtx, event.ID, payload); err != nil {
			log.Error("failed to publish fraud check result", "payment_id", event.ID, "error", err)
			return err
		}

		log.Info("fraud evaluation finished",
			"payment_id", event.ID,
			"approved", approved,
			"reason", reason,
		)
		return nil
	})

	// 7. Graceful Shutdown
	<-ctx.Done()
	log.Info("Shutting down Fraud Detection Service gracefully...")
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Fraud Detection Service stopped clean")
}
