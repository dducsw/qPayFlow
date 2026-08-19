package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qpayflow/cmd/payment-service/internal/config"
	"qpayflow/cmd/payment-service/internal/database"
	"qpayflow/cmd/payment-service/internal/outbox"
	"qpayflow/cmd/payment-service/internal/payment"
	"qpayflow/cmd/payment-service/internal/redis"
	"qpayflow/cmd/payment-service/internal/saga"
	"qpayflow/pkg/kafka"
	"qpayflow/pkg/logger"
)

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting Payment Service...")

	// 2. Load Configuration
	cfg := config.Load()

	// 3. Connect to Database (PostgreSQL)
	db, err := database.Init(cfg.Postgres)
	if err != nil {
		log.Error("failed to initialize postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	log.Info("Postgres connected successfully")

	// 4. Connect to Redis
	rdb, err := redis.Init(cfg.Redis)
	if err != nil {
		log.Error("failed to initialize redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("Redis connected successfully")

	// 5. Initialize Components
	paymentRepo := payment.NewRepository(db)
	paymentSvc := payment.NewService(paymentRepo, rdb)
	paymentHandler := payment.NewHandler(paymentSvc)

	// 6. Setup HTTP Router
	mux := http.NewServeMux()
	paymentHandler.RegisterRoutes(mux)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	// 7. Start HTTP Server
	go func() {
		log.Info("Payment Service HTTP listening on", "addr", serverAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed to listen", "error", err)
			os.Exit(1)
		}
	}()

	// 8. Start Background Outbox Relay Worker
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	outboxProducer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.PaymentEventsTopic)
	defer outboxProducer.Close()

	relay := outbox.NewRelayWorker(paymentRepo, outboxProducer, 50*time.Millisecond, 50)
	go relay.Start(ctx)

	// 9. Start Saga Consumer for Fraud Results
	sagaConsumer := saga.NewConsumer(
		cfg.Kafka.Brokers,
		"payment-saga-group",
		cfg.Kafka.FraudEventsTopic,
		cfg.Kafka.PaymentEventsTopic,
		paymentSvc,
	)
	defer sagaConsumer.Close()
	go sagaConsumer.Start(ctx)

	// 10. Graceful Shutdown
	<-ctx.Done()
	log.Info("Shutting down Payment Service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to gracefully shutdown server", "error", err)
	}

	fmt.Println("Payment Service stopped clean")
}
