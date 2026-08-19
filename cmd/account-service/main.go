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

	"qpayflow/cmd/account-service/internal/account"
	"qpayflow/cmd/account-service/internal/config"
	"qpayflow/cmd/account-service/internal/database"
	"qpayflow/cmd/account-service/internal/redis"
	"qpayflow/pkg/logger"
)

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting Account & Ledger Service...")

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
	accountRepo := account.NewRepository(db)
	accountSvc := account.NewService(accountRepo, rdb)
	accountHandler := account.NewHandler(accountSvc)

	// 6. Setup HTTP Router
	mux := http.NewServeMux()
	accountHandler.RegisterRoutes(mux)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	// 7. Start HTTP Server
	go func() {
		log.Info("Account Service HTTP listening on", "addr", serverAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed to listen", "error", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	log.Info("Shutting down Account Service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to gracefully shutdown server", "error", err)
	}

	fmt.Println("Account Service stopped clean")
}
