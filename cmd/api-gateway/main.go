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

	"qpayflow/cmd/api-gateway/internal/config"
	"qpayflow/cmd/api-gateway/internal/gateway"
	"qpayflow/cmd/api-gateway/internal/redis"
	"qpayflow/pkg/logger"
)

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting API Gateway Service...")

	// 2. Load Configuration
	cfg := config.Load()

	// 3. Connect to Redis (used for Rate Limiting / Session checking)
	rdb, err := redis.Init(cfg.Redis)
	if err != nil {
		log.Error("failed to initialize redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("Redis connected successfully")

	// 4. Initialize Handlers and Middlewares
	gwHandler := gateway.NewHandler()
	mw := gateway.NewMiddleware(rdb)

	// 5. Setup Router (using Go 1.22 routing logic)
	mux := http.NewServeMux()
	
	// Add healthcheck
	mux.HandleFunc("GET /health", gwHandler.ProxyHealth)
	
	// Register proxied routes
	gwHandler.RegisterRoutes(mux, mw)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	// 6. Start HTTP Server
	go func() {
		log.Info("API Gateway listening on", "addr", serverAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed to listen", "error", err)
			os.Exit(1)
		}
	}()

	// 7. Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	log.Info("Shutting down API Gateway gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to gracefully shutdown server", "error", err)
	}

	fmt.Println("API Gateway stopped clean")
}
