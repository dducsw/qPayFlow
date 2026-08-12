package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dducsw/qPayFlow/pkg/logger"
)

func main() {
	log := logger.Init("info")
	log.Info("Starting Payment Service...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Service lifecycle simulated
	<-ctx.Done()
	log.Info("Shutting down Payment Service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = shutdownCtx
	fmt.Println("Payment Service stopped clean")
}
