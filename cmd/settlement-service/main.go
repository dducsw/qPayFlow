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

	"qpayflow/cmd/settlement-service/internal/config"
	"qpayflow/cmd/settlement-service/internal/leader"
	"qpayflow/cmd/settlement-service/internal/reconciliation"
	"qpayflow/pkg/database"
	"qpayflow/pkg/logger"
	pkgredis "qpayflow/pkg/redis"
)

func main() {
	// 1. Initialize Logger
	log := logger.Init("debug")
	log.Info("Starting Settlement & Reconciliation Service...")

	// 2. Load Configuration
	cfg := config.Load()

	// 3. Connect to Database (PostgreSQL)
	db, err := database.NewPostgresDB(database.Config{
		Host:     cfg.Postgres.Host,
		Port:     cfg.Postgres.Port,
		User:     cfg.Postgres.User,
		Password: cfg.Postgres.Password,
		DBName:   cfg.Postgres.DBName,
		SSLMode:  cfg.Postgres.SSLMode,
	})
	if err != nil {
		log.Error("failed to initialize postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	log.Info("Postgres connected successfully")

	// 4. Connect to Redis (used for Leader Election)
	rdb, err := pkgredis.NewRedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Error("failed to initialize redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("Redis connected successfully")

	// 5. Initialize Reconciliation Job & Leader Elector
	reconJob := reconciliation.NewReconciliationJob(db, 30*time.Second)
	elector := leader.NewElector(rdb, "leader:settlement:lock", 10*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 6. Run Leader Election in background
	go elector.Run(ctx, func(leaderCtx context.Context) {
		log.Info("node became leader, starting periodic settlement audits")
		reconJob.Start(leaderCtx)
	})

	// 7. Setup HTTP Router for Healthcheck
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		status := "FOLLOWER"
		if elector.IsLeader() {
			status = "LEADER"
		}
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"UP","service":"settlement-service","role":"%s","instance_id":"%s"}`, status, elector.InstanceID())))
	})

	mux.HandleFunc("POST /reconcile/now", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		reconJob.RunAudit(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"SUCCESS","message":"Reconciliation audit executed"}`))
	})

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	go func() {
		log.Info("Settlement Service HTTP listening on", "addr", serverAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed to listen", "error", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown
	<-ctx.Done()
	log.Info("Shutting down Settlement Service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to gracefully shutdown server", "error", err)
	}

	fmt.Println("Settlement Service stopped clean")
}
