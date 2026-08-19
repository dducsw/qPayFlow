package database

import (
	"database/sql"
	"fmt"

	"qpayflow/cmd/payment-service/internal/config"
	"qpayflow/pkg/database"
)

func Init(cfg config.PostgresConfig) (*sql.DB, error) {
	pkgCfg := database.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
		DBName:   cfg.DBName,
		SSLMode:  cfg.SSLMode,
	}

	db, err := database.NewPostgresDB(pkgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres: %w", err)
	}

	return db, nil
}
