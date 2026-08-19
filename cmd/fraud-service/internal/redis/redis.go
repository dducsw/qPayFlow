package redis

import (
	"fmt"

	"qpayflow/cmd/fraud-service/internal/config"
	"qpayflow/pkg/redis"
	sdk "github.com/redis/go-redis/v9"
)

func Init(cfg config.RedisConfig) (*sdk.Client, error) {
	client, err := redis.NewRedisClient(cfg.Addr, cfg.Password, cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize redis: %w", err)
	}

	return client, nil
}
