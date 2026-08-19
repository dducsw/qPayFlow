package fraud

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Service interface {
	CheckPayment(ctx context.Context, sourceAccountID string, amount float64) (bool, string, error)
}

type fraudService struct {
	redis *redis.Client
}

func NewService(rdb *redis.Client) Service {
	return &fraudService{redis: rdb}
}

func (s *fraudService) CheckPayment(ctx context.Context, sourceAccountID string, amount float64) (bool, string, error) {
	// Rule 1: Maximum Transaction Amount Limit ($10,000)
	if amount > 10000.0 {
		return false, "transaction amount exceeds maximum limit ($10,000)", nil
	}

	// Rule 2: Sliding-Window Velocity Check (Max 10 transactions per minute per account)
	key := fmt.Sprintf("fraud:velocity:%s", sourceAccountID)
	now := time.Now().UnixNano()
	oneMinuteAgo := time.Now().Add(-1 * time.Minute).UnixNano()

	pipe := s.redis.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", oneMinuteAgo))
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: now})
	pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, 2*time.Minute)

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		slog.Warn("redis error on velocity check, failing open for resilience", "error", err)
		return true, "PASSED_WITH_WARNING", nil
	}

	zcardCmd, ok := cmds[2].(*redis.IntCmd)
	if !ok {
		return true, "PASSED", nil
	}

	txCount, err := zcardCmd.Result()
	if err != nil {
		return true, "PASSED", nil
	}

	if txCount > 10 {
		slog.Warn("fraud velocity threshold exceeded", "account_id", sourceAccountID, "count", txCount)
		return false, fmt.Sprintf("velocity limit exceeded: %d transactions in 1 minute", txCount), nil
	}

	return true, "PASSED", nil
}
