package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// renewScript safely extends lease TTL if token matches.
const renewScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
else
    return 0
end
`

// releaseScript safely releases lease if token matches.
const releaseScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

type Elector struct {
	rdb        *redis.Client
	lockKey    string
	instanceID string
	ttl        time.Duration
	isLeader   bool
}

func NewElector(rdb *redis.Client, lockKey string, ttl time.Duration) *Elector {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	instanceID := "node_" + hex.EncodeToString(b)

	if ttl <= 0 {
		ttl = 10 * time.Second
	}

	return &Elector{
		rdb:        rdb,
		lockKey:    lockKey,
		instanceID: instanceID,
		ttl:        ttl,
	}
}

func (e *Elector) IsLeader() bool {
	return e.isLeader
}

func (e *Elector) InstanceID() string {
	return e.instanceID
}

// Run starts the leader election loop with periodic lease renewal.
func (e *Elector) Run(ctx context.Context, onElected func(ctx context.Context)) {
	slog.Info("starting leader election candidate", "instance_id", e.instanceID, "key", e.lockKey)
	heartbeatInterval := e.ttl / 3
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	var leaderCancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			if leaderCancel != nil {
				leaderCancel()
			}
			e.stepDown(context.Background())
			return
		case <-ticker.C:
			if e.isLeader {
				// Renew heartbeat lease
				ttlSeconds := int64(e.ttl.Seconds())
				res, err := e.rdb.Eval(ctx, renewScript, []string{e.lockKey}, e.instanceID, ttlSeconds).Result()
				if err != nil || res.(int64) != 1 {
					slog.Warn("failed to renew leader lease, stepping down", "error", err)
					e.isLeader = false
					if leaderCancel != nil {
						leaderCancel()
						leaderCancel = nil
					}
				}
			} else {
				// Try to acquire leadership
				ok, err := e.rdb.SetNX(ctx, e.lockKey, e.instanceID, e.ttl).Result()
				if err == nil && ok {
					slog.Info(">>> ELECTED AS CLUSTER LEADER <<<", "instance_id", e.instanceID)
					e.isLeader = true
					var leaderCtx context.Context
					leaderCtx, leaderCancel = context.WithCancel(ctx)
					go onElected(leaderCtx)
				}
			}
		}
	}
}

func (e *Elector) stepDown(ctx context.Context) {
	if e.isLeader {
		_, _ = e.rdb.Eval(ctx, releaseScript, []string{e.lockKey}, e.instanceID).Result()
		e.isLeader = false
		slog.Info("leader stepped down cleanly", "instance_id", e.instanceID)
	}
}
