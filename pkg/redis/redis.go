package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("redis lock: lock is already held")
	ErrLockNotOwned    = errors.New("redis lock: lock is not owned by this token")
)

// releaseLockLua ensures lock is only released if the caller owns the token.
const releaseLockLua = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

// Lock holds metadata for an acquired distributed lock.
type Lock struct {
	client *redis.Client
	Key    string
	Token  string
}

func NewRedisClient(addr string, password string, db int) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis failed: %w", err)
	}

	return rdb, nil
}

// AcquireLock attempts to acquire a distributed lock using Redis SETNX with TTL.
func AcquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*Lock, error) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("generate lock token failed: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)

	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("setnx lock failed: %w", err)
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}

	return &Lock{
		client: rdb,
		Key:    key,
		Token:  token,
	}, nil
}

// Release safely releases the distributed lock using a Lua script.
func (l *Lock) Release(ctx context.Context) error {
	res, err := l.client.Eval(ctx, releaseLockLua, []string{l.Key}, l.Token).Result()
	if err != nil {
		return fmt.Errorf("eval release lock lua failed: %w", err)
	}

	intRes, ok := res.(int64)
	if !ok || intRes != 1 {
		return ErrLockNotOwned
	}
	return nil
}
