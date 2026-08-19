package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRequestInFlight = errors.New("idempotency: request is currently in-flight")
	ErrKeyConflict     = errors.New("idempotency: key reused with mismatched request payload")
)

type Status string

const (
	StatusInFlight  Status = "IN_FLIGHT"
	StatusCompleted Status = "COMPLETED"
)

type Record struct {
	Key          string          `json:"key"`
	Status       Status          `json:"status"`
	PayloadHash  string          `json:"payload_hash,omitempty"`
	ResponseData json.RawMessage `json:"response_data,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type Manager struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewManager(rdb *redis.Client, ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Manager{rdb: rdb, ttl: ttl}
}

func (m *Manager) keyPrefix(key string) string {
	return fmt.Sprintf("idempotency:%s", key)
}

// Start checks and reserves an idempotency key. Returns existing Record if already processed.
func (m *Manager) Start(ctx context.Context, key string, payloadHash string) (*Record, error) {
	if key == "" {
		return nil, errors.New("idempotency key cannot be empty")
	}

	redisKey := m.keyPrefix(key)
	rec := Record{
		Key:         key,
		Status:      StatusInFlight,
		PayloadHash: payloadHash,
		CreatedAt:   time.Now(),
	}

	data, _ := json.Marshal(rec)

	// Try setting with NX (only if key does not exist)
	ok, err := m.rdb.SetNX(ctx, redisKey, data, m.ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("idempotency redis SetNX failed: %w", err)
	}

	if ok {
		// First time request
		return &rec, nil
	}

	// Key already exists, read status
	existingRaw, err := m.rdb.Get(ctx, redisKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("idempotency redis Get failed: %w", err)
	}

	var existing Record
	if err := json.Unmarshal(existingRaw, &existing); err != nil {
		return nil, fmt.Errorf("idempotency unmarshal failed: %w", err)
	}

	if payloadHash != "" && existing.PayloadHash != "" && payloadHash != existing.PayloadHash {
		return nil, ErrKeyConflict
	}

	if existing.Status == StatusInFlight {
		return nil, ErrRequestInFlight
	}

	return &existing, nil
}

// Complete marks the idempotency key as COMPLETED and caches the response.
func (m *Manager) Complete(ctx context.Context, key string, responsePayload interface{}) error {
	redisKey := m.keyPrefix(key)

	respBytes, err := json.Marshal(responsePayload)
	if err != nil {
		return fmt.Errorf("failed to marshal response payload: %w", err)
	}

	rec := Record{
		Key:          key,
		Status:       StatusCompleted,
		ResponseData: json.RawMessage(respBytes),
		CreatedAt:    time.Now(),
	}

	data, _ := json.Marshal(rec)
	if err := m.rdb.Set(ctx, redisKey, data, m.ttl).Err(); err != nil {
		return fmt.Errorf("idempotency redis set completed failed: %w", err)
	}

	return nil
}

// Release deletes the idempotency key in case of an unrecoverable early validation failure.
func (m *Manager) Release(ctx context.Context, key string) error {
	return m.rdb.Del(ctx, m.keyPrefix(key)).Err()
}
