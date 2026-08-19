package payment

import (
	"encoding/json"
	"time"
)

type PaymentStatus string

const (
	StatusPending    PaymentStatus = "PENDING"
	StatusProcessing PaymentStatus = "PROCESSING"
	StatusSuccess    PaymentStatus = "SUCCESS"
	StatusFailed     PaymentStatus = "FAILED"
)

type Payment struct {
	ID              string        `json:"id"`
	IdempotencyKey  string        `json:"idempotency_key"`
	SourceAccountID string        `json:"source_account_id"`
	TargetAccountID string        `json:"target_account_id"`
	Amount          float64       `json:"amount"`
	Currency        string        `json:"currency"`
	Status          PaymentStatus `json:"status"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
}

type OutboxEvent struct {
	ID            string          `json:"id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"` // PENDING, PROCESSED, FAILED
	CreatedAt     time.Time       `json:"created_at"`
}

type PaymentCreatedEvent struct {
	ID              string  `json:"id"`
	IdempotencyKey  string  `json:"idempotency_key"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Status          string  `json:"status"`
}

type FraudCheckedEvent struct {
	PaymentID string `json:"payment_id"`
	Approved  bool   `json:"approved"`
	Reason    string `json:"reason"`
}

type PaymentResultEvent struct {
	ID              string  `json:"id"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Status          string  `json:"status"`
	Reason          string  `json:"reason,omitempty"`
}
