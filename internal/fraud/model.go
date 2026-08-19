package fraud

import (
	"time"
)

type FraudResult struct {
	PaymentID  string    `json:"payment_id"`
	IsApproved bool      `json:"is_approved"`
	Reason     string    `json:"reason"`
	CheckedAt  time.Time `json:"checked_at"`
}
