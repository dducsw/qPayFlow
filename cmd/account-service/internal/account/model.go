package account

import (
	"time"
)

type Account struct {
	ID        string    `json:"id"`
	OwnerID   string    `json:"owner_id"`
	Balance   float64   `json:"balance"`
	Currency  string    `json:"currency"`
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type BalanceLedger struct {
	ID          string    `json:"id"`
	AccountID   string    `json:"account_id"`
	Amount      float64   `json:"amount"`
	Type        string    `json:"type"` // DEBIT or CREDIT
	ReferenceID string    `json:"reference_id"`
	CreatedAt   time.Time `json:"created_at"`
}
