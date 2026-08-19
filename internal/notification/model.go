package notification

import (
	"time"
)

type NotificationType string

const (
	TypeEmail NotificationType = "EMAIL"
	TypeSMS   NotificationType = "SMS"
	TypePush  NotificationType = "PUSH"
)

type Notification struct {
	ID        string           `json:"id"`
	UserID    int64            `json:"user_id"`
	Type      NotificationType `json:"type"`
	Target    string           `json:"target"` // Email address or phone number
	Message   string           `json:"message"`
	SentAt    time.Time        `json:"sent_at"`
	Status    string           `json:"status"` // SUCCESS, FAILED
}
