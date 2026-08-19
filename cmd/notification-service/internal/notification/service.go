package notification

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"qpayflow/pkg/tracing"
)

type Service interface {
	SendEmail(ctx context.Context, accountID string, subject, body string) error
	SendSMS(ctx context.Context, accountID string, message string) error
	TriggerMerchantWebhook(ctx context.Context, webhookURL string, secretKey string, eventPayload interface{}) error
}

type notificationService struct {
	httpClient *http.Client
}

func NewService() Service {
	return &notificationService{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (s *notificationService) SendEmail(ctx context.Context, accountID string, subject, body string) error {
	slog.Info("MOCK: sending email notification",
		"account_id", accountID,
		"subject", subject,
		"body", body,
		"traceparent", tracing.FromContext(ctx),
	)
	return nil
}

func (s *notificationService) SendSMS(ctx context.Context, accountID string, message string) error {
	slog.Info("MOCK: sending SMS notification",
		"account_id", accountID,
		"message", message,
		"traceparent", tracing.FromContext(ctx),
	)
	return nil
}

func (s *notificationService) TriggerMerchantWebhook(ctx context.Context, webhookURL string, secretKey string, eventPayload interface{}) error {
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	// Compute HMAC-SHA256 Signature (Concept 10)
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write(payloadBytes)
	signature := hex.EncodeToString(h.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to build webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature-SHA256", signature)
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	tracing.InjectToHTTP(req, tracing.FromContext(ctx))

	slog.Info("sending signed HMAC-SHA256 webhook to merchant",
		"url", webhookURL,
		"signature", signature,
		"traceparent", tracing.FromContext(ctx),
	)

	return nil
}
