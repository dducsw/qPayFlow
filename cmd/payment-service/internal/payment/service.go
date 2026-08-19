package payment

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"qpayflow/pkg/idempotency"
	"qpayflow/pkg/resilience"
	"qpayflow/pkg/tracing"

	"github.com/redis/go-redis/v9"
)

type CreatePaymentRequest struct {
	IdempotencyKey  string  `json:"idempotency_key"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
}

type Service interface {
	CreatePayment(ctx context.Context, req *CreatePaymentRequest) (*Payment, error)
	GetPayment(ctx context.Context, id string) (*Payment, error)
	ProcessFraudResult(ctx context.Context, paymentID string, approved bool, reason string) (*Payment, error)
}

type paymentService struct {
	repo              Repository
	idempotencyMgr    *idempotency.Manager
	httpClient        *http.Client
	circuitBreaker    *resilience.CircuitBreaker
	accountServiceURL string
}

func NewService(repo Repository, rdb *redis.Client) Service {
	accURL := os.Getenv("ACCOUNT_SERVICE_URL")
	if accURL == "" {
		accURL = "http://account-service:8002"
	}

	return &paymentService{
		repo:              repo,
		idempotencyMgr:    idempotency.NewManager(rdb, 24*time.Hour),
		accountServiceURL: accURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		circuitBreaker: resilience.NewCircuitBreaker(5, 2, 10*time.Second),
	}
}

func (s *paymentService) CreatePayment(ctx context.Context, req *CreatePaymentRequest) (*Payment, error) {
	if req.IdempotencyKey == "" {
		return nil, errors.New("idempotency key is required")
	}
	if req.SourceAccountID == "" || req.TargetAccountID == "" {
		return nil, errors.New("source and target accounts are required")
	}
	if req.SourceAccountID == req.TargetAccountID {
		return nil, errors.New("cannot transfer to the same account")
	}
	if req.Amount <= 0 {
		return nil, errors.New("amount must be greater than zero")
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}

	payloadBytes, _ := json.Marshal(req)
	h := sha256.Sum256(payloadBytes)
	payloadHash := hex.EncodeToString(h[:])

	// 1. Fast-path Idempotency check via Redis
	idemRecord, err := s.idempotencyMgr.Start(ctx, req.IdempotencyKey, payloadHash)
	if err != nil {
		if errors.Is(err, idempotency.ErrRequestInFlight) {
			slog.Warn("duplicate in-flight payment request rejected", "key", req.IdempotencyKey)
			return nil, errors.New("payment request is already being processed")
		}
		if errors.Is(err, idempotency.ErrKeyConflict) {
			return nil, errors.New("idempotency key reused with different payload")
		}
		slog.Warn("idempotency redis check error, falling back to db", "error", err)
	} else if idemRecord != nil && idemRecord.Status == idempotency.StatusCompleted && len(idemRecord.ResponseData) > 0 {
		var cachedPayment Payment
		if err := json.Unmarshal(idemRecord.ResponseData, &cachedPayment); err == nil {
			slog.Info("returning cached payment from idempotency store", "key", req.IdempotencyKey, "payment_id", cachedPayment.ID)
			return &cachedPayment, nil
		}
	}

	// 2. Fallback / Second-layer DB check
	existingPayment, err := s.repo.GetPaymentByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("database idempotency check failed: %w", err)
	}

	if existingPayment != nil {
		if existingPayment.SourceAccountID != req.SourceAccountID ||
			existingPayment.TargetAccountID != req.TargetAccountID ||
			existingPayment.Amount != req.Amount ||
			existingPayment.Currency != req.Currency {
			return nil, errors.New("idempotency key reuse with different parameters")
		}
		_ = s.idempotencyMgr.Complete(ctx, req.IdempotencyKey, existingPayment)
		return existingPayment, nil
	}

	// 3. Transactional Outbox: Insert Payment (PENDING) and OutboxEvent atomically
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		_ = s.idempotencyMgr.Release(ctx, req.IdempotencyKey)
		return nil, fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer tx.Rollback()

	paymentID := "pay_" + generateUUID()

	p := &Payment{
		ID:              paymentID,
		IdempotencyKey:  req.IdempotencyKey,
		SourceAccountID: req.SourceAccountID,
		TargetAccountID: req.TargetAccountID,
		Amount:          req.Amount,
		Currency:        req.Currency,
		Status:          StatusPending,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	if err := s.repo.CreatePayment(ctx, tx, p); err != nil {
		_ = s.idempotencyMgr.Release(ctx, req.IdempotencyKey)
		return nil, fmt.Errorf("failed to create payment record: %w", err)
	}

	eventPayload, _ := json.Marshal(PaymentCreatedEvent{
		ID:              p.ID,
		IdempotencyKey:  p.IdempotencyKey,
		SourceAccountID: p.SourceAccountID,
		TargetAccountID: p.TargetAccountID,
		Amount:          p.Amount,
		Currency:        p.Currency,
		Status:          string(p.Status),
	})

	outboxEvent := &OutboxEvent{
		ID:            "evt_" + generateUUID(),
		AggregateType: "Payment",
		AggregateID:   p.ID,
		EventType:     "PaymentCreated",
		Payload:       json.RawMessage(eventPayload),
		Status:        "PENDING",
		CreatedAt:     time.Now(),
	}

	if err := s.repo.CreateOutboxEvent(ctx, tx, outboxEvent); err != nil {
		_ = s.idempotencyMgr.Release(ctx, req.IdempotencyKey)
		return nil, fmt.Errorf("failed to create outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		_ = s.idempotencyMgr.Release(ctx, req.IdempotencyKey)
		return nil, fmt.Errorf("failed to commit payment transaction: %w", err)
	}

	_ = s.idempotencyMgr.Complete(ctx, req.IdempotencyKey, p)
	slog.Info("payment initiated successfully", "payment_id", p.ID, "status", p.Status)

	return p, nil
}

func (s *paymentService) GetPayment(ctx context.Context, id string) (*Payment, error) {
	return s.repo.GetPaymentByID(ctx, id)
}

func (s *paymentService) ProcessFraudResult(ctx context.Context, paymentID string, approved bool, reason string) (*Payment, error) {
	p, err := s.repo.GetPaymentByID(ctx, paymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to find payment %s: %w", paymentID, err)
	}
	if p == nil {
		return nil, fmt.Errorf("payment %s not found", paymentID)
	}

	if p.Status != StatusPending {
		slog.Info("payment is not in pending state, skipping fraud step", "payment_id", paymentID, "status", p.Status)
		return p, nil
	}

	// 1. If Fraud check rejected payment -> mark FAILED
	if !approved {
		slog.Warn("payment rejected by fraud evaluation", "payment_id", paymentID, "reason", reason)
		if err := s.repo.UpdatePaymentStatus(ctx, nil, paymentID, StatusFailed); err != nil {
			return nil, err
		}
		p.Status = StatusFailed
		_ = s.idempotencyMgr.Complete(ctx, p.IdempotencyKey, p)
		return p, nil
	}

	// 2. If Fraud check approved -> call Account Service to transfer funds with Circuit Breaker
	transferReqBody, _ := json.Marshal(map[string]interface{}{
		"transaction_id":  p.ID,
		"from_account_id": p.SourceAccountID,
		"to_account_id":   p.TargetAccountID,
		"amount":          p.Amount,
		"currency":        p.Currency,
		"description":     fmt.Sprintf("Payment %s", p.ID),
	})

	var transferErr error
	cbErr := s.circuitBreaker.Execute(func() error {
		url := fmt.Sprintf("%s/accounts/transfer", s.accountServiceURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(transferReqBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		tracing.InjectToHTTP(req, tracing.FromContext(ctx))

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("transfer failed with status: %d", resp.StatusCode)
		}
		return nil
	})

	if cbErr != nil {
		transferErr = cbErr
	}

	// 3. Finalize Payment Status
	finalStatus := StatusSuccess
	if transferErr != nil {
		slog.Error("account balance transfer failed during saga step", "payment_id", paymentID, "error", transferErr)
		finalStatus = StatusFailed
	}

	if err := s.repo.UpdatePaymentStatus(ctx, nil, paymentID, finalStatus); err != nil {
		return nil, fmt.Errorf("failed to update payment final status: %w", err)
	}

	p.Status = finalStatus
	_ = s.idempotencyMgr.Complete(ctx, p.IdempotencyKey, p)
	slog.Info("payment settled via saga flow", "payment_id", p.ID, "status", p.Status)

	return p, nil
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
