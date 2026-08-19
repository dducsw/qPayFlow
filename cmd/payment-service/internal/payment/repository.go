package payment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Repository interface {
	BeginTx(ctx context.Context) (*sql.Tx, error)
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	CreatePayment(ctx context.Context, tx *sql.Tx, p *Payment) error
	GetPaymentByID(ctx context.Context, id string) (*Payment, error)
	UpdatePaymentStatus(ctx context.Context, tx *sql.Tx, id string, status PaymentStatus) error
	CreateOutboxEvent(ctx context.Context, tx *sql.Tx, event *OutboxEvent) error
	GetPendingOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkOutboxEventProcessed(ctx context.Context, id string) error
}

type postgresRepository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) Repository {
	return &postgresRepository{db: db}
}

func (r *postgresRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, nil)
}

func (r *postgresRepository) GetPaymentByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	query := `
		SELECT id, idempotency_key, source_account_id, target_account_id, amount, currency, status, created_at, updated_at
		FROM payments WHERE idempotency_key = $1
	`
	var p Payment
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.ID, &p.IdempotencyKey, &p.SourceAccountID, &p.TargetAccountID, &p.Amount, &p.Currency, &p.Status, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get payment by idempotency key: %w", err)
	}
	return &p, nil
}

func (r *postgresRepository) CreatePayment(ctx context.Context, tx *sql.Tx, p *Payment) error {
	query := `
		INSERT INTO payments (id, idempotency_key, source_account_id, target_account_id, amount, currency, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	var err error
	if tx != nil {
		_, err = tx.ExecContext(ctx, query, p.ID, p.IdempotencyKey, p.SourceAccountID, p.TargetAccountID, p.Amount, p.Currency, p.Status, p.CreatedAt, p.UpdatedAt)
	} else {
		_, err = r.db.ExecContext(ctx, query, p.ID, p.IdempotencyKey, p.SourceAccountID, p.TargetAccountID, p.Amount, p.Currency, p.Status, p.CreatedAt, p.UpdatedAt)
	}
	if err != nil {
		return fmt.Errorf("failed to insert payment: %w", err)
	}
	return nil
}

func (r *postgresRepository) GetPaymentByID(ctx context.Context, id string) (*Payment, error) {
	query := `
		SELECT id, idempotency_key, source_account_id, target_account_id, amount, currency, status, created_at, updated_at
		FROM payments WHERE id = $1
	`
	var p Payment
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&p.ID, &p.IdempotencyKey, &p.SourceAccountID, &p.TargetAccountID, &p.Amount, &p.Currency, &p.Status, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get payment by ID: %w", err)
	}
	return &p, nil
}

func (r *postgresRepository) UpdatePaymentStatus(ctx context.Context, tx *sql.Tx, id string, status PaymentStatus) error {
	query := `
		UPDATE payments
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`
	var err error
	if tx != nil {
		_, err = tx.ExecContext(ctx, query, status, id)
	} else {
		_, err = r.db.ExecContext(ctx, query, status, id)
	}
	if err != nil {
		return fmt.Errorf("failed to update payment status: %w", err)
	}
	return nil
}

func (r *postgresRepository) CreateOutboxEvent(ctx context.Context, tx *sql.Tx, e *OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	var err error
	if tx != nil {
		_, err = tx.ExecContext(ctx, query, e.ID, e.AggregateType, e.AggregateID, e.EventType, e.Payload, e.Status, e.CreatedAt)
	} else {
		_, err = r.db.ExecContext(ctx, query, e.ID, e.AggregateType, e.AggregateID, e.EventType, e.Payload, e.Status, e.CreatedAt)
	}
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	return nil
}

func (r *postgresRepository) GetPendingOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, status, created_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.Status, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}
	return events, nil
}

func (r *postgresRepository) MarkOutboxEventProcessed(ctx context.Context, id string) error {
	query := `
		UPDATE outbox_events
		SET status = 'PROCESSED'
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event processed: %w", err)
	}
	return nil
}
