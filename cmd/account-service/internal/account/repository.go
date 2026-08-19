package account

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Repository interface {
	BeginTx(ctx context.Context) (*sql.Tx, error)
	GetAccountByID(ctx context.Context, id string) (*Account, error)
	GetAccountByIDWithLock(ctx context.Context, tx *sql.Tx, id string) (*Account, error)
	UpdateAccountBalance(ctx context.Context, tx *sql.Tx, id string, newBalance float64, oldVersion int) error
	CreateLedgerEntry(ctx context.Context, tx *sql.Tx, entry *BalanceLedger) error
	GetLedgerSum(ctx context.Context, accountID string) (float64, error)
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

func (r *postgresRepository) GetAccountByID(ctx context.Context, id string) (*Account, error) {
	query := `
		SELECT id, owner_id, balance, currency, version, created_at, updated_at
		FROM accounts WHERE id = $1
	`
	var acc Account
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&acc.ID, &acc.OwnerID, &acc.Balance, &acc.Currency, &acc.Version, &acc.CreatedAt, &acc.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	return &acc, nil
}

func (r *postgresRepository) GetAccountByIDWithLock(ctx context.Context, tx *sql.Tx, id string) (*Account, error) {
	query := `
		SELECT id, owner_id, balance, currency, version, created_at, updated_at
		FROM accounts WHERE id = $1 FOR UPDATE
	`
	var acc Account
	var row *sql.Row
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, id)
	} else {
		row = r.db.QueryRowContext(ctx, query, id)
	}

	err := row.Scan(
		&acc.ID, &acc.OwnerID, &acc.Balance, &acc.Currency, &acc.Version, &acc.CreatedAt, &acc.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get account with lock: %w", err)
	}
	return &acc, nil
}

func (r *postgresRepository) UpdateAccountBalance(ctx context.Context, tx *sql.Tx, id string, newBalance float64, oldVersion int) error {
	query := `
		UPDATE accounts
		SET balance = $1, version = version + 1, updated_at = NOW()
		WHERE id = $2 AND version = $3
	`
	var result sql.Result
	var err error
	if tx != nil {
		result, err = tx.ExecContext(ctx, query, newBalance, id, oldVersion)
	} else {
		result, err = r.db.ExecContext(ctx, query, newBalance, id, oldVersion)
	}
	if err != nil {
		return fmt.Errorf("failed to update account balance: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("optimistic lock failure: account version mismatch")
	}

	return nil
}

func (r *postgresRepository) CreateLedgerEntry(ctx context.Context, tx *sql.Tx, entry *BalanceLedger) error {
	query := `
		INSERT INTO balance_ledgers (id, account_id, amount, type, reference_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	var err error
	if tx != nil {
		_, err = tx.ExecContext(ctx, query, entry.ID, entry.AccountID, entry.Amount, entry.Type, entry.ReferenceID, entry.CreatedAt)
	} else {
		_, err = r.db.ExecContext(ctx, query, entry.ID, entry.AccountID, entry.Amount, entry.Type, entry.ReferenceID, entry.CreatedAt)
	}
	if err != nil {
		return fmt.Errorf("failed to insert balance ledger entry: %w", err)
	}
	return nil
}

func (r *postgresRepository) GetLedgerSum(ctx context.Context, accountID string) (float64, error) {
	query := `
		SELECT COALESCE(
			SUM(CASE WHEN type = 'CREDIT' THEN amount ELSE -amount END), 0.0000
		)
		FROM balance_ledgers
		WHERE account_id = $1
	`
	var total float64
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate ledger sum: %w", err)
	}
	return total, nil
}
