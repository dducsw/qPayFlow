package account

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pkgredis "qpayflow/pkg/redis"

	"github.com/redis/go-redis/v9"
)

type Service interface {
	GetAccount(ctx context.Context, id string) (*Account, error)
	Transfer(ctx context.Context, txID string, fromAccID string, toAccID string, amount float64, currency string, description string) error
	TransferWithRedisLock(ctx context.Context, txID string, fromAccID string, toAccID string, amount float64, currency string, description string) error
}

type accountService struct {
	repo Repository
	rdb  *redis.Client
}

func NewService(repo Repository, rdb *redis.Client) Service {
	return &accountService{
		repo: repo,
		rdb:  rdb,
	}
}

func (s *accountService) GetAccount(ctx context.Context, id string) (*Account, error) {
	return s.repo.GetAccountByID(ctx, id)
}

// Transfer executes money transfer with Deadlock Prevention (alphabetical lock ordering) and Double-Entry Bookkeeping.
func (s *accountService) Transfer(ctx context.Context, txID string, fromAccID string, toAccID string, amount float64, currency string, description string) error {
	if amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	if fromAccID == "" || toAccID == "" {
		return errors.New("source and destination account IDs are required")
	}
	if fromAccID == toAccID {
		return errors.New("cannot transfer to the same account")
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Deadlock Prevention (Concept 16): Lock accounts in deterministic lexicographical order
	firstID, secondID := fromAccID, toAccID
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}

	firstAcc, err := s.repo.GetAccountByIDWithLock(ctx, tx, firstID)
	if err != nil {
		return fmt.Errorf("failed to lock account %s: %w", firstID, err)
	}
	if firstAcc == nil {
		return fmt.Errorf("account %s not found", firstID)
	}

	secondAcc, err := s.repo.GetAccountByIDWithLock(ctx, tx, secondID)
	if err != nil {
		return fmt.Errorf("failed to lock account %s: %w", secondID, err)
	}
	if secondAcc == nil {
		return fmt.Errorf("account %s not found", secondID)
	}

	// Map back to source and destination
	var srcAcc, destAcc *Account
	if firstAcc.ID == fromAccID {
		srcAcc = firstAcc
		destAcc = secondAcc
	} else {
		srcAcc = secondAcc
		destAcc = firstAcc
	}

	// 2. Check sufficient balance
	if srcAcc.Balance < amount {
		return fmt.Errorf("insufficient balance: current balance %.4f is less than transfer amount %.4f", srcAcc.Balance, amount)
	}

	// 3. Update account balances
	newSrcBalance := srcAcc.Balance - amount
	newDestBalance := destAcc.Balance + amount

	if err := s.repo.UpdateAccountBalance(ctx, tx, srcAcc.ID, newSrcBalance, srcAcc.Version); err != nil {
		return fmt.Errorf("failed to update source balance: %w", err)
	}

	if err := s.repo.UpdateAccountBalance(ctx, tx, destAcc.ID, newDestBalance, destAcc.Version); err != nil {
		return fmt.Errorf("failed to update destination balance: %w", err)
	}

	// 4. Double-Entry Bookkeeping (Concept 12): Create matching DEBIT and CREDIT entries
	debitEntry := &BalanceLedger{
		ID:          "ldg_dr_" + generateUUID(),
		AccountID:   srcAcc.ID,
		Amount:      amount,
		Type:        "DEBIT",
		ReferenceID: txID,
		CreatedAt:   time.Now(),
	}

	creditEntry := &BalanceLedger{
		ID:          "ldg_cr_" + generateUUID(),
		AccountID:   destAcc.ID,
		Amount:      amount,
		Type:        "CREDIT",
		ReferenceID: txID,
		CreatedAt:   time.Now(),
	}

	if err := s.repo.CreateLedgerEntry(ctx, tx, debitEntry); err != nil {
		return fmt.Errorf("failed to write debit ledger entry: %w", err)
	}

	if err := s.repo.CreateLedgerEntry(ctx, tx, creditEntry); err != nil {
		return fmt.Errorf("failed to write credit ledger entry: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transfer transaction: %w", err)
	}

	slog.Info("transfer completed successfully",
		"tx_id", txID,
		"from_account", fromAccID,
		"to_account", toAccID,
		"amount", amount,
	)

	return nil
}

// TransferWithRedisLock demonstrates Redis Distributed Lock across accounts.
func (s *accountService) TransferWithRedisLock(ctx context.Context, txID string, fromAccID string, toAccID string, amount float64, currency string, description string) error {
	if s.rdb == nil {
		return s.Transfer(ctx, txID, fromAccID, toAccID, amount, currency, description)
	}

	firstID, secondID := fromAccID, toAccID
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}

	lock1, err := pkgredis.AcquireLock(ctx, s.rdb, "lock:acc:"+firstID, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to acquire redis lock on %s: %w", firstID, err)
	}
	defer lock1.Release(context.Background())

	lock2, err := pkgredis.AcquireLock(ctx, s.rdb, "lock:acc:"+secondID, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to acquire redis lock on %s: %w", secondID, err)
	}
	defer lock2.Release(context.Background())

	return s.Transfer(ctx, txID, fromAccID, toAccID, amount, currency, description)
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
