package reconciliation

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"time"
)

type ReconciliationJob struct {
	db       *sql.DB
	interval time.Duration
}

func NewReconciliationJob(db *sql.DB, interval time.Duration) *ReconciliationJob {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ReconciliationJob{
		db:       db,
		interval: interval,
	}
}

// Start begins periodic reconciliation when the node is elected leader.
func (j *ReconciliationJob) Start(ctx context.Context) {
	slog.Info("starting distributed reconciliation job", "interval", j.interval)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	// Run initial pass immediately
	j.RunAudit(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("reconciliation job stopped (context canceled)")
			return
		case <-ticker.C:
			j.RunAudit(ctx)
		}
	}
}

// RunAudit performs both Double-Entry Ledger Reconciliation and Three-Way Reconciliation checks.
func (j *ReconciliationJob) RunAudit(ctx context.Context) {
	slog.Info("=== STARTING PERIODIC RECONCILIATION AUDIT ===")

	// 1. Ledger Balance Integrity Audit: SUM(ledger.amount) == account.balance
	ledgerDiscrepancies, err := j.auditLedgerIntegrity(ctx)
	if err != nil {
		slog.Error("error during ledger integrity audit", "error", err)
	} else if len(ledgerDiscrepancies) == 0 {
		slog.Info("Ledger Integrity Check: PASSED (All accounts match ledger sum)")
	} else {
		for _, d := range ledgerDiscrepancies {
			slog.Error("DISCREPANCY DETECTED IN LEDGER",
				"account_id", d.AccountID,
				"balance", d.CurrentBalance,
				"ledger_sum", d.LedgerSum,
				"difference", d.Difference,
			)
		}
	}

	// 2. Three-Way Reconciliation: Compare Payments table vs Ledger entries
	missingInLedger, err := j.auditThreeWayReconciliation(ctx)
	if err != nil {
		slog.Error("error during three-way reconciliation", "error", err)
	} else if missingInLedger == 0 {
		slog.Info("Three-Way Reconciliation Check: PASSED (All SUCCESS payments have matching ledger pairs)")
	} else {
		slog.Warn("Three-Way Reconciliation: Inconsistencies detected", "missing_count", missingInLedger)
	}

	slog.Info("=== RECONCILIATION AUDIT FINISHED ===")
}

type LedgerDiscrepancy struct {
	AccountID      string
	CurrentBalance float64
	LedgerSum      float64
	Difference     float64
}

func (j *ReconciliationJob) auditLedgerIntegrity(ctx context.Context) ([]LedgerDiscrepancy, error) {
	query := `
		SELECT 
			a.id, 
			a.balance, 
			COALESCE(SUM(CASE WHEN l.type = 'CREDIT' THEN l.amount ELSE -l.amount END), 0.0000) AS ledger_sum
		FROM accounts a
		LEFT JOIN balance_ledgers l ON a.id = l.account_id
		GROUP BY a.id, a.balance
	`

	rows, err := j.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledger integrity: %w", err)
	}
	defer rows.Close()

	var discrepancies []LedgerDiscrepancy
	for rows.Next() {
		var accountID string
		var balance, ledgerSum float64
		if err := rows.Scan(&accountID, &balance, &ledgerSum); err != nil {
			return nil, err
		}

		diff := math.Abs(balance - ledgerSum)
		if diff > 0.0001 { // Account for floating point epsilon
			discrepancies = append(discrepancies, LedgerDiscrepancy{
				AccountID:      accountID,
				CurrentBalance: balance,
				LedgerSum:      ledgerSum,
				Difference:     balance - ledgerSum,
			})
		}
	}

	return discrepancies, nil
}

func (j *ReconciliationJob) auditThreeWayReconciliation(ctx context.Context) (int, error) {
	// Query SUCCESS payments that lack balanced (debit & credit) ledger entries
	query := `
		SELECT COUNT(p.id)
		FROM payments p
		LEFT JOIN (
			SELECT reference_id, COUNT(id) as ledger_count
			FROM balance_ledgers
			GROUP BY reference_id
		) l ON p.id = l.reference_id
		WHERE p.status = 'SUCCESS' AND (l.ledger_count IS NULL OR l.ledger_count < 2)
	`
	var missingCount int
	err := j.db.QueryRowContext(ctx, query).Scan(&missingCount)
	if err != nil {
		return 0, fmt.Errorf("failed to check three-way reconciliation: %w", err)
	}
	return missingCount, nil
}
