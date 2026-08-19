package main

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// PaymentRequest is the HTTP payload for the payments API.
type PaymentRequest struct {
	IdempotencyKey  string  `json:"idempotency_key"`
	SourceAccountID string  `json:"source_account_id"`
	TargetAccountID string  `json:"target_account_id"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
}

// MetricCollector is a thread-safe latency and status accumulator.
type MetricCollector struct {
	mu           sync.Mutex
	latencies    []time.Duration
	statusCodes  map[int]int
	errorCount   int
	successCount int
}

func newMetricCollector(capacity int) *MetricCollector {
	return &MetricCollector{
		latencies:   make([]time.Duration, 0, capacity),
		statusCodes: make(map[int]int),
	}
}

func (m *MetricCollector) Record(status int, duration time.Duration, isErr bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, duration)
	m.statusCodes[status]++
	if isErr || status >= 400 {
		m.errorCount++
	} else {
		m.successCount++
	}
}

func main() {
	mode := flag.String("mode", "seed", "Execution mode: 'seed' (DB seeding) or 'load' (HTTP traffic generator)")
	count := flag.Int("count", 100, "Number of accounts to seed (seed mode) / payments to generate (load mode)")
	accountPool := flag.Int("accounts", 100, "Number of existing accounts pool to generate payments between (load mode)")
	currency := flag.String("currency", "USD", "Currency for transactions and account provisioning (default: USD)")
	dbURL := flag.String("db", "postgres://qpayflow:qpayflow_secret@localhost:5432/qpayflow_db?sslmode=disable", "Postgres Connection URL")
	gatewayURL := flag.String("url", "http://localhost:8000/payments", "API Gateway payments endpoint")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers for load generator")
	rampUp := flag.Duration("ramp-up", 0, "Staggered worker start duration for load mode (e.g. 10s). 0 = instant")
	retryRatio := flag.Float64("retry-ratio", 0.1, "Fraction [0.0–1.0] of load requests that replay a prior idempotency key")
	seedWorkers := flag.Int("seed-workers", 8, "Concurrent DB workers for seed mode")
	lowBalancePct := flag.Int("low-balance-pct", 20, "Percent of seeded accounts with low balance (50–200) to exercise FAILED payment path")
	since := flag.Duration("since", 5*time.Minute, "Verify mode: look at payments created within this window (e.g. 5m)")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "Verify mode: DB polling interval")
	verifyTimeout := flag.Duration("verify-timeout", 60*time.Second, "Verify mode: max time to wait for all payments to settle")

	flag.Parse()

	switch *mode {
	case "seed":
		runDBSeeder(*dbURL, *currency, *count, *seedWorkers, *lowBalancePct)
	case "load":
		runTrafficGenerator(*gatewayURL, *currency, *count, *accountPool, *concurrency, *rampUp, *retryRatio)
	case "verify":
		runVerifyMode(*dbURL, *since, *pollInterval, *verifyTimeout)
	default:
		log.Fatalf("Unknown mode: %q. Use 'seed', 'load', or 'verify'", *mode)
	}
}

// ── Seed mode ─────────────────────────────────────────────────────────────────

func runDBSeeder(dbURL, currency string, count, workers, lowBalancePct int) {
	log.Printf("[Seeder] Connecting to Postgres…")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("[Seeder] Open DB: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("[Seeder] Ping: %v", err)
	}

	log.Printf("[Seeder] Seeding %d accounts (%d%% low-balance) using %d workers…", count, lowBalancePct, workers)

	type seedJob struct {
		index      int
		lowBalance bool
	}

	jobs := make(chan seedJob, count)
	var (
		mu      sync.Mutex
		success int
		wg      sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				balance := 10_000.0000
				if job.lowBalance {
					// 50, 100, 150, or 200 — small enough to cause FAILED payments under load
					balance = float64((cryptoRandInt(4) + 1) * 50)
				}
				if seedAccount(db, job.index, balance, currency) {
					mu.Lock()
					success++
					mu.Unlock()
				}
			}
		}()
	}

	for i := 1; i <= count; i++ {
		jobs <- seedJob{index: i, lowBalance: cryptoRandInt(100) < lowBalancePct}
	}
	close(jobs)
	wg.Wait()

	log.Printf("[Seeder] Done: %d/%d accounts provisioned.", success, count)
}

// seedAccount inserts one account + initial ledger entry atomically.
// ON CONFLICT DO NOTHING makes it safe to re-run without corrupting existing balances.
func seedAccount(db *sql.DB, idx int, balance float64, currency string) bool {
	accountID := fmt.Sprintf("acc_%04d", idx)
	ownerID := fmt.Sprintf("owner_%04d", idx)

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[Seeder] Begin tx %s: %v", accountID, err)
		return false
	}

	_, err = tx.Exec(`
		INSERT INTO accounts (id, owner_id, balance, currency, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 1, NOW(), NOW())
		ON CONFLICT (id) DO NOTHING`,
		accountID, ownerID, balance, currency,
	)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("[Seeder] Rollback failed for %s: %v", accountID, rbErr)
		}
		log.Printf("[Seeder] Insert account %s: %v", accountID, err)
		return false
	}

	ledgerID := "ldg_init_" + generateUUID()
	_, err = tx.Exec(`
		INSERT INTO balance_ledgers (id, account_id, amount, type, reference_id, created_at)
		VALUES ($1, $2, $3, 'CREDIT', 'system_init', NOW())
		ON CONFLICT (id) DO NOTHING`,
		ledgerID, accountID, balance,
	)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("[Seeder] Rollback failed (ledger) for %s: %v", accountID, rbErr)
		}
		log.Printf("[Seeder] Insert ledger for %s: %v", accountID, err)
		return false
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[Seeder] Commit for %s: %v", accountID, err)
		return false
	}
	return true
}

// ── Load mode ─────────────────────────────────────────────────────────────────

// requestStore holds previously sent payloads for idempotency replay.
type requestStore struct {
	mu    sync.RWMutex
	items []PaymentRequest
}

func (s *requestStore) add(p PaymentRequest) {
	s.mu.Lock()
	s.items = append(s.items, p)
	s.mu.Unlock()
}

// sample returns a random previously stored payload for idempotency replay.
func (s *requestStore) sample() (PaymentRequest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.items) == 0 {
		return PaymentRequest{}, false
	}
	return s.items[cryptoRandInt(len(s.items))], true
}

func runTrafficGenerator(
	gatewayURL, currency string,
	totalPayments, accountPoolSize, concurrency int,
	rampUp time.Duration,
	retryRatio float64,
) {
	if accountPoolSize < 2 {
		log.Fatalf("Account pool (-accounts) must be >= 2 to simulate transfers.")
	}

	log.Printf("=================================================================")
	log.Printf("Starting Traffic & Benchmark Generator")
	log.Printf("Gateway URL  : %s", gatewayURL)
	log.Printf("Currency     : %s", currency)
	log.Printf("Total Target : %d payments", totalPayments)
	log.Printf("Account Pool : acc_0001 → acc_%04d", accountPoolSize)
	log.Printf("Concurrency  : %d workers", concurrency)
	log.Printf("Ramp-Up      : %v", rampUp)
	log.Printf("Retry Ratio  : %.0f%% (idempotency replay)", retryRatio*100)
	log.Printf("=================================================================")

	metrics := newMetricCollector(totalPayments)
	store := &requestStore{}

	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}

	// Staggered start: spread worker activation evenly over rampUp duration.
	var workerDelay time.Duration
	if rampUp > 0 && concurrency > 1 {
		workerDelay = rampUp / time.Duration(concurrency)
	}

	jobs := make(chan int, totalPayments)
	var wg sync.WaitGroup
	benchmarkStart := time.Now()

	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Ramp-up: each worker starts after a staggered delay.
			if workerDelay > 0 {
				time.Sleep(time.Duration(workerID-1) * workerDelay)
			}

			client := &http.Client{
				Transport: transport,
				Timeout:   10 * time.Second,
			}

			for range jobs {
				var payload PaymentRequest

				// Idempotency replay: re-send a previously sent exact payload.
				isRetry := retryRatio > 0 && cryptoRandInt(1000) < int(retryRatio*1000)
				if isRetry {
					if p, ok := store.sample(); ok {
						payload = p
					} else {
						isRetry = false // nothing stored yet, fall through to fresh request
					}
				}

				if !isRetry {
					srcIdx := cryptoRandInt(accountPoolSize) + 1
					dstIdx := cryptoRandInt(accountPoolSize) + 1
					for dstIdx == srcIdx {
						dstIdx = cryptoRandInt(accountPoolSize) + 1
					}
					payload = PaymentRequest{
						IdempotencyKey:  generateUUID(),
						SourceAccountID: fmt.Sprintf("acc_%04d", srcIdx),
						TargetAccountID: fmt.Sprintf("acc_%04d", dstIdx),
						Amount:          float64((cryptoRandInt(40) + 1) * 5), // 5..200, step 5
						Currency:        currency,
					}
					store.add(payload)
				}

				body, _ := json.Marshal(payload)
				req, err := http.NewRequest(http.MethodPost, gatewayURL, bytes.NewBuffer(body))
				if err != nil {
					metrics.Record(0, 0, true)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", payload.IdempotencyKey)

				start := time.Now()
				resp, err := client.Do(req)
				duration := time.Since(start)

				if err != nil {
					metrics.Record(0, duration, true)
					continue
				}

				// Drain to enable connection reuse.
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				metrics.Record(resp.StatusCode, duration, resp.StatusCode >= 400)
			}
		}(w)
	}

	for i := 1; i <= totalPayments; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	totalDuration := time.Since(benchmarkStart)
	printBenchmarkReport(metrics, totalDuration, totalPayments)
}

func printBenchmarkReport(m *MetricCollector, totalDuration time.Duration, totalExpected int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	total := len(m.latencies)
	if total == 0 {
		log.Println("No requests were executed.")
		return
	}

	sort.Slice(m.latencies, func(i, j int) bool {
		return m.latencies[i] < m.latencies[j]
	})

	// clamp prevents index out-of-bounds on small sample sizes.
	clamp := func(pct float64) time.Duration {
		idx := int(float64(total) * pct)
		if idx >= total {
			idx = total - 1
		}
		return m.latencies[idx]
	}

	var totalLatency time.Duration
	for _, l := range m.latencies {
		totalLatency += l
	}

	avg := totalLatency / time.Duration(total)
	rps := float64(total) / totalDuration.Seconds()

	fmt.Println()
	fmt.Println("====================== BENCHMARK SUMMARY ======================")
	fmt.Printf("Total Requests   : %d / %d\n", total, totalExpected)
	fmt.Printf("Duration         : %v\n", totalDuration)
	fmt.Printf("Throughput (RPS) : %.2f req/s\n", rps)
	fmt.Printf("Success Rate     : %.2f%% (%d OK / %d ERR)\n",
		float64(m.successCount)/float64(total)*100.0, m.successCount, m.errorCount)
	fmt.Println("---------------------- LATENCY DISTRIBUTION -------------------")
	fmt.Printf("Min              : %v\n", m.latencies[0])
	fmt.Printf("Avg              : %v\n", avg)
	fmt.Printf("P50 (Median)     : %v\n", clamp(0.50))
	fmt.Printf("P90              : %v\n", clamp(0.90))
	fmt.Printf("P95              : %v\n", clamp(0.95))
	fmt.Printf("P99              : %v\n", clamp(0.99))
	fmt.Printf("Max              : %v\n", m.latencies[total-1])
	fmt.Println("---------------------- HTTP STATUS CODES ----------------------")
	for code, count := range m.statusCodes {
		fmt.Printf("  HTTP %-3d       : %d\n", code, count)
	}
	fmt.Println("===============================================================")
}

// ── Verify mode ───────────────────────────────────────────────────────────────

// paymentStat holds the status and async processing latency of a single payment.
type paymentStat struct {
	status  string
	latency time.Duration // updated_at - created_at; zero if still PENDING/PROCESSING
}

// runVerifyMode polls Postgres to measure async payment processing latency.
// Looks at payments created within `since` and waits until all settle or `verifyTimeout` elapses.
func runVerifyMode(dbURL string, since, pollInterval, verifyTimeout time.Duration) {
	log.Printf("[Verify] Connecting to Postgres\u2026")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("[Verify] Open DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("[Verify] Ping: %v", err)
	}

	windowStart := time.Now().Add(-since)
	log.Printf("[Verify] Window  : last %v (since %v)", since, windowStart.Format(time.RFC3339))
	log.Printf("[Verify] Timeout : %v  Poll: %v", verifyTimeout, pollInterval)

	deadline := time.Now().Add(verifyTimeout)
	var lastStats []paymentStat
	var outboxPending int

	for time.Now().Before(deadline) {
		stats, pending, err := queryPaymentStats(db, windowStart)
		if err != nil {
			log.Printf("[Verify] Query error: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		lastStats = stats
		outboxPending = pending

		settled := 0
		for _, s := range stats {
			if s.status != "PENDING" && s.status != "PROCESSING" {
				settled++
			}
		}
		log.Printf("[Verify] %d total | %d settled | %d outbox PENDING",
			len(stats), settled, outboxPending)

		if len(stats) > 0 && settled == len(stats) {
			log.Printf("[Verify] All payments settled.")
			break
		}
		time.Sleep(pollInterval)
	}

	printVerifyReport(lastStats, outboxPending)
}

// queryPaymentStats fetches status + async latency for all payments in window,
// and the current outbox PENDING count as a consumer-lag proxy.
func queryPaymentStats(db *sql.DB, windowStart time.Time) ([]paymentStat, int, error) {
	rows, err := db.Query(`
		SELECT status,
		       EXTRACT(EPOCH FROM (updated_at - created_at)) * 1e9 AS latency_ns
		FROM   payments
		WHERE  created_at >= $1`,
		windowStart,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("payments query: %w", err)
	}
	defer rows.Close()

	var stats []paymentStat
	for rows.Next() {
		var status string
		var latencyNs float64
		if err := rows.Scan(&status, &latencyNs); err != nil {
			return nil, 0, fmt.Errorf("scan: %w", err)
		}
		stats = append(stats, paymentStat{
			status:  status,
			latency: time.Duration(latencyNs),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// Outbox backlog: PENDING events = proxy for Kafka consumer lag.
	var pending int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM outbox_events
		WHERE  status = 'PENDING' AND created_at >= $1`, windowStart,
	).Scan(&pending); err != nil {
		return nil, 0, fmt.Errorf("outbox query: %w", err)
	}

	return stats, pending, nil
}

func printVerifyReport(stats []paymentStat, outboxPending int) {
	if len(stats) == 0 {
		log.Println("[Verify] No payments found in window.")
		return
	}

	statusCount := map[string]int{}
	var settled []time.Duration
	for _, s := range stats {
		statusCount[s.status]++
		if s.status != "PENDING" && s.status != "PROCESSING" && s.latency > 0 {
			settled = append(settled, s.latency)
		}
	}

	sort.Slice(settled, func(i, j int) bool { return settled[i] < settled[j] })

	clamp := func(pct float64) time.Duration {
		if len(settled) == 0 {
			return 0
		}
		idx := int(float64(len(settled)) * pct)
		if idx >= len(settled) {
			idx = len(settled) - 1
		}
		return settled[idx]
	}

	fmt.Println()
	fmt.Println("================== ASYNC PROCESSING REPORT ====================")
	fmt.Printf("Total Payments   : %d\n", len(stats))
	fmt.Println("--- Status Breakdown -------------------------------------------")
	for status, count := range statusCount {
		pct := float64(count) / float64(len(stats)) * 100
		fmt.Printf("  %-12s : %d (%.1f%%)\n", status, count, pct)
	}
	fmt.Println("--- Async End-to-End Latency (created_at \u2192 updated_at) ----------")
	if len(settled) == 0 {
		fmt.Println("  No settled payments to measure.")
	} else {
		var total time.Duration
		for _, l := range settled {
			total += l
		}
		fmt.Printf("  Settled        : %d\n", len(settled))
		fmt.Printf("  Min            : %v\n", settled[0])
		fmt.Printf("  Avg            : %v\n", total/time.Duration(len(settled)))
		fmt.Printf("  P50            : %v\n", clamp(0.50))
		fmt.Printf("  P90            : %v\n", clamp(0.90))
		fmt.Printf("  P95            : %v\n", clamp(0.95))
		fmt.Printf("  P99            : %v\n", clamp(0.99))
		fmt.Printf("  Max            : %v\n", settled[len(settled)-1])
	}
	fmt.Println("--- Outbox Backlog (Kafka consumer lag proxy) ------------------")
	fmt.Printf("  PENDING events : %d\n", outboxPending)
	fmt.Println("================================================================")
}

// generateUUID produces an RFC 4122 v4 UUID.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// cryptoRandInt returns a cryptographically random int in [0, max).
func cryptoRandInt(max int) int {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return int(n.Int64())
}
