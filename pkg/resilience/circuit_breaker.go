package resilience

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker: open (service unavailable)")
)

type State string

const (
	StateClosed   State = "CLOSED"
	StateHalfOpen State = "HALF_OPEN"
	StateOpen     State = "OPEN"
)

// CircuitBreaker Config & Struct
type CircuitBreaker struct {
	mu               sync.RWMutex
	state            State
	failureThreshold int
	successThreshold int
	timeout          time.Duration
	failureCount     int
	successCount     int
	lastStateChange  time.Time
}

func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		lastStateChange:  time.Now(),
	}
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	if cb.state == StateOpen {
		if time.Since(cb.lastStateChange) > cb.timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		if cb.state == StateHalfOpen || cb.failureCount >= cb.failureThreshold {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
			cb.failureCount = 0
		}
		return err
	}

	// Success path
	if cb.state == StateHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = StateClosed
			cb.failureCount = 0
			cb.lastStateChange = time.Now()
		}
	} else if cb.state == StateClosed {
		cb.failureCount = 0
	}

	return nil
}

// RetryWithBackoffJitter executes an operation with exponential backoff and randomized jitter.
func RetryWithBackoffJitter(
	ctx context.Context,
	maxAttempts int,
	baseDelay time.Duration,
	maxDelay time.Duration,
	fn func(attempt int) error,
) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}

		// Calculate Exponential Delay: baseDelay * 2^(attempt-1)
		multiplier := 1 << (attempt - 1)
		delay := baseDelay * time.Duration(multiplier)
		if delay > maxDelay {
			delay = maxDelay
		}

		// Add Full Jitter: rand [0, delay)
		jitter := time.Duration(0)
		if delay > 0 {
			n, _ := rand.Int(rand.Reader, big.NewInt(int64(delay)))
			jitter = time.Duration(n.Int64())
		}
		sleepDuration := jitter

		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("retry exceeded %d attempts: %w", maxAttempts, lastErr)
}
