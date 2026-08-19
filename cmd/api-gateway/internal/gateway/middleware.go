package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"qpayflow/pkg/tracing"

	"github.com/redis/go-redis/v9"
)

// slidingWindowLua executes atomic sliding-window rate limiting on Redis ZSET.
const slidingWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clearBefore = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, clearBefore)
local currentRequests = redis.call('ZCARD', key)

if currentRequests < limit then
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, math.ceil(window / 1000000000))
    return 1
else
    return 0
end
`

type Middleware struct {
	redis *redis.Client
}

func NewMiddleware(rdb *redis.Client) *Middleware {
	return &Middleware{redis: rdb}
}

func (m *Middleware) Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tp := tracing.FromContext(r.Context())

		slog.Info("incoming request",
			"method", r.Method,
			"path", r.URL.Path,
			"remote_addr", r.RemoteAddr,
			"traceparent", tp,
		)

		next.ServeHTTP(w, r)

		slog.Info("completed request",
			"method", r.Method,
			"path", r.URL.Path,
			"duration", time.Since(start),
			"traceparent", tp,
		)
	})
}

// RateLimit implements distributed Sliding-Window Rate Limiting (e.g. 100 requests per minute per IP).
func (m *Middleware) RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			clientIP = xff
		}

		key := fmt.Sprintf("ratelimit:gw:%s", clientIP)
		now := time.Now().UnixNano()
		window := int64(1 * time.Minute)
		limit := int64(300) // 300 requests per minute

		res, err := m.redis.Eval(r.Context(), slidingWindowLua, []string{key}, now, window, limit).Result()
		if err != nil {
			// Fail open on Redis connectivity issue, but log error
			slog.Warn("rate limiter evaluation error, failing open", "error", err)
			next.ServeHTTP(w, r)
			return
		}

		allowed, ok := res.(int64)
		if !ok || allowed != 1 {
			slog.Warn("rate limit exceeded", "client_ip", clientIP, "path", r.URL.Path)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"too many requests, rate limit exceeded"}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Traceparent extracts or assigns W3C traceparent and attaches it to request context and header.
func (m *Middleware) Traceparent(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tp := tracing.ExtractFromHTTP(r)
		tracing.InjectToHTTP(r, tp)
		w.Header().Set(tracing.TraceparentHeader, tp)

		ctx := tracing.WithTraceparent(r.Context(), tp)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
