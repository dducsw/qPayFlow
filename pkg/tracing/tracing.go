package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"
)

type contextKey string

const (
	TraceparentHeader            = "traceparent"
	TraceparentCtxKey contextKey = "traceparent"
)

// GenerateTraceparent creates a W3C traceparent string: "00-<trace_id>-<parent_id>-01"
func GenerateTraceparent() string {
	traceID := make([]byte, 16)
	parentID := make([]byte, 8)
	_, _ = rand.Read(traceID)
	_, _ = rand.Read(parentID)
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID), hex.EncodeToString(parentID))
}

// ExtractFromHTTP extracts traceparent from HTTP headers, or generates a new one.
func ExtractFromHTTP(r *http.Request) string {
	tp := r.Header.Get(TraceparentHeader)
	if tp == "" || !isValidTraceparent(tp) {
		tp = GenerateTraceparent()
	}
	return tp
}

// InjectToHTTP injects traceparent into outgoing HTTP request headers.
func InjectToHTTP(req *http.Request, traceparent string) {
	if traceparent == "" {
		traceparent = GenerateTraceparent()
	}
	req.Header.Set(TraceparentHeader, traceparent)
}

// ExtractFromKafka extracts traceparent from Kafka message headers.
func ExtractFromKafka(msg kafka.Message) string {
	for _, h := range msg.Headers {
		if strings.EqualFold(h.Key, TraceparentHeader) {
			if isValidTraceparent(string(h.Value)) {
				return string(h.Value)
			}
		}
	}
	return GenerateTraceparent()
}

// InjectToKafkaHeaders adds traceparent header to Kafka message headers.
func InjectToKafkaHeaders(headers []kafka.Header, traceparent string) []kafka.Header {
	if traceparent == "" {
		traceparent = GenerateTraceparent()
	}
	// Avoid duplicates
	var clean []kafka.Header
	for _, h := range headers {
		if !strings.EqualFold(h.Key, TraceparentHeader) {
			clean = append(clean, h)
		}
	}
	return append(clean, kafka.Header{
		Key:   TraceparentHeader,
		Value: []byte(traceparent),
	})
}

// WithTraceparent stores traceparent in Go context.
func WithTraceparent(ctx context.Context, tp string) context.Context {
	return context.WithValue(ctx, TraceparentCtxKey, tp)
}

// FromContext retrieves traceparent from Go context.
func FromContext(ctx context.Context) string {
	if v, ok := ctx.Value(TraceparentCtxKey).(string); ok && v != "" {
		return v
	}
	return GenerateTraceparent()
}

func isValidTraceparent(tp string) bool {
	parts := strings.Split(tp, "-")
	return len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16
}
