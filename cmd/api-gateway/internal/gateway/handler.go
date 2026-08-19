package gateway

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

type Handler struct {
	paymentServiceProxy *httputil.ReverseProxy
	accountServiceProxy *httputil.ReverseProxy
}

func NewHandler() *Handler {
	// Downstream targets
	paymentRawURL := os.Getenv("PAYMENT_SERVICE_URL")
	if paymentRawURL == "" {
		paymentRawURL = "http://payment-service:8001"
	}
	paymentURL, _ := url.Parse(paymentRawURL)
	if paymentURL == nil || paymentURL.Host == "" {
		paymentURL, _ = url.Parse("http://localhost:8001")
	}

	accountRawURL := os.Getenv("ACCOUNT_SERVICE_URL")
	if accountRawURL == "" {
		accountRawURL = "http://account-service:8002"
	}
	accountURL, _ := url.Parse(accountRawURL)
	if accountURL == nil || accountURL.Host == "" {
		accountURL, _ = url.Parse("http://localhost:8002")
	}

	slog.Info("initializing proxy routes",
		"payment_service_url", paymentURL.String(),
		"account_service_url", accountURL.String(),
	)

	return &Handler{
		paymentServiceProxy: httputil.NewSingleHostReverseProxy(paymentURL),
		accountServiceProxy: httputil.NewSingleHostReverseProxy(accountURL),
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux, mw *Middleware) {
	// Payment routes
	mux.Handle("POST /payments", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyPayment)))))
	mux.Handle("GET /payments/{id}", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyPayment)))))
	mux.Handle("/payments", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyPayment)))))
	mux.Handle("/payments/", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyPayment)))))

	// Account routes
	mux.Handle("GET /accounts/{id}", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyAccount)))))
	mux.Handle("POST /accounts/transfer", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyAccount)))))
	mux.Handle("/accounts", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyAccount)))))
	mux.Handle("/accounts/", mw.Traceparent(mw.RateLimit(mw.Logging(http.HandlerFunc(h.ProxyAccount)))))
}

func (h *Handler) ProxyPayment(w http.ResponseWriter, r *http.Request) {
	slog.Debug("proxying request to payment-service", "path", r.URL.Path)
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	h.paymentServiceProxy.ServeHTTP(w, r)
}

func (h *Handler) ProxyAccount(w http.ResponseWriter, r *http.Request) {
	slog.Debug("proxying request to account-service", "path", r.URL.Path)
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	h.accountServiceProxy.ServeHTTP(w, r)
}

func (h *Handler) ProxyHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP","service":"api-gateway"}`))
}
