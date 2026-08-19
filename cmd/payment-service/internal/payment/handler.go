package payment

import (
	"encoding/json"
	"errors"
	"net/http"

	"qpayflow/pkg/idempotency"
)

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /payments", h.CreatePayment)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)
	mux.HandleFunc("GET /health", h.HealthCheck)
}

func (h *Handler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var req CreatePaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	// Capture idempotency-key header if not in JSON body
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}

	p, err := h.service.CreatePayment(r.Context(), &req)
	if err != nil {
		if errors.Is(err, idempotency.ErrRequestInFlight) {
			h.respondError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, idempotency.ErrKeyConflict) {
			h.respondError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.respondJSON(w, http.StatusCreated, p)
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "payment id is required")
		return
	}

	p, err := h.service.GetPayment(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if p == nil {
		h.respondError(w, http.StatusNotFound, "payment not found")
		return
	}

	h.respondJSON(w, http.StatusOK, p)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "UP", "service": "payment-service"})
}

func (h *Handler) respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, message string) {
	h.respondJSON(w, status, map[string]string{"error": message})
}
