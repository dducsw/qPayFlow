package account

import (
	"encoding/json"
	"net/http"
)

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /accounts/{id}", h.GetAccount)
	mux.HandleFunc("POST /accounts/transfer", h.Transfer)
	mux.HandleFunc("GET /health", h.HealthCheck)
}

func (h *Handler) GetAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "invalid account id")
		return
	}

	acc, err := h.service.GetAccount(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if acc == nil {
		h.respondError(w, http.StatusNotFound, "account not found")
		return
	}

	h.respondJSON(w, http.StatusOK, acc)
}

type TransferRequest struct {
	TransactionID string  `json:"transaction_id"`
	FromAccountID string  `json:"from_account_id"`
	ToAccountID   string  `json:"to_account_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	Description   string  `json:"description"`
	LockStrategy  string  `json:"lock_strategy,omitempty"` // "pessimistic" (default) or "redis"
}

func (h *Handler) Transfer(w http.ResponseWriter, r *http.Request) {
	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var err error
	if req.LockStrategy == "redis" {
		err = h.service.TransferWithRedisLock(r.Context(), req.TransactionID, req.FromAccountID, req.ToAccountID, req.Amount, req.Currency, req.Description)
	} else {
		err = h.service.Transfer(r.Context(), req.TransactionID, req.FromAccountID, req.ToAccountID, req.Amount, req.Currency, req.Description)
	}

	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]string{
		"status":         "SUCCESS",
		"transaction_id": req.TransactionID,
	})
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "UP", "service": "account-service"})
}

func (h *Handler) respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, message string) {
	h.respondJSON(w, status, map[string]string{"error": message})
}
