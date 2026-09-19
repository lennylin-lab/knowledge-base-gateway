package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// APIError is the stable, documented error envelope.
type APIError struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the nested error payload.
type ErrorBody struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(w http.ResponseWriter, requestID string, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", requestID)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIError{Error: ErrorBody{
		Type: typ, Code: code, Message: msg, RequestID: requestID,
	}})
}

// V1.4 async error sentinels. New codes are additive to the stable envelope
// and never change the existing sync mappings.
var (
	errIdempotencyConflict = errors.New("idempotency key conflict")
	errResponseNotFound    = errors.New("response not found")
	errResponseExpired     = errors.New("response expired")
	errQueueUnavailable    = errors.New("job queue unavailable")
)

// mapError translates internal failures into the documented envelope without
// leaking provider status codes, secrets, or internal details.
func mapError(w http.ResponseWriter, requestID string, err error) {
	// V1.4 async outcomes: the roadmap-stable codes, classified before the
	// generic fallbacks so they can never collide with an internal 500.
	switch {
	case errors.Is(err, errQueueUnavailable):
		// Job-store outage: 503-class, never disguised as a job failure.
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "job_queue_unavailable", "the service is temporarily unable to accept requests")
		return
	case errors.Is(err, errIdempotencyConflict):
		writeError(w, requestID, http.StatusConflict, "invalid_request_error", "idempotency_conflict", "this Idempotency-Key was already used with a different request")
		return
	case errors.Is(err, errResponseNotFound):
		// Non-leaky: absent and not-owned are indistinguishable.
		writeError(w, requestID, http.StatusNotFound, "invalid_request_error", "response_not_found", "the response does not exist for this principal")
		return
	case errors.Is(err, errResponseExpired):
		writeError(w, requestID, http.StatusGone, "invalid_request_error", "response_expired", "the response result has expired")
		return
	}

	// Limiter/quota infrastructure failure: 503-class with a non-leaky
	// envelope; must not be confused with a genuine limit denial (429).
	if errors.Is(err, limiter.ErrUnavailable) {
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "limiter_unavailable", "the service is temporarily unable to accept requests")
		return
	}
	var qErr *quota.Error
	if errors.As(err, &qErr) {
		// Quota denial: 429 with the stable quota_exceeded code. Policy
		// internals (limits, remaining budgets) are never echoed.
		writeError(w, requestID, http.StatusTooManyRequests, "rate_limit_error", qErr.Code, "token quota exceeded for this subject")
		return
	}
	var lErr *limiter.Error
	if errors.As(err, &lErr) {
		writeError(w, requestID, http.StatusTooManyRequests, "rate_limit_error", lErr.Code, "rate limit exceeded")
		return
	}
	switch {
	case errors.Is(err, auth.ErrInvalid):
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid API key")
	case errors.Is(err, auth.ErrExpired):
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "api_key_expired", "API key expired")
	case errors.Is(err, auth.ErrRevoked):
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "api_key_revoked", "API key revoked")
	case errors.Is(err, gateway.ErrNoRoute):
		writeError(w, requestID, http.StatusServiceUnavailable, "temporary_error", "no_route_available", "no provider route is currently available")
	case errors.Is(err, gateway.ErrUnknownModel), errors.Is(err, gateway.ErrNotPermitted):
		// Deliberately non-leaky: missing vs forbidden are indistinguishable.
		writeError(w, requestID, http.StatusForbidden, "permission_error", "model_not_allowed", "the requested model is not available for this principal")
	case errors.Is(err, model.ErrCapabilityNotSupported):
		// Declared capability rejected before any provider invocation. The
		// message is self-describing and content-free (capability key +
		// protocol name); the envelope shape stays frozen. Admissions-layer
		// rejections carry a *model.CapabilityError; provider-sourced
		// capability failures keep the generic message.
		msg := "the requested capability is not supported by this model"
		var capErr *model.CapabilityError
		if errors.As(err, &capErr) {
			msg = capErr.Error()
		}
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "capability_not_supported", msg)
	case errors.Is(err, model.ErrEmbeddingDimMismatch):
		// Gateway configuration error: the upstream vector width differs from
		// the catalog declaration. 500-class, nothing returned, no vector or
		// dimension internals echoed beyond the config problem itself.
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "embedding_dim_mismatch", "model embedding dimension does not match the configured declaration")
	case errors.Is(err, errValidation):
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, requestID, http.StatusGatewayTimeout, "timeout_error", "upstream_timeout", "upstream request timed out")
	default:
		switch provider.ClassOf(err) {
		case provider.ClassTimeout:
			writeError(w, requestID, http.StatusGatewayTimeout, "timeout_error", "upstream_timeout", "upstream request timed out")
		case provider.ClassRateLimited:
			writeError(w, requestID, http.StatusServiceUnavailable, "temporary_error", "upstream_rate_limited", "upstream is temporarily rate limited")
		case provider.ClassServer, provider.ClassNetwork:
			writeError(w, requestID, http.StatusServiceUnavailable, "temporary_error", "upstream_unavailable", "upstream is temporarily unavailable")
		case provider.ClassInvalid:
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "upstream_rejected_request", "the request was rejected by the model provider")
		default:
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "internal_error", "internal server error")
		}
	}
}
