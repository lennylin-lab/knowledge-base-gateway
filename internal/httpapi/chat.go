package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

var errValidation = errors.New("invalid request")

// chatRequest is the accepted OpenAI-compatible request subset.
type chatRequest struct {
	Model       string             `json:"model"`
	Messages    []provider.Message `json:"messages"`
	Temperature *float64           `json:"temperature"`
	MaxTokens   *int               `json:"max_tokens"`
	Stream      bool               `json:"stream"`
	Metadata    json.RawMessage    `json:"metadata"`
}

// Authenticator is the bearer-key resolution boundary; both the in-memory
// auth.Store and the PostgreSQL-backed authenticator satisfy it.
type Authenticator interface {
	Authenticate(key string, now time.Time) (auth.Principal, error)
}

// ChatHandler serves POST /v1/chat/completions.
type ChatHandler struct {
	Auth     Authenticator
	Service  *gateway.Service
	Policy   *policy.Policy
	Limiter  limiter.Gate
	Audit    audit.Sink
	Metrics  *metrics.Registry
	MaxBody  int64
	MaxMsgs  int
	MaxChars int
}

func (h *ChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" || len(requestID) > 128 {
		requestID = newRequestID()
	}
	w.Header().Set("X-Request-ID", requestID)
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = requestID // request ID doubles as the trace root
	}
	w.Header().Set("X-Trace-ID", traceID)

	var principal auth.Principal
	fail := func(err error) {
		mapError(w, requestID, err)
		h.record(requestID, traceID, principal.SubjectID, principal.KeyID, "", "", 0, err, 1, nil, false, start)
	}

	// 1. Authentication before anything else.
	key, ok := bearer(r)
	if !ok {
		fail(fmt.Errorf("%w: missing bearer key", auth.ErrInvalid))
		return
	}
	var err error
	principal, err = h.Auth.Authenticate(key, time.Now())
	if err != nil {
		fail(err)
		return
	}

	// 2. Bounded request decoding and validation.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBody))
	if err != nil {
		fail(errValidation)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		fail(errValidation)
		return
	}
	if err := validate(&req, h.MaxMsgs, h.MaxChars); err != nil {
		fail(err)
		return
	}

	// 3. Model existence + subject policy (non-leaky combined errors).
	plan, err := h.Service.Resolve(principal.SubjectID, req.Model)
	if err != nil {
		fail(err)
		return
	}
	if h.Policy != nil && !h.Policy.Permitted(principal.SubjectID, req.Model) {
		fail(gateway.ErrNotPermitted)
		return
	}
	if h.Policy != nil {
		if limits, ok := h.Policy.LimitsFor(principal.SubjectID); ok && limits.MaxOutputTokens > 0 {
			if req.MaxTokens == nil || *req.MaxTokens > limits.MaxOutputTokens {
				capped := limits.MaxOutputTokens
				req.MaxTokens = &capped
			}
		}
	}

	// 4. Rate/concurrency limits.
	ok, retryAfter, release, limErr := h.Limiter.Allow(r.Context(), principal.SubjectID, time.Now())
	if limErr != nil {
		// Limiter infrastructure failure: 503-class, never a rate-limit 429.
		fail(limErr)
		return
	}
	if !ok {
		if retryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())+1))
		}
		h.MetricsIncRateLimited(req.Model)
		fail(&limiter.Error{Code: "rate_limit_exceeded"})
		return
	}
	defer release()

	preq := provider.ChatRequest{
		Model: plan.Primary(), Messages: req.Messages, Temperature: req.Temperature,
		MaxTokens: req.MaxTokens, RequestID: requestID,
	}

	if !req.Stream {
		h.complete(w, r, requestID, traceID, principal, req.Model, plan, preq, start)
		return
	}
	h.stream(w, r, requestID, traceID, principal, req.Model, plan, preq, start)
}

func (h *ChatHandler) complete(w http.ResponseWriter, r *http.Request, requestID, traceID string, principal auth.Principal, model string, plan gateway.Plan, preq provider.ChatRequest, start time.Time) {
	resp, providerName, err := h.Service.Complete(r.Context(), plan, preq)
	if err != nil {
		mapError(w, requestID, err)
		h.record(requestID, traceID, principal.SubjectID, principal.KeyID, model, providerName, 0, err, 1, nil, false, start)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
	h.record(requestID, traceID, principal.SubjectID, principal.KeyID, model, providerName, http.StatusOK, nil, 1, resp.Usage, false, start)
}

func (h *ChatHandler) stream(w http.ResponseWriter, r *http.Request, requestID, traceID string, principal auth.Principal, model string, plan gateway.Plan, preq provider.ChatRequest, start time.Time) {
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "streaming_unsupported", "streaming is not supported by this connection")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	var (
		writeErr  error
		sawOutput bool
	)
	send := func(payload []byte) error {
		sawOutput = true
		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	providerName, err := h.Service.Stream(r.Context(), plan, preq, send)
	if err == nil {
		// Terminal event only on normal completion.
		_, writeErr = io.WriteString(w, "data: [DONE]\n\n")
		if writeErr == nil {
			flusher.Flush()
		}
	}
	h.record(requestID, traceID, principal.SubjectID, principal.KeyID, model, providerName, statusFor(err, sawOutput), err, attempts(plan), nil, true, start)
	if err != nil && !sawOutput {
		// Nothing was sent yet: emit a normalized SSE error event.
		_, _ = fmt.Fprintf(w, "data: {\"error\":{\"type\":\"%s\",\"request_id\":%q}}\n\n", errorType(err), requestID)
		flusher.Flush()
	}
}

func statusFor(err error, sawOutput bool) int {
	if err == nil {
		return http.StatusOK
	}
	if sawOutput {
		return http.StatusOK // headers already sent; stream truncated
	}
	return 0
}

func errorType(err error) string {
	switch provider.ClassOf(err) {
	case provider.ClassTimeout:
		return "timeout_error"
	case provider.ClassRateLimited, provider.ClassServer, provider.ClassNetwork:
		return "temporary_error"
	default:
		return "internal_error"
	}
}

func attempts(plan gateway.Plan) int {
	if len(plan.Candidates) > 1 {
		return len(plan.Candidates)
	}
	return 1
}

func (h *ChatHandler) MetricsIncRateLimited(model string) {
	if h.Metrics != nil {
		h.Metrics.IncRateLimit(model)
	}
}

func (h *ChatHandler) record(requestID, traceID, subject, keyID, model, providerName string, status int, err error, routeAttempts int, usage *provider.Usage, streaming bool, start time.Time) {
	if h.Audit == nil {
		return
	}
	class := ""
	if err != nil {
		class = provider.ClassOf(err).String()
	}
	h.Audit.Write(audit.Event{
		RequestID: requestID, SubjectID: subject, KeyID: keyID, Model: model,
		Provider: providerName, Status: status, ErrorClass: class,
		LatencyMillis:    time.Since(start).Milliseconds(),
		PromptTokens:     usageTokens(usage, true),
		CompletionTokens: usageTokens(usage, false),
		Streaming:        streaming, CreatedAt: start,
		TraceID: traceID, RouteAttempts: routeAttempts,
	})
	if h.Metrics != nil {
		h.Metrics.IncRequest(model, fmt.Sprint(status))
		if usage != nil && usage.Known {
			h.Metrics.AddTokens(model, usage.PromptTokens, usage.CompletionTokens)
		}
		if err != nil && provider.ClassOf(err) != provider.ClassInternal {
			h.Metrics.IncUpstreamError(model, providerName, provider.ClassOf(err).String())
		}
	}
}

// usageTokens extracts prompt or completion tokens only when the upstream
// reported usage; unknown usage stays nil and is never coerced to zero.
func usageTokens(u *provider.Usage, prompt bool) *int {
	if u == nil || !u.Known {
		return nil
	}
	v := u.PromptTokens
	if !prompt {
		v = u.CompletionTokens
	}
	return &v
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}

func validate(req *chatRequest, maxMsgs, maxChars int) error {
	if req.Model == "" {
		return fmt.Errorf("%w: model is required", errValidation)
	}
	if len(req.Messages) == 0 {
		return fmt.Errorf("%w: messages must not be empty", errValidation)
	}
	if len(req.Messages) > maxMsgs {
		return fmt.Errorf("%w: too many messages", errValidation)
	}
	for _, m := range req.Messages {
		if m.Role == "" || len(m.Content) > maxChars {
			return fmt.Errorf("%w: invalid message", errValidation)
		}
	}
	if req.MaxTokens != nil && (*req.MaxTokens <= 0 || *req.MaxTokens > 1_000_000) {
		return fmt.Errorf("%w: max_tokens out of range", errValidation)
	}
	return nil
}

var requestCounter atomic.Int64

func newRequestID() string {
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), requestCounter.Add(1))
}
