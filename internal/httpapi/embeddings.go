package httpapi

// POST /v1/embeddings: the OpenAI-compatible embeddings proxy. Requests ride
// the shared admission pipeline (authentication before any body read,
// capability precheck before any provider call, rate limit, and the
// subject's shared daily/monthly token pool) with the "embeddings" protocol
// label. Vectors are never logged or audited; only metadata and reported
// input-token usage are recorded. Embeddings settle quota to the reported
// input-token usage; unknown usage keeps the conservative reservation.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// EmbeddingsHandler serves POST /v1/embeddings.
type EmbeddingsHandler struct {
	Auth     Authenticator
	Service  *gateway.Service
	Policy   *policy.Policy
	Limiter  limiter.Gate
	Quota    quota.Gate
	Audit    audit.Sink
	Metrics  *metrics.Registry
	MaxBody  int64
	MaxItems int
	MaxChars int
}

// fromEmbeddings builds the shared deps from an EmbeddingsHandler's fields.
func fromEmbeddings(h *EmbeddingsHandler) admissionDeps {
	return admissionDeps{
		Auth: h.Auth, Service: h.Service, Policy: h.Policy,
		Limiter: h.Limiter, Quota: h.Quota, Audit: h.Audit, Metrics: h.Metrics,
	}
}

// validateChatOnlyFields rejects the chat/responses-only request fields an
// embeddings call can never use. JSON null counts as absent.
func validateChatOnlyFields(raw map[string]json.RawMessage) error {
	for _, name := range []string{"stream", "messages", "instructions", "tools",
		"tool_choice", "response_format", "text", "max_tokens", "max_output_tokens"} {
		v, ok := raw[name]
		if !ok || len(v) == 0 || strings.EqualFold(strings.TrimSpace(string(v)), "null") {
			continue
		}
		return fmt.Errorf("%w: %s is not valid for embeddings requests", errValidation, name)
	}
	return nil
}

func (h *EmbeddingsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	traceID := traceIDOf(r, requestID)
	w.Header().Set("X-Trace-ID", traceID)
	deps := fromEmbeddings(h)

	fail := func(principal auth.Principal, err error) {
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, "", "", 0, err, 1, nil, start)
	}

	// 1. Authentication before anything else — and before any body read — so
	// invalid, expired, and revoked keys receive their 401 without driving
	// any bounded parse work.
	principal, err := authenticate(r, deps)
	if err != nil {
		fail(auth.Principal{}, err)
		return
	}

	// 2. Bounded decoding. Unknown-field policy follows Chat Completions
	// (ignore): the endpoint is OpenAI chat-family shaped. Chat/responses-only
	// fields are rejected explicitly so misuse never degrades into a silent
	// embeddings call.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBody))
	if err != nil {
		fail(principal, errValidation)
		return
	}
	var req embeddingsWireRequest
	if err := json.Unmarshal(body, &req); err != nil {
		fail(principal, errValidation)
		return
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(body, &rawFields); err != nil {
		fail(principal, errValidation)
		return
	}
	if err := validateChatOnlyFields(rawFields); err != nil {
		fail(principal, err)
		return
	}
	input, err := req.decodeInput(h.MaxItems, h.MaxChars)
	if err != nil {
		fail(principal, err)
		return
	}

	// 3. Default-model backfill: a request without `model` resolves the
	// subject's default_embedding_model before model resolution; neither a
	// configured default nor an explicit model is a stable 400.
	publicModel, err := resolvePublicModel(deps, protocolEmbeddings, principal.SubjectID, req.Model)
	if err != nil {
		fail(principal, err)
		return
	}

	// 4. Shared admission pipeline: model/policy -> capability precheck
	// ("embeddings") -> input ceilings -> rate limit -> quota. The admission
	// request carries only protocol-neutral signals (input size), with each
	// input as its own item so the deterministic chars/4 estimate matches the
	// embeddings request exactly.
	mreq := model.Request{PublicModel: publicModel, RequestID: requestID}
	for _, s := range input {
		mreq.Input = append(mreq.Input, model.InputItem{Role: model.RoleUser, Text: s})
	}
	adm, auditErr := admit(w, r, deps, protocolEmbeddings, publicModel, &mreq, principal)
	if auditErr != nil {
		fail(principal, auditErr)
		return
	}
	defer adm.release()

	ereq := model.EmbeddingsRequest{
		PublicModel: publicModel,
		RequestID:   requestID,
		Input:       input,
	}
	resp, providerName, err := h.Service.Embeddings(r.Context(), adm.plan, ereq)
	if err != nil {
		// No billable response: refund the reservation idempotently.
		adm.qres.Release()
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, publicModel, providerName, 0, err, adm.attempts(), nil, start)
		return
	}
	// Settle exactly once to the reported input-token total; unknown usage
	// keeps the conservative reservation and is never fabricated as zero.
	adm.qres.Settle(usageTotal(resp.Usage))

	// Dimension gate: a response vector whose width differs from the
	// catalog-declared embedding_dim is a gateway configuration error. It
	// fails loud (500-class, nothing returned) — the upstream did the work,
	// so the reported usage still settles — and never leaks vector content.
	dimErr := error(nil)
	if caps, ok := h.Service.Capabilities(publicModel); ok {
		dimErr = model.CheckEmbeddingDim(caps.EmbeddingDim, resp)
	}
	if dimErr != nil {
		mapError(w, requestID, dimErr)
		h.record(deps, requestID, traceID, principal, publicModel, providerName, http.StatusInternalServerError, dimErr, adm.attempts(), resp.Usage, start)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	// The public envelope echoes the public model name; upstream names never
	// cross this boundary.
	resp.Model = publicModel
	_ = json.NewEncoder(w).Encode(encodeEmbeddings(resp))
	h.record(deps, requestID, traceID, principal, publicModel, providerName, http.StatusOK, nil, adm.attempts(), resp.Usage, start)
}

// auditEvent assembles the metadata-only audit record for one embeddings
// request. Vector and input content never appear here.
func (h *EmbeddingsHandler) auditEvent(requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, start time.Time) audit.Event {
	return audit.Event{
		RequestID: requestID, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
		Model: modelName, Provider: providerName, Status: status,
		ErrorClass: classifyErr(err), LatencyMillis: time.Since(start).Milliseconds(),
		PromptTokens: usageTokens(usage, true), CompletionTokens: usageTokens(usage, false),
		Streaming: false, CreatedAt: start, TraceID: traceID,
		RouteAttempts: routeAttempts, Protocol: protocolEmbeddings,
	}
}

func (h *EmbeddingsHandler) record(deps admissionDeps, requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, start time.Time) {
	recordRequest(deps, h.auditEvent(requestID, traceID, principal, modelName, providerName, status, err, routeAttempts, usage, start), usage)
}
