package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// errValidation aliases the domain validation sentinel so both handler-level
// and domain-level validation failures map to the same 400-class envelope.
var errValidation = model.ErrValidation

// chatRequest is the accepted OpenAI-compatible request subset. The V1.2
// additions (tools, tool_choice, response_format) are additive: every
// pre-V1.2 field keeps its exact semantics.
type chatRequest struct {
	Model          string            `json:"model"`
	Messages       []chatWireMessage `json:"messages"`
	Temperature    *float64          `json:"temperature"`
	MaxTokens      *int              `json:"max_tokens"`
	Stream         bool              `json:"stream"`
	Metadata       json.RawMessage   `json:"metadata"`
	Tools          []chatWireTool    `json:"tools"`
	ToolChoice     string            `json:"tool_choice"`
	ResponseFormat *chatWireFormat   `json:"response_format"`
}

// chatWireMessage decodes messages including the tool-calling extensions.
// Plain text messages decode exactly as before.
type chatWireMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content"`
	ToolCallID string           `json:"tool_call_id"`
	ToolCalls  []chatToolCallIn `json:"tool_calls"`
}

type chatToolCallIn struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type chatWireTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

type chatWireFormat struct {
	Type       string `json:"type"`
	JSONSchema struct {
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"schema"`
		Strict bool            `json:"strict"`
	} `json:"json_schema"`
}

// toDomain translates the chat wire request into the domain request.
func (req *chatRequest) toDomain(requestID string) (model.Request, error) {
	mreq := model.Request{
		PublicModel: req.Model,
		Stream:      req.Stream,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
		RequestID:   requestID,
		Metadata:    req.Metadata,
	}
	switch req.ToolChoice {
	case "", "auto", "none", "required":
		mreq.ToolChoice = req.ToolChoice
	default:
		return mreq, fmt.Errorf("%w: tool_choice must be auto, none, or required", errValidation)
	}
	for _, m := range req.Messages {
		switch {
		case len(m.ToolCalls) > 0:
			for _, tc := range m.ToolCalls {
				if tc.Type != "" && tc.Type != "function" {
					return mreq, fmt.Errorf("%w: unsupported tool call type", errValidation)
				}
				mreq.Input = append(mreq.Input, model.InputItem{
					Role: m.Role,
					ToolCall: &model.ToolCall{
						ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
					},
				})
			}
			if m.Content != "" {
				mreq.Input = append(mreq.Input, model.InputItem{Role: m.Role, Text: m.Content})
			}
		case m.ToolCallID != "":
			mreq.Input = append(mreq.Input, model.InputItem{
				Role:       model.RoleTool,
				ToolResult: &model.ToolResult{CallID: m.ToolCallID, Content: m.Content},
			})
		default:
			mreq.Input = append(mreq.Input, model.InputItem{Role: m.Role, Text: m.Content})
		}
	}
	for _, t := range req.Tools {
		if t.Type != "" && t.Type != "function" {
			return mreq, fmt.Errorf("%w: unsupported tool type", errValidation)
		}
		mreq.Tools = append(mreq.Tools, model.ToolDefinition{
			Name: t.Function.Name, Description: t.Function.Description, Parameters: t.Function.Parameters,
		})
	}
	if req.ResponseFormat != nil {
		spec := &model.ResponseSpec{Mode: model.ResponseMode(req.ResponseFormat.Type)}
		switch spec.Mode {
		case model.ModeJSON:
		case model.ModeJSONSchema:
			spec.Name = req.ResponseFormat.JSONSchema.Name
			spec.Schema = req.ResponseFormat.JSONSchema.Schema
			spec.Strict = req.ResponseFormat.JSONSchema.Strict
		default:
			return mreq, fmt.Errorf("%w: unsupported response_format type", errValidation)
		}
		mreq.ResponseSpec = spec
	}
	return mreq, nil
}

// Authenticator is the bearer-key resolution boundary; both the in-memory
// auth.Store and the PostgreSQL-backed authenticator satisfy it.
type Authenticator interface {
	Authenticate(key string, now time.Time) (auth.Principal, error)
}

// ChatHandler serves POST /v1/chat/completions.
type ChatHandler struct {
	Auth       Authenticator
	Service    *gateway.Service
	Policy     *policy.Policy
	Limiter    limiter.Gate
	Quota      quota.Gate
	Accounting *accounting.Gate
	Audit      audit.Sink
	Metrics    *metrics.Registry
	MaxBody    int64
	MaxMsgs    int
	MaxChars   int
}

func (h *ChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	traceID := traceIDOf(r, requestID)
	w.Header().Set("X-Trace-ID", traceID)
	deps := fromChat(h)

	fail := func(principal auth.Principal, err error) {
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, "", "", 0, err, 1, nil, false, start, nil)
	}

	// 1. Authentication before anything else — and before any body read — so
	// invalid, expired, and revoked keys receive their 401 without driving
	// any bounded parse work.
	principal, err := authenticate(r, deps)
	if err != nil {
		fail(auth.Principal{}, err)
		return
	}

	// 2. Bounded request decoding and validation.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBody))
	if err != nil {
		fail(principal, errValidation)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		fail(principal, errValidation)
		return
	}
	if err := validate(&req, h.MaxMsgs, h.MaxChars); err != nil {
		fail(principal, err)
		return
	}

	// 3. Default-model backfill: a request without `model` resolves the
	// subject's default_model before model resolution; an explicit model
	// keeps the current behavior, and neither default nor model is a stable
	// 400 invalid_request.
	publicModel, err := resolvePublicModel(deps, protocolChat, principal.SubjectID, req.Model)
	if err != nil {
		fail(principal, err)
		return
	}
	mreq, err := req.toDomain(requestID)
	if err != nil {
		fail(principal, err)
		return
	}

	// 4. Shared admission pipeline: model/policy -> capability precheck ->
	// clamps -> rate limit -> quota.
	adm, auditErr := admit(w, r, deps, protocolChat, publicModel, &mreq, principal)
	if auditErr != nil {
		fail(principal, auditErr)
		return
	}
	defer adm.release()

	mreq.Model = adm.plan.Primary()
	mreq.PublicModel = publicModel
	if !req.Stream {
		h.complete(w, r, deps, requestID, traceID, principal, adm, publicModel, mreq, start)
		return
	}
	h.stream(w, r, deps, requestID, traceID, principal, adm, publicModel, mreq, start)
}

// auditEvent assembles the metadata-only audit record. firstTokenMillis is
// the stream time-to-first-output measurement; non-streaming requests pass
// nil (their full-latency equivalent is LatencyMillis).
func (h *ChatHandler) auditEvent(requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time, firstTokenMillis *int64) audit.Event {
	return audit.Event{
		RequestID: requestID, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
		Model: modelName, Provider: providerName, Status: status,
		ErrorClass: classifyErr(err), LatencyMillis: time.Since(start).Milliseconds(),
		PromptTokens: usageTokens(usage, true), CompletionTokens: usageTokens(usage, false),
		FirstTokenMillis: firstTokenMillis,
		Streaming:        streaming, CreatedAt: start, TraceID: traceID,
		RouteAttempts: routeAttempts, Protocol: protocolChat,
	}
}

func (h *ChatHandler) record(deps admissionDeps, requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time, firstTokenMillis *int64) {
	recordRequest(deps, h.auditEvent(requestID, traceID, principal, modelName, providerName, status, err, routeAttempts, usage, streaming, start, firstTokenMillis), usage)
}

func (h *ChatHandler) complete(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	resp, providerName, err := h.Service.Complete(r.Context(), adm.plan, preq)
	if err != nil {
		// No billable response: refund the reservations idempotently.
		adm.refund()
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, modelName, providerName, 0, err, adm.attempts(), nil, false, start, nil)
		return
	}
	// Settle exactly once to the reported total; unknown usage keeps the
	// conservative reservation and is never fabricated as zero.
	adm.settle(providerName, resp.Usage)

	// Final output validation: failures are recorded in audit (never marked
	// successful silently) but the transport still delivers the payload.
	verr := model.ValidateOutput(preq, resp)
	auditErr := error(nil)
	if verr != nil {
		auditErr = verr
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(encodeChatCompletion(resp))
	h.record(deps, requestID, traceID, principal, modelName, providerName, http.StatusOK, auditErr, adm.attempts(), resp.Usage, false, start, nil)
}

func (h *ChatHandler) stream(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		// No provider call will happen; refund the reservations.
		adm.refund()
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "streaming_unsupported", "streaming is not supported by this connection")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	enc := newChatStreamEncoder(w, flusher)
	providerName, err := h.Service.Stream(r.Context(), adm.plan, preq, enc.Handle)
	auditErr := error(nil)
	var usage *model.Usage
	if err == nil {
		final := enc.FinalResponse()
		usage = final.Usage
		// Settle exactly once to the reported stream total (idempotent
		// finalize); unknown usage keeps the conservative reservation —
		// Settle with unknown usage is a deliberate no-op.
		adm.settle(providerName, usage)
		// Final output validation: invalid streamed tool arguments or
		// structured output are recorded in audit, never silently marked
		// successful. The V1 transport has no post-output failure event, so
		// the delivered chunks keep their [DONE] terminator.
		if verr := model.ValidateOutput(preq, final); verr != nil {
			auditErr = verr
		}
		// Terminal event only on normal completion.
		if _, werr := io.WriteString(w, "data: [DONE]\n\n"); werr == nil {
			flusher.Flush()
		}
	} else if !enc.SawOutput() {
		// Failed before any output: nothing billable, refund idempotently.
		// Failures after output started keep the conservative reservation
		// because the consumed usage is unknown and never fabricated.
		adm.refund()
	}
	recordErr := err
	if recordErr == nil {
		recordErr = auditErr
	}
	h.record(deps, requestID, traceID, principal, modelName, providerName, statusFor(err, enc.SawOutput()), recordErr, adm.attempts(), usage, true, start, enc.FirstTokenMillis(start))
	if err != nil && !enc.SawOutput() {
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

func validate(req *chatRequest, maxMsgs, maxChars int) error {
	// The model-required check lives in the default-model backfill
	// (resolvePublicModel): requests without `model` first try the subject's
	// configured default, and only fail when neither is configured.
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

func requestIDOf(r *http.Request) string {
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" || len(requestID) > 128 {
		return newRequestID()
	}
	return requestID
}

func traceIDOf(r *http.Request, requestID string) string {
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		return requestID // request ID doubles as the trace root
	}
	return traceID
}

var requestCounter atomic.Int64

func newRequestID() string {
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), requestCounter.Add(1))
}
