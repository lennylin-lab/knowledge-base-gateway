package httpapi

// POST /v1/responses: the V1.2 Responses-compatible protocol. Requests are
// bounded, translated to the shared domain model, and served by the same
// routing/quota/audit pipeline as Chat Completions. Only documented MVP
// fields are accepted (unknown fields are rejected), and responses contain
// only stable gateway types — never provider-private fields.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// ResponsesHandler serves POST /v1/responses.
type ResponsesHandler struct {
	Auth       Authenticator
	Service    *gateway.Service
	Policy     *policy.Policy
	Limiter    limiter.Gate
	Quota      quota.Gate
	Accounting *accounting.Gate
	Audit      audit.Sink
	Metrics    *metrics.Registry
	MaxBody    int64
	MaxItems   int
	MaxChars   int
	// Async enables background:true acceptance (V1.4). Nil keeps the frozen
	// synchronous-only behavior: background requests receive the stable 503
	// job_queue_unavailable (the documented rollback posture).
	Async *Async
}

// responsesWireTool accepts both function-tool dialects on /v1/responses:
// the flat Responses-native shape (`{"type":"function","name":...}`,
// what the openai SDK sends for its typed `tools` parameter) and the nested
// chat-completions shape (`{"type":"function","function":{...}}`, the
// gateway MVP dialect). Unknown fields are rejected inside either shape, so
// the accepted contract stays explicit.
type responsesWireTool struct {
	chatWireTool
}

func (t *responsesWireTool) UnmarshalJSON(data []byte) error {
	var nested chatWireTool
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&nested); err == nil {
		*t = responsesWireTool{chatWireTool: nested}
		return nil
	}
	var flat struct {
		Type        string          `json:"type"`
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&flat); err != nil {
		return fmt.Errorf("tool must be a function tool object (nested chat shape or flat Responses shape)")
	}
	t.Type = flat.Type
	t.Function.Name = flat.Name
	t.Function.Description = flat.Description
	t.Function.Parameters = flat.Parameters
	return nil
}

// responsesWireText is the Responses-native output configuration
// (`text.format`), the parameter the openai SDK uses for structured output
// and plain-text selection. The gateway MVP dialect (`response_format`)
// stays accepted; the two are mutually exclusive.
type responsesWireText struct {
	Format *responsesWireFormat `json:"format"`
}

// responsesWireFormat is the flat Responses format object: `{"type":"text"}`
// for plain text, or `json_object` / `json_schema` mirroring the nested MVP
// dialect's schema fields.
type responsesWireFormat struct {
	Type   string          `json:"type"`
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Strict bool            `json:"strict"`
}

// responsesRequest is the documented MVP request subset. Background (V1.4)
// is additive and optional; absent or false keeps the synchronous contract.
type responsesRequest struct {
	Model           string              `json:"model"`
	Input           json.RawMessage     `json:"input"`
	Instructions    string              `json:"instructions"`
	Temperature     *float64            `json:"temperature"`
	MaxOutputTokens *int                `json:"max_output_tokens"`
	Stream          bool                `json:"stream"`
	Tools           []responsesWireTool `json:"tools"`
	ToolChoice      string              `json:"tool_choice"`
	ResponseFormat  *chatWireFormat     `json:"response_format"`
	Text            *responsesWireText  `json:"text"`
	Metadata        json.RawMessage     `json:"metadata"`
	Background      *bool               `json:"background"`
}

// toDomain translates the Responses request into the domain request.
func (req *responsesRequest) toDomain(requestID string, maxItems, maxChars int) (model.Request, error) {
	mreq := model.Request{
		PublicModel:  req.Model,
		Stream:       req.Stream,
		Instructions: req.Instructions,
		Temperature:  req.Temperature,
		MaxTokens:    req.MaxOutputTokens,
		RequestID:    requestID,
		Metadata:     req.Metadata,
	}
	if req.Temperature != nil && (*req.Temperature < -2 || *req.Temperature > 2) {
		return mreq, fmt.Errorf("%w: temperature out of range", errValidation)
	}
	if req.MaxOutputTokens != nil && (*req.MaxOutputTokens <= 0 || *req.MaxOutputTokens > 1_000_000) {
		return mreq, fmt.Errorf("%w: max_output_tokens out of range", errValidation)
	}
	if len(req.Instructions) > maxChars {
		return mreq, fmt.Errorf("%w: instructions too long", errValidation)
	}
	if len(req.Metadata) > model.MaxMetadataBytes {
		return mreq, fmt.Errorf("%w: metadata too large", errValidation)
	}
	if len(req.Metadata) > 0 {
		var meta any
		if err := json.Unmarshal(req.Metadata, &meta); err != nil {
			return mreq, fmt.Errorf("%w: metadata must be a JSON object", errValidation)
		}
	}
	switch req.ToolChoice {
	case "", "auto", "none", "required":
		mreq.ToolChoice = req.ToolChoice
	default:
		return mreq, fmt.Errorf("%w: tool_choice must be auto, none, or required", errValidation)
	}
	for _, t := range req.Tools {
		if t.Type != "" && t.Type != "function" {
			return mreq, fmt.Errorf("%w: unsupported tool type", errValidation)
		}
		mreq.Tools = append(mreq.Tools, model.ToolDefinition{
			Name: t.Function.Name, Description: t.Function.Description, Parameters: t.Function.Parameters,
		})
	}
	if req.ResponseFormat != nil && req.Text != nil && req.Text.Format != nil {
		return mreq, fmt.Errorf("%w: text and response_format are mutually exclusive", errValidation)
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
	if req.Text != nil && req.Text.Format != nil {
		f := req.Text.Format
		switch model.ResponseMode(f.Type) {
		case model.ModeJSON:
			mreq.ResponseSpec = &model.ResponseSpec{Mode: model.ModeJSON}
		case model.ModeJSONSchema:
			mreq.ResponseSpec = &model.ResponseSpec{
				Mode: model.ModeJSONSchema, Name: f.Name, Schema: f.Schema, Strict: f.Strict,
			}
		case "text":
			// Plain-text selection: no output spec.
		default:
			return mreq, fmt.Errorf("%w: unsupported text format type", errValidation)
		}
	}

	// Input: a plain string or an array of typed items.
	var raw any
	if len(req.Input) == 0 {
		return mreq, fmt.Errorf("%w: input is required", errValidation)
	}
	if err := json.Unmarshal(req.Input, &raw); err != nil {
		return mreq, fmt.Errorf("%w: invalid input", errValidation)
	}
	switch v := raw.(type) {
	case string:
		if len(v) > maxChars {
			return mreq, fmt.Errorf("%w: input too long", errValidation)
		}
		mreq.Input = append(mreq.Input, model.InputItem{Role: model.RoleUser, Text: v})
	case []any:
		if len(v) > maxItems {
			return mreq, fmt.Errorf("%w: too many input items", errValidation)
		}
		for i, item := range v {
			ii, err := decodeInputItem(item, maxChars)
			if err != nil {
				return mreq, fmt.Errorf("%w: input[%d]: %v", errValidation, i, err)
			}
			mreq.Input = append(mreq.Input, ii...)
		}
	default:
		return mreq, fmt.Errorf("%w: input must be a string or an array", errValidation)
	}
	if len(mreq.Input) == 0 {
		return mreq, fmt.Errorf("%w: input must not be empty", errValidation)
	}
	return mreq, nil
}

// decodeInputItem decodes one Responses input item into zero or more domain
// input items (a message with content parts can yield several).
func decodeInputItem(item any, maxChars int) ([]model.InputItem, error) {
	obj, ok := item.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("item must be an object")
	}
	typ, _ := obj["type"].(string)
	if typ == "" {
		typ = "message" // role/content shorthand
	}
	switch typ {
	case "message":
		role, _ := obj["role"].(string)
		switch role {
		case model.RoleSystem, model.RoleUser, model.RoleAssistant:
		default:
			return nil, fmt.Errorf("unsupported role %q", role)
		}
		var out []model.InputItem
		appendText := func(text string) error {
			if len(text) > maxChars {
				return fmt.Errorf("content too long")
			}
			out = append(out, model.InputItem{Role: role, Text: text})
			return nil
		}
		switch content := obj["content"].(type) {
		case string:
			if err := appendText(content); err != nil {
				return nil, err
			}
		case []any:
			for _, part := range content {
				p, ok := part.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("content part must be an object")
				}
				pt, _ := p["type"].(string)
				switch pt {
				case "input_text", "output_text":
					text, _ := p["text"].(string)
					if err := appendText(text); err != nil {
						return nil, err
					}
				case "refusal":
					text, _ := p["refusal"].(string)
					if err := appendText(text); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unsupported content part type %q", pt)
				}
			}
		default:
			return nil, fmt.Errorf("content must be a string or an array")
		}
		return out, nil
	case "function_call":
		name, _ := obj["name"].(string)
		callID, _ := obj["call_id"].(string)
		args, _ := obj["arguments"].(string)
		if name == "" {
			return nil, fmt.Errorf("function_call requires a name")
		}
		return []model.InputItem{{ToolCall: &model.ToolCall{ID: callID, Name: name, Arguments: args}}}, nil
	case "function_call_output":
		callID, _ := obj["call_id"].(string)
		output, _ := obj["output"].(string)
		if callID == "" {
			return nil, fmt.Errorf("function_call_output requires call_id")
		}
		if len(output) > maxChars {
			return nil, fmt.Errorf("output too long")
		}
		return []model.InputItem{{ToolResult: &model.ToolResult{CallID: callID, Content: output}}}, nil
	default:
		return nil, fmt.Errorf("unsupported input item type %q", typ)
	}
}

func (h *ResponsesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sw, r, span := startHTTPSpan(w, r, protocolResponses)
	w = sw // every later write flows through the recording writer so the span carries the real final status
	defer finishHTTPSpan(sw, span)
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	traceID := traceIDOf(r, requestID)
	w.Header().Set("X-Trace-ID", traceID)
	deps := fromResponses(h)

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

	// 2. Bounded decoding; unknown fields are rejected so the MVP contract
	// stays explicit and forward-breaking fields cannot slip through silently.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBody))
	if err != nil {
		fail(principal, errValidation)
		return
	}
	var req responsesRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		fail(principal, errValidation)
		return
	}
	// Exactly one JSON document: a second concatenated document or any
	// non-whitespace trailing bytes is rejected so a smuggled tail can never
	// change request semantics.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		fail(principal, errValidation)
		return
	}

	// Default-model backfill: a request without `model` resolves the
	// subject's default_model before model resolution; an explicit model
	// keeps the current behavior, and neither default nor model is a stable
	// 400 invalid_request.
	publicModel, err := resolvePublicModel(deps, protocolResponses, principal.SubjectID, req.Model)
	if err != nil {
		fail(principal, err)
		return
	}
	mreq, err := req.toDomain(requestID, h.MaxItems, h.MaxChars)
	if err != nil {
		fail(principal, err)
		return
	}

	// Background requests never stream: a queued job has no live connection
	// to stream to, so the combination is a stable 400 before admission.
	if req.Background != nil && *req.Background && req.Stream {
		err := fmt.Errorf("%w: background requests do not support streaming", errValidation)
		fail(principal, err)
		return
	}

	// 3. Shared admission pipeline: model/policy -> capability precheck ->
	// clamps -> rate limit -> quota. Background requests reuse it verbatim;
	// the reservation it holds is a creation-time gate only.
	adm, auditErr := admit(w, r, deps, protocolResponses, publicModel, &mreq, principal)
	if auditErr != nil {
		fail(principal, auditErr)
		return
	}
	if req.Background != nil && *req.Background {
		h.createBackground(w, r, deps, requestID, traceID, principal, adm, publicModel, mreq, start)
		return
	}
	defer adm.release()

	mreq.Model = adm.plan.Primary()
	if !req.Stream {
		h.complete(w, r, deps, requestID, traceID, principal, adm, publicModel, mreq, start)
		return
	}
	h.stream(w, r, deps, requestID, traceID, principal, adm, publicModel, mreq, start)
}

// auditEvent assembles the metadata-only audit record. firstTokenMillis is
// the stream time-to-first-output measurement; non-streaming requests pass
// nil (their full-latency equivalent is LatencyMillis).
func (h *ResponsesHandler) auditEvent(requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time, firstTokenMillis *int64) audit.Event {
	return audit.Event{
		RequestID: requestID, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
		Model: modelName, Provider: providerName, Status: status,
		ErrorClass: classifyErr(err), LatencyMillis: time.Since(start).Milliseconds(),
		PromptTokens: usageTokens(usage, true), CompletionTokens: usageTokens(usage, false),
		FirstTokenMillis: firstTokenMillis,
		Streaming:        streaming, CreatedAt: start, TraceID: traceID,
		RouteAttempts: routeAttempts, Protocol: protocolResponses,
	}
}

func (h *ResponsesHandler) record(deps admissionDeps, requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time, firstTokenMillis *int64) {
	recordRequest(deps, h.auditEvent(requestID, traceID, principal, modelName, providerName, status, err, routeAttempts, usage, streaming, start, firstTokenMillis), usage)
}

func (h *ResponsesHandler) complete(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	resp, providerName, err := h.Service.Complete(r.Context(), adm.plan, preq)
	if err != nil {
		adm.refund()
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, modelName, providerName, 0, err, adm.attempts(), nil, false, start, nil)
		return
	}
	adm.settle(providerName, resp.Usage)

	// Output validation failures are recorded in audit, never silently
	// treated as success.
	auditErr := model.ValidateOutput(preq, resp)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(encodeResponse(publicResponseID(resp, requestID), modelName, resp, preq.Metadata))
	h.record(deps, requestID, traceID, principal, modelName, providerName, http.StatusOK, auditErr, adm.attempts(), resp.Usage, false, start, nil)
}

func (h *ResponsesHandler) stream(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		adm.refund()
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "streaming_unsupported", "streaming is not supported by this connection")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	enc := newResponsesStreamEncoder(w, flusher, modelName, requestID, preq.Metadata, preq)
	providerName, err := h.Service.Stream(r.Context(), adm.plan, preq, enc.Handle)
	// Snapshot output-before-failure: the unified failure event itself does
	// not count as billable output.
	outputBeforeFailure := enc.SawOutput()
	var usage *model.Usage
	if err == nil {
		streamed := enc.Response()
		usage = streamed.Usage
		// Settle exactly once to the reported stream total (idempotent
		// finalize); unknown usage keeps the conservative reservation.
		adm.settle(providerName, usage)
	} else {
		// Unified failure event: emitted as the first event when nothing was
		// sent yet, or as a terminal event when the stream truncated.
		_ = enc.WriteFailed(errorType(err))
		if !outputBeforeFailure {
			// Nothing reached the client before the failure: refund the
			// reservations idempotently. Truncations after output keep the
			// conservative reservation; the consumed usage is unknown and
			// never fabricated.
			adm.refund()
		}
	}
	// Audit the recorded output-validation failure rather than the Stream
	// error: built-in adapters wrap emit errors as internal transport
	// failures, so the ErrOutputValidation sentinel never crosses the
	// provider boundary.
	auditErr := err
	if verr := enc.ValidationErr(); verr != nil {
		auditErr = verr
	}
	h.record(deps, requestID, traceID, principal, modelName, providerName, statusFor(err, outputBeforeFailure), auditErr, adm.attempts(), usage, true, start, enc.FirstTokenMillis(start))
}

// publicResponseID derives the gateway-owned response identifier. The
// upstream response ID is provider-private and never surfaces here.
func publicResponseID(_ model.Response, requestID string) string {
	return "resp_" + requestID
}
