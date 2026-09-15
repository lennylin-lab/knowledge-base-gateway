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

// responsesRequest is the documented MVP request subset.
type responsesRequest struct {
	Model           string          `json:"model"`
	Input           json.RawMessage `json:"input"`
	Instructions    string          `json:"instructions"`
	Temperature     *float64        `json:"temperature"`
	MaxOutputTokens *int            `json:"max_output_tokens"`
	Stream          bool            `json:"stream"`
	Tools           []chatWireTool  `json:"tools"`
	ToolChoice      string          `json:"tool_choice"`
	ResponseFormat  *chatWireFormat `json:"response_format"`
	Metadata        json.RawMessage `json:"metadata"`
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
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	traceID := traceIDOf(r, requestID)
	w.Header().Set("X-Trace-ID", traceID)
	deps := fromResponses(h)

	fail := func(principal auth.Principal, err error) {
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, "", "", 0, err, 1, nil, false, start)
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
	if req.Model == "" {
		fail(principal, fmt.Errorf("%w: model is required", errValidation))
		return
	}
	mreq, err := req.toDomain(requestID, h.MaxItems, h.MaxChars)
	if err != nil {
		fail(principal, err)
		return
	}

	// 3. Shared admission pipeline: model/policy -> capability precheck ->
	// clamps -> rate limit -> quota.
	adm, auditErr := admit(w, r, deps, "responses", req.Model, &mreq, principal)
	if auditErr != nil {
		fail(principal, auditErr)
		return
	}
	defer adm.release()

	mreq.Model = adm.plan.Primary()
	if !req.Stream {
		h.complete(w, r, deps, requestID, traceID, principal, adm, req.Model, mreq, start)
		return
	}
	h.stream(w, r, deps, requestID, traceID, principal, adm, req.Model, mreq, start)
}

func (h *ResponsesHandler) auditEvent(requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time) audit.Event {
	return audit.Event{
		RequestID: requestID, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
		Model: modelName, Provider: providerName, Status: status,
		ErrorClass: classifyErr(err), LatencyMillis: time.Since(start).Milliseconds(),
		PromptTokens: usageTokens(usage, true), CompletionTokens: usageTokens(usage, false),
		Streaming: streaming, CreatedAt: start, TraceID: traceID,
		RouteAttempts: routeAttempts, Protocol: "responses",
	}
}

func (h *ResponsesHandler) record(deps admissionDeps, requestID, traceID string, principal auth.Principal, modelName, providerName string, status int, err error, routeAttempts int, usage *model.Usage, streaming bool, start time.Time) {
	recordRequest(deps, h.auditEvent(requestID, traceID, principal, modelName, providerName, status, err, routeAttempts, usage, streaming, start), usage)
}

func (h *ResponsesHandler) complete(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	resp, providerName, err := h.Service.Complete(r.Context(), adm.plan, preq)
	if err != nil {
		adm.qres.Release()
		mapError(w, requestID, err)
		h.record(deps, requestID, traceID, principal, modelName, providerName, 0, err, adm.attempts(), nil, false, start)
		return
	}
	adm.qres.Settle(usageTotal(resp.Usage))

	// Output validation failures are recorded in audit, never silently
	// treated as success.
	auditErr := model.ValidateOutput(preq, resp)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(encodeResponse(publicResponseID(resp, requestID), modelName, resp, preq.Metadata))
	h.record(deps, requestID, traceID, principal, modelName, providerName, http.StatusOK, auditErr, adm.attempts(), resp.Usage, false, start)
}

func (h *ResponsesHandler) stream(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, modelName string, preq model.Request, start time.Time) {
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		adm.qres.Release()
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
	if err != nil {
		// Unified failure event: emitted as the first event when nothing was
		// sent yet, or as a terminal event when the stream truncated.
		_ = enc.WriteFailed(errorType(err))
		if !outputBeforeFailure {
			// Nothing reached the client before the failure: refund the
			// reservation idempotently.
			adm.qres.Release()
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
	streamed := enc.Response()
	h.record(deps, requestID, traceID, principal, modelName, providerName, statusFor(err, outputBeforeFailure), auditErr, adm.attempts(), streamed.Usage, true, start)
}

// publicResponseID derives the gateway-owned response identifier. The
// upstream response ID is provider-private and never surfaces here.
func publicResponseID(_ model.Response, requestID string) string {
	return "resp_" + requestID
}
