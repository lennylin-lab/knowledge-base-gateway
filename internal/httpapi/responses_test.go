package httpapi

// Tests for POST /v1/responses: the documented MVP contract (A2), streaming
// events (A3), capability enforcement (A4), tool safety (A5), and structured
// output validation (A6).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// fullCaps is the declared capability matrix for the responses test catalog.
var fullCaps = model.Capabilities{
	Chat: true, Responses: true, Stream: true, Tools: true,
	StructuredOutput: true, JSONMode: true, Usage: true,
	ContextTokens: 8192, MaxOutputTokens: 2048, MaxTools: 8,
}

// textOnlyCaps declares a model that cannot do tools or structured output.
var textOnlyCaps = model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}

// noResponsesCaps declares a chat-only model.
var noResponsesCaps = model.Capabilities{Chat: true, Stream: true, Usage: true}

// callCounter counts provider invocations to prove denials never reach one.
type callCounter struct {
	inner provider.Provider
	calls *int
}

func (c *callCounter) Name() string { return c.inner.Name() }

func (c *callCounter) Capabilities(m string) model.Capabilities { return c.inner.Capabilities(m) }

func (c *callCounter) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	*c.calls++
	return c.inner.Complete(ctx, req)
}

func (c *callCounter) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	*c.calls++
	return c.inner.Stream(ctx, req, emit)
}

type responsesFixture struct {
	h     *ResponsesHandler
	sink  *audit.MemorySink
	calls *int
}

func newResponsesFixture(t *testing.T, caps model.Capabilities, p provider.Provider) *responsesFixture {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-r", Subject: "subject-r", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: p.Name(), UpstreamModel: "upstream-full", Enabled: true, Capabilities: caps, ConfigVersion: 3},
	})
	pol := policy.New()
	pol.Allow("subject-r", "full-model")
	calls := 0
	counting := &callCounter{inner: p, calls: &calls}
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): counting}, 2*time.Second, 0)
	svc.RetryWait = time.Millisecond
	sink := audit.NewMemorySink(nil)
	h := &ResponsesHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100),
		Audit:   sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
	}
	return &responsesFixture{h: h, sink: sink, calls: &calls}
}

func doResponses(t *testing.T, f *responsesFixture, body, key, requestID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if requestID != "" {
		req.Header.Set("X-Request-ID", requestID)
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec
}

const responsesEchoBody = `{"model":"full-model","input":"hello"}`

// --- A2: non-streaming contract -------------------------------------------

func TestResponsesNonStreamingContract(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, responsesEchoBody, testKey, "req-resp-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID == "" || resp.Object != "response" || resp.Created == 0 {
		t.Errorf("missing stable envelope fields: %+v", resp)
	}
	if resp.Model != "full-model" {
		t.Errorf("model must be the public name, got %q", resp.Model)
	}
	if resp.Status != model.StatusCompleted {
		t.Errorf("status = %q", resp.Status)
	}
	if len(resp.Output) != 1 || resp.Output[0].Type != "message" ||
		resp.Output[0].Content[0].Type != "output_text" ||
		resp.Output[0].Content[0].Text != "echo: hello" {
		t.Errorf("output wrong: %+v", resp.Output)
	}
	if resp.Usage == nil || resp.Usage.InputTokens != 10 || resp.Usage.TotalTokens != 21 {
		t.Errorf("usage wrong: %+v", resp.Usage)
	}
	if resp.Error != nil {
		t.Errorf("error must be null on success: %+v", resp.Error)
	}
	// No provider-private fields leak.
	for _, leaked := range []string{"chatcmpl", "system_fingerprint", "logprobs"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Errorf("response leaks provider field %q", leaked)
		}
	}
	if rec.Header().Get("X-Request-ID") != "req-resp-1" {
		t.Error("X-Request-ID must be echoed")
	}
	ev := f.sink.Snapshot()
	if len(ev) != 1 || ev[0].Protocol != "responses" || ev[0].Status != 200 {
		t.Errorf("audit wrong: %+v", ev)
	}
}

func TestResponsesRejectsUnknownFields(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, `{"model":"full-model","input":"hi","truncation":"auto"}`, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field must be rejected, got %d: %s", rec.Code, rec.Body.String())
	}
	if *f.calls != 0 {
		t.Errorf("rejected request reached the provider")
	}
}

func TestResponsesInputShapes(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	cases := []struct {
		name string
		body string
		want string // expected response text prefix
	}{
		{"string input", `{"model":"full-model","input":"hi"}`, "echo: hi"},
		{"message items", `{"model":"full-model","input":[{"type":"message","role":"user","content":"hi"}]}`, "echo: hi"},
		{"content parts", `{"model":"full-model","input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]}]}`, "echo: hi"},
		{"instructions", `{"model":"full-model","instructions":"be brief","input":"hi"}`, "echo: hi"},
	}
	for _, tc := range cases {
		rec := doResponses(t, f, tc.body, testKey, "")
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d body = %s", tc.name, rec.Code, rec.Body.String())
			continue
		}
		var resp responseObject
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if len(resp.Output) == 0 || resp.Output[0].Content[0].Text != tc.want {
			t.Errorf("%s: output = %+v, want text %q", tc.name, resp.Output, tc.want)
		}
	}
}

func TestResponsesValidationBounds(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	cases := map[string]string{
		"missing input":     `{"model":"full-model"}`,
		"bad input type":    `{"model":"full-model","input":42}`,
		"bad role":          `{"model":"full-model","input":[{"type":"message","role":"wizard","content":"x"}]}`,
		"bad content part":  `{"model":"full-model","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://x"}]}]}`,
		"bad temperature":   `{"model":"full-model","input":"x","temperature":99}`,
		"bad max tokens":    `{"model":"full-model","input":"x","max_output_tokens":-3}`,
		"unknown tool type": `{"model":"full-model","input":"x","tools":[{"type":"web_search"}]}`,
		"bad tool_choice":   `{"model":"full-model","input":"x","tool_choice":"first"}`,
		"bad format":        `{"model":"full-model","input":"x","response_format":{"type":"xml"}}`,
		"empty input array": `{"model":"full-model","input":[]}`,
	}
	for name, body := range cases {
		rec := doResponses(t, f, body, testKey, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d want 400: %s", name, rec.Code, rec.Body.String())
		}
	}
	if *f.calls != 0 {
		t.Errorf("invalid requests reached the provider")
	}
}

// TestResponsesAcceptsOpenAISDKNativeShapes pins the openai-python SDK
// compatibility pass: the SDK's typed parameters use the flat Responses
// function-tool shape and the `text.format` output configuration, so strict
// decoding must accept both alongside the gateway MVP dialects. The narrowing
// is bounded: unknown fields, unknown format types, and the combined
// text+response_format form stay rejected before any provider invocation
// (single-document decoding is pinned by TestResponsesSingleDocumentDecode).
func TestResponsesAcceptsOpenAISDKNativeShapes(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})

	// Flat Responses-native function tool (the SDK `tools` parameter): must
	// reach the provider and produce the same function_call output as the
	// nested MVP shape.
	flatToolDecl := `{"type":"function","name":"get_weather","description":"Look up weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}`
	rec := doResponses(t, f, `{"model":"full-model","input":"paris?","tools":[`+flatToolDecl+`]}`, testKey, "req-sdk-tool")
	if rec.Code != http.StatusOK {
		t.Fatalf("flat tool: status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Output) != 1 || resp.Output[0].Type != "function_call" || resp.Output[0].Name != "get_weather" {
		t.Fatalf("flat tool output = %+v", resp.Output)
	}
	callID := resp.Output[0].CallID

	// Round trip with the flat declaration, exactly as the SDK sends it.
	roundTrip := `{"model":"full-model","input":[{"role":"user","content":"paris?"},{"type":"function_call","call_id":"` + callID + `","name":"get_weather","arguments":"{\"city\":\"paris\"}"},{"type":"function_call_output","call_id":"` + callID + `","output":"sunny 22C"}],"tools":[` + flatToolDecl + `]}`
	rec = doResponses(t, f, roundTrip, testKey, "req-sdk-tool-rt")
	if rec.Code != http.StatusOK {
		t.Fatalf("flat tool round trip: status = %d body = %s", rec.Code, rec.Body.String())
	}

	// text.format json_schema (the SDK `text` parameter): structured output
	// with the same validation as the response_format dialect.
	schema := `{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"],"additionalProperties":false}`
	rec = doResponses(t, f, `{"model":"full-model","input":"hello","text":{"format":{"type":"json_schema","name":"echo_answer","strict":true,"schema":`+schema+`}}}`, testKey, "req-sdk-schema")
	if rec.Code != http.StatusOK {
		t.Fatalf("text.format json_schema: status = %d body = %s", rec.Code, rec.Body.String())
	}
	var structured responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &structured); err != nil {
		t.Fatal(err)
	}
	if len(structured.Output) == 0 || structured.Output[0].Content[0].Text != `{"echo":"hello"}` {
		t.Fatalf("structured output wrong: %+v", structured.Output)
	}

	// text.format plain text: explicit no-spec selection.
	rec = doResponses(t, f, `{"model":"full-model","input":"hello","text":{"format":{"type":"text"}}}`, testKey, "req-sdk-text")
	if rec.Code != http.StatusOK {
		t.Fatalf("text.format text: status = %d body = %s", rec.Code, rec.Body.String())
	}

	if *f.calls != 4 {
		t.Fatalf("expected 4 successful provider calls, got %d", *f.calls)
	}

	// The narrowing stops at standard SDK shapes: everything else stays a
	// 400 invalid_request that never reaches the provider.
	rejections := map[string]string{
		"unknown field in flat tool":  `{"model":"full-model","input":"x","tools":[{"type":"function","name":"w","strict":true}]}`,
		"unknown field inside text":   `{"model":"full-model","input":"x","text":{"verbosity":"low"}}`,
		"unknown text format type":    `{"model":"full-model","input":"x","text":{"format":{"type":"xml"}}}`,
		"text plus response_format":   `{"model":"full-model","input":"x","text":{"format":{"type":"text"}},"response_format":{"type":"json_object"}}`,
		"flat tool with empty name":   `{"model":"full-model","input":"x","tools":[{"type":"function","name":"","parameters":{"type":"object"}}]}`,
		"flat tool unknown tool type": `{"model":"full-model","input":"x","tools":[{"type":"web_search"}]}`,
	}
	before := *f.calls
	for name, body := range rejections {
		rec := doResponses(t, f, body, testKey, "req-sdk-reject")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "invalid_request") {
			t.Errorf("%s: envelope must be invalid_request: %s", name, rec.Body.String())
		}
	}
	if *f.calls != before {
		t.Errorf("rejected SDK-shape requests reached the provider (%d extra calls)", *f.calls-before)
	}
}

// --- A3: streaming ----------------------------------------------------------

type sseEvent struct {
	Event string
	Data  map[string]any
}

func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var out []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				ev.Event = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev.Data); err != nil {
					t.Fatalf("bad SSE data line %q: %v", line, err)
				}
			}
		}
		out = append(out, ev)
	}
	return out
}

func TestResponsesStreamingTextEvents(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, `{"model":"full-model","input":"hello","stream":true}`, testKey, "req-resp-s1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	events := parseSSE(t, rec.Body.String())
	if len(events) == 0 || events[0].Event != "response.created" {
		t.Fatalf("first event must be response.created: %+v", events)
	}
	created := events[0].Data["response"].(map[string]any)
	if created["object"] != "response" || created["status"] != "in_progress" {
		t.Errorf("created response wrong: %v", created)
	}
	if events[len(events)-1].Event != "response.completed" {
		t.Fatalf("last event must be response.completed: %+v", events[len(events)-1])
	}
	deltas := 0
	var text strings.Builder
	for _, ev := range events[1 : len(events)-1] {
		switch ev.Event {
		case "response.output_text.delta":
			deltas++
			text.WriteString(ev.Data["delta"].(string))
		case "response.output_text.done":
			if ev.Data["text"] != text.String() {
				t.Errorf("done text %q != assembled %q", ev.Data["text"], text.String())
			}
		default:
			t.Errorf("unexpected event %q in text stream", ev.Event)
		}
	}
	if deltas < 2 || text.String() != "echo: hello" {
		t.Errorf("deltas = %d text = %q", deltas, text.String())
	}
	completed := events[len(events)-1].Data["response"].(map[string]any)
	if completed["status"] != model.StatusCompleted {
		t.Errorf("completed status = %v", completed["status"])
	}
	if ev := f.sink.Snapshot(); len(ev) != 1 || !ev[0].Streaming {
		t.Errorf("audit streaming flag wrong: %+v", ev)
	}
}

// failBeforeEventProvider fails before any event is emitted.
type failBeforeEventProvider struct{ inner provider.Provider }

func (p *failBeforeEventProvider) Name() string { return "fail-before" }

func (p *failBeforeEventProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *failBeforeEventProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return p.inner.Complete(ctx, req)
}

func (p *failBeforeEventProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return &provider.Error{Class: provider.ClassTimeout, Msg: "connect timeout"}
}

// dieAfterFirstEventProvider emits created then dies: no provider switch may
// rescue the stream and the failure event must terminate it.
type dieAfterFirstEventProvider struct {
	inner    provider.Provider
	uploaded *router.Routes
}

func (p *dieAfterFirstEventProvider) Name() string { return "die-after-first" }

func (p *dieAfterFirstEventProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *dieAfterFirstEventProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return p.inner.Complete(ctx, req)
}

func (p *dieAfterFirstEventProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	if err := emit(model.Event{Kind: model.EventCreated, Response: &model.Response{ID: "x", Status: "in_progress"}}); err != nil {
		return err
	}
	return &provider.Error{Class: provider.ClassNetwork, Msg: "stream broke"}
}

func TestResponsesStreamPreFirstEventFailure(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, &failBeforeEventProvider{inner: provider.Fake{}})
	rec := doResponses(t, f, `{"model":"full-model","input":"hello","stream":true}`, testKey, "req-resp-f1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	events := parseSSE(t, rec.Body.String())
	if len(events) != 1 || events[0].Event != "response.failed" {
		t.Fatalf("pre-first-event failure must emit exactly one response.failed: %+v", events)
	}
	resp := events[0].Data["response"].(map[string]any)
	if resp["status"] != "failed" {
		t.Errorf("failed status = %v", resp["status"])
	}
	errBody := resp["error"].(map[string]any)
	if errBody["code"] != "timeout_error" {
		t.Errorf("error code = %v", errBody["code"])
	}
	if ev := f.sink.Snapshot(); len(ev) != 1 || ev[0].Status != 0 || ev[0].ErrorClass != "timeout" {
		t.Errorf("audit wrong: %+v", ev)
	}
}

func TestResponsesStreamPostFirstEventFailure(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, &dieAfterFirstEventProvider{inner: provider.Fake{}})
	rec := doResponses(t, f, `{"model":"full-model","input":"hello","stream":true}`, testKey, "req-resp-f2")
	events := parseSSE(t, rec.Body.String())
	if len(events) < 2 {
		t.Fatalf("want created then failed, got %+v", events)
	}
	if events[0].Event != "response.created" || events[len(events)-1].Event != "response.failed" {
		t.Fatalf("event order wrong: %+v", events)
	}
	// Only one stream: the failure event must not be followed by completed.
	for _, ev := range events[1 : len(events)-1] {
		if ev.Event == "response.completed" {
			t.Fatal("truncated stream must not end completed")
		}
	}
}

func TestResponsesStreamClientCancel(t *testing.T) {
	slow := &slowStreamProvider{inner: provider.Fake{}}
	f := newResponsesFixture(t, fullCaps, slow)
	// Issue the request with a client that disconnects after the first read.
	srv := httptest.NewServer(f.h)
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses",
		strings.NewReader(`{"model":"full-model","input":"hello","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+testKey)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "response.created") {
		t.Fatalf("first event = %q", string(buf[:n]))
	}
	resp.Body.Close() // client hangs up mid-stream
	// The handler must finish promptly; give it a moment and assert the
	// provider observed cancellation instead of running to completion.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !slow.sawCancel() {
		time.Sleep(5 * time.Millisecond)
	}
	if !slow.sawCancel() {
		t.Fatal("client disconnect must cancel the upstream stream")
	}
}

// slowStreamProvider blocks in the stream until its context is canceled.
type slowStreamProvider struct {
	inner    provider.Provider
	canceled chan struct{}
	once     sync.Once
}

func (p *slowStreamProvider) Name() string { return "slow-stream" }

func (p *slowStreamProvider) sawCancel() bool {
	select {
	case <-p.canceled:
		return true
	default:
		return false
	}
}

func (p *slowStreamProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *slowStreamProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return p.inner.Complete(ctx, req)
}

func (p *slowStreamProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	if p.canceled == nil {
		p.canceled = make(chan struct{})
	}
	if err := emit(model.Event{Kind: model.EventCreated, Response: &model.Response{ID: "slow", Status: "in_progress"}}); err != nil {
		return err
	}
	<-ctx.Done()
	p.once.Do(func() { close(p.canceled) })
	return ctx.Err()
}

// --- A4: capability enforcement ---------------------------------------------

func TestResponsesCapabilityRejectionBeforeProvider(t *testing.T) {
	f := newResponsesFixture(t, textOnlyCaps, provider.Fake{})
	cases := map[string]string{
		"tools":             `{"model":"full-model","input":"x","tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]}`,
		"json_mode":         `{"model":"full-model","input":"x","response_format":{"type":"json_object"}}`,
		"structured_output": `{"model":"full-model","input":"x","response_format":{"type":"json_schema","json_schema":{"name":"s","schema":{"type":"object"}}}}`,
	}
	for name, body := range cases {
		rec := doResponses(t, f, body, testKey, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		var env APIError
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		if env.Error.Code != "capability_not_supported" {
			t.Errorf("%s: code = %q", name, env.Error.Code)
		}
	}
	if *f.calls != 0 {
		t.Errorf("capability-rejected requests reached the provider (%d calls)", *f.calls)
	}
}

func TestResponsesProtocolGate(t *testing.T) {
	f := newResponsesFixture(t, noResponsesCaps, provider.Fake{})
	rec := doResponses(t, f, responsesEchoBody, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "capability_not_supported") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if *f.calls != 0 {
		t.Error("provider must not be called")
	}
}

func TestResponsesModelOutputClamp(t *testing.T) {
	// fullCaps declares MaxOutputTokens 2048; a larger request is clamped
	// before the provider sees it.
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, `{"model":"full-model","input":"x","max_output_tokens":100000}`, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

// --- A5: tool calling ---------------------------------------------------------

// toolReqPrefix is the tool-calling request without the closing brace so
// tests can append extra fields.
const toolReqPrefix = `{"model":"full-model","input":"paris?","tools":[{"type":"function","function":{"name":"get_weather","description":"lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]`

const toolReqBody = toolReqPrefix + `}`

func TestResponsesToolCallRoundTrip(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	// Step 1: the model issues a tool call.
	rec := doResponses(t, f, toolReqBody, testKey, "req-tool-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("step1 status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Output) != 1 || resp.Output[0].Type != "function_call" {
		t.Fatalf("output = %+v", resp.Output)
	}
	fc := resp.Output[0]
	if fc.Name != "get_weather" || fc.CallID == "" {
		t.Fatalf("function call wrong: %+v", fc)
	}
	if !json.Valid([]byte(fc.Arguments)) {
		t.Fatalf("arguments not valid JSON: %q", fc.Arguments)
	}

	// Step 2: the caller returns the tool result as input.
	roundTrip := `{"model":"full-model","input":[{"role":"user","content":"paris?"},{"type":"function_call","call_id":"` + fc.CallID + `","name":"get_weather","arguments":"{\"city\":\"paris\"}"},{"type":"function_call_output","call_id":"` + fc.CallID + `","output":"sunny 22C"}],"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]}`
	rec = doResponses(t, f, roundTrip, testKey, "req-tool-2")
	if rec.Code != http.StatusOK {
		t.Fatalf("step2 status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp2 responseObject
	_ = json.Unmarshal(rec.Body.Bytes(), &resp2)
	if len(resp2.Output) != 1 || resp2.Output[0].Content[0].Text != "tool ok: sunny 22C" {
		t.Fatalf("round trip output = %+v", resp2.Output)
	}
}

func TestResponsesStreamToolArgumentsAssembly(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, toolReqPrefix+`,"stream":true}`, testKey, "req-tool-s")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	events := parseSSE(t, rec.Body.String())
	if events[0].Event != "response.created" {
		t.Fatalf("first event = %q", events[0].Event)
	}
	if events[len(events)-1].Event != "response.completed" {
		t.Fatalf("last event = %q", events[len(events)-1].Event)
	}
	var args strings.Builder
	argDeltas, argsDone := 0, 0
	for _, ev := range events {
		switch ev.Event {
		case "response.function_call_arguments.delta":
			argDeltas++
			args.WriteString(ev.Data["delta"].(string))
		case "response.function_call_arguments.done":
			argsDone++
			if ev.Data["arguments"] != args.String() {
				t.Errorf("done args %q != assembled %q", ev.Data["arguments"], args.String())
			}
			if ev.Data["name"] != "get_weather" {
				t.Errorf("done name = %v", ev.Data["name"])
			}
		}
	}
	if argDeltas < 2 || argsDone != 1 {
		t.Errorf("argDeltas = %d argsDone = %d", argDeltas, argsDone)
	}
	if !json.Valid([]byte(args.String())) {
		t.Errorf("assembled arguments invalid: %q", args.String())
	}
	completed := events[len(events)-1].Data["response"].(map[string]any)
	output := completed["output"].([]any)
	found := false
	for _, o := range output {
		item := o.(map[string]any)
		if item["type"] == "function_call" && item["arguments"] == args.String() {
			found = true
		}
	}
	if !found {
		t.Errorf("completed response must carry the assembled function call: %v", output)
	}
}

// --- A6: structured output -----------------------------------------------------

func TestResponsesJSONMode(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, `{"model":"full-model","input":"hello","response_format":{"type":"json_object"}}`, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	text := resp.Output[0].Content[0].Text
	if !json.Valid([]byte(text)) {
		t.Fatalf("json mode output not valid JSON: %q", text)
	}
}

func TestResponsesSchemaValidationFailureRecorded(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	// The mock echoes {"echo": "..."}; a schema requiring a different shape
	// fails validation, which must be recorded in audit with the reason and
	// never silently marked as clean success.
	body := `{"model":"full-model","input":"hello","response_format":{"type":"json_schema","json_schema":{"name":"other","schema":{"type":"object","required":["different_field"],"properties":{"different_field":{"type":"string"}}}}}}`
	rec := doResponses(t, f, body, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	ev := f.sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("schema validation failure must be recorded in audit: %+v", ev)
	}
}

func TestResponsesSchemaSizeAndDepthBounds(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	big := `{"model":"full-model","input":"x","response_format":{"type":"json_schema","json_schema":{"name":"s","schema":{"type":"object","description":"` + strings.Repeat("a", 40_000) + `"}}}}`
	rec := doResponses(t, f, big, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized schema must be rejected, got %d", rec.Code)
	}
	if *f.calls != 0 {
		t.Error("provider must not be called for oversized schema")
	}
}

// --- auth -------------------------------------------------------------------

func TestResponsesAuthRequired(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, responsesEchoBody, "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	rec = doResponses(t, f, responsesEchoBody, "sk-wrong", "")
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "invalid_api_key") {
		t.Fatalf("bad key: %d %s", rec.Code, rec.Body.String())
	}
	if *f.calls != 0 {
		t.Error("unauthenticated requests reached the provider")
	}
}

func TestResponsesUnknownModelNonLeaky(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	rec := doResponses(t, f, `{"model":"secret-model","input":"x"}`, testKey, "")
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}
