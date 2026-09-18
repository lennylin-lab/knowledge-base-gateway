package httpapi

// Golden compatibility fixtures for the V1 Chat Completions public contract.
// These lock the JSON body shapes, SSE framing, error envelope, and request-ID
// behavior before any internal domain-model refactor. Canonicalization zeroes
// volatile fields (created timestamps) so only the wire shape matters.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite golden fixture files")

const goldenModel = "golden-model"

// newGoldenHandler wires a ChatHandler against the deterministic fake
// provider with a fixed upstream model name and the legacy text-only matrix.
func newGoldenHandler(t *testing.T, p provider.Provider, rate int) (*ChatHandler, *audit.MemorySink) {
	return newGoldenHandlerCaps(t, testCaps(), p, rate)
}

// newGoldenHandlerCaps wires the same handler with an explicit capability
// matrix (used by the V1.2 tool/structured-output contract tests).
func newGoldenHandlerCaps(t *testing.T, caps model.Capabilities, p provider.Provider, rate int) (*ChatHandler, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-g", Subject: "subject-g", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: goldenModel, Provider: p.Name(), UpstreamModel: "upstream-golden", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.Allow("subject-g", goldenModel)
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 1)
	svc.RetryWait = time.Millisecond
	sink := audit.NewMemorySink(nil)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(rate, 100), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, sink
}

// canonical renders a JSON value deterministically with volatile fields
// masked so golden files capture shape, not timestamps.
func canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var tree any
	if err := json.Unmarshal(b, &tree); err != nil {
		t.Fatal(err)
	}
	maskVolatile(tree)
	out, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func maskVolatile(v any) {
	switch t := v.(type) {
	case map[string]any:
		if _, ok := t["created"]; ok {
			t["created"] = 0
		}
		for _, k := range []string{"id"} {
			if s, ok := t[k].(string); ok && s != "" {
				t[k] = "<id>"
			}
		}
		for _, k := range []string{"call_id", "tool_call_id"} {
			if s, ok := t[k].(string); ok && s != "" {
				t[k] = "<id>"
			}
		}
		for _, child := range t {
			maskVolatile(child)
		}
	case []any:
		for _, child := range t {
			maskVolatile(child)
		}
	}
}

func goldenPath(name string) string {
	return filepath.Join("testdata", "golden", name)
}

func compareGolden(t *testing.T, name, got string) {
	t.Helper()
	path := goldenPath(name)
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("golden fixture %s missing (run with -update-golden): %v", name, err)
	}
	if got != strings.TrimRight(string(want), "\n") {
		t.Errorf("golden mismatch in %s:\n got: %s\nwant: %s", name, got, strings.TrimRight(string(want), "\n"))
	}
}

func goldenPost(t *testing.T, h http.Handler, path, body, key, requestID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if requestID != "" {
		req.Header.Set("X-Request-ID", requestID)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGoldenChatNonStreaming(t *testing.T) {
	h, _ := newGoldenHandler(t, provider.Fake{}, 1000)
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hello"}],"stream":false}`,
		testKey, "req_golden_1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	compareGolden(t, "chat_non_stream.json", canonical(t, v))
	if rec.Header().Get("X-Request-ID") != "req_golden_1" {
		t.Error("X-Request-ID must be echoed")
	}
}

func TestGoldenChatStreaming(t *testing.T) {
	h, _ := newGoldenHandler(t, provider.Fake{}, 1000)
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hello"}],"stream":true}`,
		testKey, "req_golden_2")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("stream must end with data: [DONE]: %q", body)
	}
	var lines []any
	for _, line := range strings.Split(strings.TrimSuffix(body, "data: [DONE]\n\n"), "\n\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var v any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &v); err != nil {
			t.Fatalf("chunk not JSON: %v in %q", err, line)
		}
		lines = append(lines, v)
	}
	compareGolden(t, "chat_stream_chunks.json", canonical(t, lines))
}

// failingStreamProvider fails Stream before any output.
type failingStreamProvider struct{ inner provider.Provider }

func (f *failingStreamProvider) Name() string { return "failing-stream" }

func (f *failingStreamProvider) Capabilities(m string) model.Capabilities {
	return f.inner.Capabilities(m)
}

func (f *failingStreamProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (f *failingStreamProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return f.inner.Complete(ctx, req)
}

func (f *failingStreamProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return &provider.Error{Class: provider.ClassServer, Msg: "upstream exploded"}
}

func TestGoldenChatStreamPreFirstEventFailure(t *testing.T) {
	h, _ := newGoldenHandler(t, &failingStreamProvider{inner: provider.Fake{}}, 1000)
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hello"}],"stream":true}`,
		testKey, "req_golden_3")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "data: [DONE]") {
		t.Fatalf("failed stream must not end with [DONE]: %q", body)
	}
	if !strings.Contains(body, `"error"`) || !strings.Contains(body, "req_golden_3") {
		t.Fatalf("failed stream must emit error event with request id: %q", body)
	}
	var evt struct {
		Error struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	for _, line := range strings.Split(body, "\n\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if strings.HasPrefix(payload, "{") {
			_ = json.Unmarshal([]byte(payload), &evt)
		}
	}
	if evt.Error.Type == "" || evt.Error.RequestID != "req_golden_3" {
		t.Errorf("error event shape wrong: %q", body)
	}
}

func TestGoldenErrorEnvelopes(t *testing.T) {
	h, _ := newGoldenHandler(t, provider.Fake{}, 1000)
	cases := []struct {
		name string
		body string
		key  string
		code string
	}{
		{"missing_key", `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"x"}]}`, "", "invalid_api_key"},
		{"invalid_key", `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"x"}]}`, "sk-wrong", "invalid_api_key"},
		{"unknown_model", `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, testKey, "model_not_allowed"},
		{"bad_max_tokens", `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"x"}],"max_tokens":-5}`, testKey, "invalid_request"},
	}
	for _, tc := range cases {
		rec := goldenPost(t, h, "/v1/chat/completions", tc.body, tc.key, "req_golden_err")
		var env APIError
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("%s: not an envelope: %v", tc.name, err)
		}
		if env.Error.Code != tc.code {
			t.Errorf("%s: code = %q, want %q", tc.name, env.Error.Code, tc.code)
		}
		if env.Error.RequestID != "req_golden_err" {
			t.Errorf("%s: request_id = %q", tc.name, env.Error.RequestID)
		}
		compareGolden(t, "error_"+tc.name+".json", canonical(t, env))
	}
}

func TestGoldenRateLimitEnvelope(t *testing.T) {
	h, _ := newGoldenHandler(t, provider.Fake{}, 1)
	if rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"x"}]}`, testKey, ""); rec.Code != 200 {
		t.Fatalf("first request: %d", rec.Code)
	}
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"x"}]}`, testKey, "req_golden_429")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 must include Retry-After when computable")
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "rate_limit_exceeded" {
		t.Errorf("code = %q", env.Error.Code)
	}
	compareGolden(t, "error_rate_limited.json", canonical(t, env))
}

// TestGoldenFixtureStability guards against accidental golden regeneration:
// canonical must be deterministic.
func TestGoldenFixtureStability(t *testing.T) {
	a := canonical(t, map[string]any{"b": 1, "a": []any{map[string]any{"created": 123, "id": "x"}}})
	b := canonical(t, map[string]any{"a": []any{map[string]any{"id": "x", "created": 999}}, "b": 1})
	if a != b {
		t.Errorf("canonical not deterministic:\n%s\n%s", a, b)
	}
	if !strings.Contains(a, `"created":0`) {
		t.Errorf("created not masked: %s", a)
	}
}

// --- V1.4 compatibility freeze: the remaining sync surface -----------------
//
// The V1.4 roadmap freezes V1.3 wire behavior before any async/cost work:
// /v1/responses and /v1/embeddings join the chat goldens, SSE termination is
// pinned for both streaming protocols, the default-model backfill, capability
// gating, token quota denial, request-ID echo, and provider redaction all get
// wire-level coverage. New V1.4 endpoints may only extend this surface
// additively; any drift in these fixtures is a compatibility regression.

// newGoldenResponsesHandler wires a ResponsesHandler with the same subject,
// key, and catalog model as the chat goldens.
func newGoldenResponsesHandler(t *testing.T, caps model.Capabilities, p provider.Provider, rate int) (*ResponsesHandler, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-g", Subject: "subject-g", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: goldenModel, Provider: p.Name(), UpstreamModel: "upstream-golden", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.Allow("subject-g", goldenModel)
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 1)
	svc.RetryWait = time.Millisecond
	sink := audit.NewMemorySink(nil)
	h := &ResponsesHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(rate, 100), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
	}
	return h, sink
}

// newGoldenEmbeddingsHandler wires an EmbeddingsHandler with the golden
// subject/key and a quota gate so quota denials can be exercised.
func newGoldenEmbeddingsHandler(t *testing.T, p provider.Provider, limits policy.Limits) (*EmbeddingsHandler, *audit.MemorySink) {
	t.Helper()
	caps := model.Capabilities{
		Chat: true, Responses: true, Embeddings: true, Stream: true, Usage: true,
		EmbeddingDim: provider.FakeEmbeddingDim,
	}
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-g", Subject: "subject-g", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: goldenModel, Provider: p.Name(), UpstreamModel: "upstream-golden", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.Allow("subject-g", goldenModel)
	if limits != (policy.Limits{}) {
		pol.SetLimits("subject-g", limits)
	}
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	h := &EmbeddingsHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 128, MaxChars: 32_000,
	}
	return h, sink
}

// goldenSSEEvents parses one framed Responses SSE body into (event, payload)
// pairs and returns the canonicalized payloads in order. Volatile fields are
// masked so the fixtures capture the event grammar, not timestamps.
func goldenSSEEvents(t *testing.T, body string) ([]string, []any) {
	t.Helper()
	var names []string
	var payloads []any
	for _, block := range strings.Split(body, "\n\n") {
		var name string
		var data string
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		if name == "" && data == "" {
			continue
		}
		if name == "" || data == "" {
			t.Fatalf("unframed SSE block: %q", block)
		}
		var v any
		if err := json.Unmarshal([]byte(data), &v); err != nil {
			t.Fatalf("event %s payload not JSON: %v", name, err)
		}
		names = append(names, name)
		payloads = append(payloads, v)
	}
	return names, payloads
}

func TestGoldenResponsesNonStreaming(t *testing.T) {
	h, _ := newGoldenResponsesHandler(t, testCaps(), provider.Fake{}, 1000)
	rec := goldenPost(t, h, "/v1/responses",
		`{"model":"`+goldenModel+`","input":"hello"}`, testKey, "req_golden_resp_1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	compareGolden(t, "responses_non_stream.json", canonical(t, v))
	if rec.Header().Get("X-Request-ID") != "req_golden_resp_1" {
		t.Error("X-Request-ID must be echoed")
	}
}

func TestGoldenResponsesStreamingTermination(t *testing.T) {
	h, _ := newGoldenResponsesHandler(t, testCaps(), provider.Fake{}, 1000)
	rec := goldenPost(t, h, "/v1/responses",
		`{"model":"`+goldenModel+`","input":"hello","stream":true}`, testKey, "req_golden_resp_2")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	if rec.Header().Get("X-Request-ID") != "req_golden_resp_2" {
		t.Error("X-Request-ID must be echoed on streams")
	}
	names, payloads := goldenSSEEvents(t, rec.Body.String())
	if len(names) == 0 || names[0] != "response.created" {
		t.Fatalf("stream must open with response.created: %v", names)
	}
	// SSE termination contract: the final event is response.completed; a
	// failed stream never carries it (pinned by the failure golden below).
	if last := names[len(names)-1]; last != "response.completed" {
		t.Fatalf("stream must terminate with response.completed, got %q (events: %v)", last, names)
	}
	compareGolden(t, "responses_stream_events.json", canonical(t, payloads))
}

func TestGoldenResponsesStreamPreFirstEventFailure(t *testing.T) {
	h, _ := newGoldenResponsesHandler(t, testCaps(), &failingStreamProvider{inner: provider.Fake{}}, 1000)
	rec := goldenPost(t, h, "/v1/responses",
		`{"model":"`+goldenModel+`","input":"hello","stream":true}`, testKey, "req_golden_resp_3")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	names, payloads := goldenSSEEvents(t, rec.Body.String())
	for _, n := range names {
		if n == "response.completed" {
			t.Fatalf("failed stream must never terminate in response.completed: %v", names)
		}
	}
	if len(names) == 0 || names[len(names)-1] != "response.failed" {
		t.Fatalf("failed stream must terminate with response.failed, got %v", names)
	}
	if len(payloads) != 1 {
		t.Fatalf("pre-first-event failure must emit exactly the terminal event, got %v", names)
	}
	compareGolden(t, "responses_stream_failed.json", canonical(t, payloads[0]))
}

func TestGoldenEmbeddingsNonStreaming(t *testing.T) {
	h, _ := newGoldenEmbeddingsHandler(t, provider.Fake{}, policy.Limits{})
	rec := goldenPost(t, h, "/v1/embeddings",
		`{"model":"`+goldenModel+`","input":"hello"}`, testKey, "req_golden_emb_1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	compareGolden(t, "embeddings_non_stream.json", canonical(t, v))
	if rec.Header().Get("X-Request-ID") != "req_golden_emb_1" {
		t.Error("X-Request-ID must be echoed")
	}
}

// TestGoldenDefaultModelBackfill freezes the V1.3 default-model backfill wire
// behavior: a request without `model` resolves the subject's slot per
// protocol (chat/responses -> default_model, embeddings ->
// default_embedding_model), the public model name is echoed in the response,
// and a request with neither explicit model nor configured default keeps its
// stable 400 invalid_request.
func TestGoldenDefaultModelBackfill(t *testing.T) {
	// Chat: the backfilled response shape is frozen with its own fixture.
	// The chat wire echoes the upstream model in `model` (frozen V1.3
	// behavior), so the backfilled public model is proven through the audit
	// record.
	h, sink := newGoldenHandlerCaps(t, testCaps(), provider.Fake{}, 1000)
	h.Policy.SetLimits("subject-g", policy.Limits{DefaultModel: goldenModel})
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hello"}],"stream":false}`, testKey, "req_golden_def_1")
	if rec.Code != http.StatusOK {
		t.Fatalf("chat backfill: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if evs := sink.Snapshot(); len(evs) != 1 || evs[0].Model != goldenModel || evs[0].Protocol != "chat" {
		t.Fatalf("chat backfill must resolve the subject default: %+v", sink.Snapshot())
	}
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	compareGolden(t, "chat_default_model.json", canonical(t, v))

	// Responses: same backfill through the responses slot resolution.
	rh, _ := newGoldenResponsesHandler(t, testCaps(), provider.Fake{}, 1000)
	rh.Policy.SetLimits("subject-g", policy.Limits{DefaultModel: goldenModel})
	rec = goldenPost(t, rh, "/v1/responses", `{"input":"hello"}`, testKey, "req_golden_def_2")
	if rec.Code != http.StatusOK {
		t.Fatalf("responses backfill: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var robj responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &robj); err != nil {
		t.Fatal(err)
	}
	if robj.Model != goldenModel {
		t.Fatalf("responses backfill model = %q", robj.Model)
	}

	// Embeddings: resolves the dedicated embedding slot.
	eh, _ := newGoldenEmbeddingsHandler(t, provider.Fake{}, policy.Limits{DefaultEmbeddingModel: goldenModel})
	rec = goldenPost(t, eh, "/v1/embeddings", `{"input":"hello"}`, testKey, "req_golden_def_3")
	if rec.Code != http.StatusOK {
		t.Fatalf("embeddings backfill: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var eobj embeddingsResponseOut
	if err := json.Unmarshal(rec.Body.Bytes(), &eobj); err != nil {
		t.Fatal(err)
	}
	if eobj.Model != goldenModel {
		t.Fatalf("embeddings backfill model = %q", eobj.Model)
	}

	// No model and no default anywhere: stable 400 invalid_request. A fresh
	// handler without a configured default slot.
	nh, _ := newGoldenHandlerCaps(t, testCaps(), provider.Fake{}, 1000)
	rec = goldenPost(t, nh, "/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hello"}]}`, testKey, "req_golden_def_4")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing model: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "invalid_request" {
		t.Fatalf("missing model code = %q", env.Error.Code)
	}
	compareGolden(t, "error_model_required.json", canonical(t, env))
}

// TestGoldenCapabilityGateEnvelope freezes the capability denial envelope:
// a protocol a model does not declare is rejected 400 capability_not_supported
// before any provider invocation, on every endpoint.
func TestGoldenCapabilityGateEnvelope(t *testing.T) {
	// Responses against a chat-only model (no responses capability).
	chatOnly := model.Capabilities{Chat: true, Stream: true, Usage: true}
	rh, _ := newGoldenResponsesHandler(t, chatOnly, provider.Fake{}, 1000)
	rec := goldenPost(t, rh, "/v1/responses",
		`{"model":"`+goldenModel+`","input":"hello"}`, testKey, "req_golden_cap_1")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("responses capability gate: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "capability_not_supported" || env.Error.Type != "invalid_request_error" {
		t.Fatalf("capability envelope = %+v", env.Error)
	}
	compareGolden(t, "error_capability_not_supported.json", canonical(t, env))

	// Embeddings against a model without the embeddings capability: swap the
	// service for a text-only catalog, keeping the handler's auth/policy.
	eh, _ := newGoldenEmbeddingsHandler(t, provider.Fake{}, policy.Limits{})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: goldenModel, Provider: "fake", UpstreamModel: "upstream-golden", Enabled: true,
			Capabilities: model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}},
	})
	eh.Service = gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, 2*time.Second, 0)
	rec = goldenPost(t, eh, "/v1/embeddings",
		`{"model":"`+goldenModel+`","input":"hello"}`, testKey, "req_golden_cap_2")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("embeddings capability gate: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	env = APIError{}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "capability_not_supported" {
		t.Fatalf("embeddings capability code = %q", env.Error.Code)
	}
}

// TestGoldenQuotaEnvelope freezes the token-quota denial envelope: a subject
// with an exhausted daily budget is denied 429 quota_exceeded with
// Retry-After, distinct from the rate limiter's rate_limit_exceeded.
func TestGoldenQuotaEnvelope(t *testing.T) {
	h, _ := newGoldenHandlerCaps(t, testCaps(), provider.Fake{}, 1000)
	h.Quota = quota.NewMemory()
	// Deterministic budget: input "hello" estimates 2 input tokens and the
	// declared max_tokens 2048 output reserve (2050 total). The first request
	// settles at the fake's reported 21 total tokens; the second reservation
	// (21 + 2050 = 2071) exceeds the 2060 daily budget and is denied.
	h.Policy.SetLimits("subject-g", policy.Limits{DailyTokens: 2060})
	if rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hello"}],"max_tokens":2048}`, testKey, ""); rec.Code != 200 {
		t.Fatalf("first request must consume the budget: %d", rec.Code)
	}
	rec := goldenPost(t, h, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hello"}],"max_tokens":2048}`, testKey, "req_golden_quota")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("quota denial must include Retry-After when computable")
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "quota_exceeded" {
		t.Errorf("code = %q, want quota_exceeded", env.Error.Code)
	}
	compareGolden(t, "error_quota_exceeded.json", canonical(t, env))
}

// TestGoldenProviderRedaction proves the public wire of every sync endpoint
// stays free of provider identity internals: the provider registry name and
// internal base URLs never appear in success or error bodies, and the
// responses/embeddings envelopes carry the public model name. The chat
// protocol's `model` echo is the upstream name by frozen V1.3 wire behavior
// (pinned by chat_non_stream.json) and is exempt from the upstream-model
// check; provider identity, URLs, and secrets never are.
func TestGoldenProviderRedaction(t *testing.T) {
	p := provider.Fake{}
	identitySentinels := []string{`"fake"`, "internal://"}
	upstreamModelSentinel := "upstream-golden"

	check := func(t *testing.T, name string, rec *httptest.ResponseRecorder, publicModelExpected bool) {
		t.Helper()
		body := rec.Body.String()
		for _, s := range identitySentinels {
			if strings.Contains(body, s) {
				t.Errorf("%s: body leaks provider internal %q: %s", name, s, body)
			}
		}
		if publicModelExpected && strings.Contains(body, upstreamModelSentinel) {
			t.Errorf("%s: body must carry the public model name, not %q: %s", name, upstreamModelSentinel, body)
		}
	}

	ch, _ := newGoldenHandler(t, p, 1000)
	check(t, "chat success", goldenPost(t, ch, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hi"}]}`, testKey, "req_red_1"), false)
	check(t, "chat unknown model", goldenPost(t, ch, "/v1/chat/completions",
		`{"model":"nope","messages":[{"role":"user","content":"hi"}]}`, testKey, "req_red_2"), false)
	fh, _ := newGoldenHandler(t, &failingStreamProvider{inner: p}, 1000)
	check(t, "chat upstream failure", goldenPost(t, fh, "/v1/chat/completions",
		`{"model":"`+goldenModel+`","messages":[{"role":"user","content":"hi"}],"stream":true}`, testKey, "req_red_3"), false)

	rh, _ := newGoldenResponsesHandler(t, testCaps(), p, 1000)
	check(t, "responses success", goldenPost(t, rh, "/v1/responses",
		`{"model":"`+goldenModel+`","input":"hi"}`, testKey, "req_red_4"), true)

	eh, _ := newGoldenEmbeddingsHandler(t, p, policy.Limits{})
	check(t, "embeddings success", goldenPost(t, eh, "/v1/embeddings",
		`{"model":"`+goldenModel+`","input":"hi"}`, testKey, "req_red_5"), true)
}
