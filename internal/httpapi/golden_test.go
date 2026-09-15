package httpapi

// Golden compatibility fixtures for the V1 Chat Completions public contract.
// These lock the JSON body shapes, SSE framing, error envelope, and request-ID
// behavior before any internal domain-model refactor. Canonicalization zeroes
// volatile fields (created timestamps) so only the wire shape matters.

import (
	"bytes"
	"context"
	"encoding/json"
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

func goldenPost(t *testing.T, h *ChatHandler, path, body, key, requestID string) *httptest.ResponseRecorder {
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
