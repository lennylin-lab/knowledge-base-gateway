package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

const testKey = "sk-test-plaintext"

type flakyProvider struct {
	fails     int
	returnErr error
	calls     int
	inner     provider.Provider
}

func (f *flakyProvider) Name() string { return "flaky" }

func (f *flakyProvider) Capabilities(m string) model.Capabilities {
	if f.inner != nil {
		return f.inner.Capabilities(m)
	}
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Tools: true, Usage: true}
}

func (f *flakyProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	f.calls++
	if f.calls <= f.fails {
		return model.Response{}, f.returnErr
	}
	return f.inner.Complete(ctx, req)
}

func (f *flakyProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return f.inner.Stream(ctx, req, emit)
}

func newTestHandler(t *testing.T, p provider.Provider, rate int) (*ChatHandler, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-1", Subject: "subject-a", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})

	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: p.Name(), UpstreamModel: "upstream-gpt-test", Enabled: true, Capabilities: testCaps()},
		{PublicName: "disabled", Provider: p.Name(), UpstreamModel: "x", Enabled: false},
	})
	pol := policy.New()
	pol.Allow("subject-a", "gpt-test")

	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 2)
	sink := audit.NewMemorySink(nil)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(rate, 100), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, sink
}

func doChat(t *testing.T, h *ChatHandler, body string, key string, requestID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
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

func chatBody(model string, stream bool) string {
	m := map[string]interface{}{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": "hello"}},
		"stream":   stream,
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestAuthFailures(t *testing.T) {
	h, _ := newTestHandler(t, provider.Fake{}, 1000)
	cases := []struct {
		name   string
		key    string
		status int
		code   string
	}{
		{"missing", "", 401, "invalid_api_key"},
		{"wrong key", "sk-wrong", 401, "invalid_api_key"},
	}
	for _, tc := range cases {
		rec := doChat(t, h, chatBody("gpt-test", false), tc.key, "")
		if rec.Code != tc.status {
			t.Errorf("%s: status = %d, want %d", tc.name, rec.Code, tc.status)
		}
		var env APIError
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("%s: bad error envelope: %v", tc.name, err)
		}
		if env.Error.Code != tc.code || env.Error.Type != "authentication_error" {
			t.Errorf("%s: envelope = %+v", tc.name, env.Error)
		}
		if env.Error.RequestID == "" {
			t.Errorf("%s: request_id missing", tc.name)
		}
		if strings.Contains(rec.Body.String(), testKey) {
			t.Errorf("%s: response leaks key", tc.name)
		}
	}
}

func TestExpiredAndRevokedKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expire time.Time
		status auth.Status
		code   string
	}{
		{"expired", time.Now().Add(-time.Hour), auth.StatusActive, "api_key_expired"},
		{"revoked", time.Time{}, auth.StatusRevoked, "api_key_revoked"},
	} {
		store := auth.NewStore()
		salt, _ := auth.NewSalt()
		store.Put(auth.KeyRecord{ID: "k", Subject: "s", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: tc.status, ExpiresAt: tc.expire})
		h := &ChatHandler{Auth: store, Service: gateway.New(policy.NewCatalog(nil), nil, time.Second, 0), Limiter: limiter.New(100, 10), MaxBody: 1 << 20, MaxMsgs: 10, MaxChars: 1000}
		rec := doChat(t, h, chatBody("m", false), testKey, "")
		if rec.Code != 401 {
			t.Errorf("%s: status = %d, want 401", tc.name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), tc.code) {
			t.Errorf("%s: want code %s in body %s", tc.name, tc.code, rec.Body.String())
		}
	}
}

func TestNonStreamingContract(t *testing.T) {
	h, sink := newTestHandler(t, provider.Fake{}, 1000)
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "req-fixed-1")
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp chatCompletionOut
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID == "" || resp.Object != "chat.completion" || resp.Created == 0 || resp.Model == "" {
		t.Errorf("missing OpenAI-compatible fields: %+v", resp)
	}
	if len(resp.Choices) == 0 || resp.Choices[0].Message == nil || !strings.HasPrefix(resp.Choices[0].Message.Content, "echo: ") {
		t.Errorf("unexpected choices: %+v", resp.Choices)
	}
	if resp.Usage == nil || resp.Usage.PromptTokens != 10 || resp.Usage.CompletionTokens != 11 || resp.Usage.TotalTokens != 21 {
		t.Errorf("usage not preserved: %+v", resp.Usage)
	}
	if rec.Header().Get("X-Request-ID") != "req-fixed-1" {
		t.Errorf("request id not echoed")
	}
	events := sink.Snapshot()
	if len(events) != 1 || events[0].SubjectID != "subject-a" || events[0].Status != 200 {
		t.Errorf("audit event wrong: %+v", events)
	}
}

func TestUnknownAndForbiddenModelsNonLeaky(t *testing.T) {
	h, _ := newTestHandler(t, provider.Fake{}, 1000)
	for _, model := range []string{"no-such-model", "disabled"} {
		rec := doChat(t, h, chatBody(model, false), testKey, "")
		if rec.Code != 403 {
			t.Errorf("model %q: status = %d, want 403", model, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "model_not_allowed") {
			t.Errorf("model %q: non-leaky code expected", model)
		}
	}
}

func TestPolicyDeniedSubject(t *testing.T) {
	// subject-a only allowed gpt-test; request a second catalog model.
	h, _ := newTestHandler(t, provider.Fake{}, 1000)
	rec := doChat(t, h, chatBody("disabled", false), testKey, "")
	if rec.Code != 403 {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestValidation(t *testing.T) {
	h, _ := newTestHandler(t, provider.Fake{}, 1000)
	cases := map[string]string{
		"empty body":       ``,
		"no model":         `{"messages":[{"role":"user","content":"x"}]}`,
		"no messages":      `{"model":"gpt-test"}`,
		"bad max_tokens":   `{"model":"gpt-test","messages":[{"role":"user","content":"x"}],"max_tokens":-1}`,
		"message too long": `{"model":"gpt-test","messages":[{"role":"user","content":"` + strings.Repeat("a", 40_000) + `"}]}`,
	}
	for name, body := range cases {
		rec := doChat(t, h, body, testKey, "")
		if rec.Code != 400 {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "invalid_request_error") {
			t.Errorf("%s: wrong error type", name)
		}
	}
}

func TestRateLimit(t *testing.T) {
	h, _ := newTestHandler(t, provider.Fake{}, 2)
	for i := 0; i < 2; i++ {
		if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
			t.Fatalf("request %d: status = %d", i, rec.Code)
		}
	}
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != 429 {
		t.Errorf("status = %d, want 429", rec.Code)
	}
}

func TestStreamingContract(t *testing.T) {
	h, sink := newTestHandler(t, provider.Fake{}, 1000)
	rec := doChat(t, h, chatBody("gpt-test", true), testKey, "")
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "data: ") {
		t.Errorf("no SSE data events")
	}
	if !strings.Contains(body, "data: [DONE]\n\n") || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Errorf("missing terminal [DONE]: %q", body)
	}
	if ev := sink.Snapshot(); len(ev) != 1 || !ev[0].Streaming {
		t.Errorf("audit streaming flag wrong: %+v", ev)
	}
}

func TestRetryOnEligibleErrors(t *testing.T) {
	inner := provider.Fake{}
	flaky := &flakyProvider{fails: 2, returnErr: &provider.Error{Class: provider.ClassServer, Msg: "boom"}, inner: inner}
	h, _ := newTestHandler(t, flaky, 1000)
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if flaky.calls != 3 {
		t.Errorf("calls = %d, want 3 (2 retries)", flaky.calls)
	}
}

func TestNoRetryOnInvalidErrors(t *testing.T) {
	inner := provider.Fake{}
	flaky := &flakyProvider{fails: 5, returnErr: &provider.Error{Class: provider.ClassInvalid, Msg: "bad request"}, inner: inner}
	h, _ := newTestHandler(t, flaky, 1000)
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != 400 {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if flaky.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry)", flaky.calls)
	}
}

func TestRetryExhaustionMapsToServiceUnavailable(t *testing.T) {
	inner := provider.Fake{}
	flaky := &flakyProvider{fails: 10, returnErr: &provider.Error{Class: provider.ClassServer, Msg: "boom"}, inner: inner}
	h, _ := newTestHandler(t, flaky, 1000)
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != 503 {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler(t, provider.Fake{}, 1000)
	mux := NewMux(h, Deps{Metrics: http.NotFoundHandler()})
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 405 {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

// testCaps is the declared catalog capability matrix used by the legacy
// contract tests: text chat with streaming and usage, no clamps, so the
// documented request arithmetic in these tests stays exact.
func testCaps() model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}
}
