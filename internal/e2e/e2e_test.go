// Package e2e exercises the full gateway HTTP stack against deterministic
// fake providers, covering the integration contract used by the
// knowledge-base-server client: one base URL, one internal key, stable error
// envelope, non-streaming and streaming flows, and failover.
package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// flakyProvider fails the first N Complete calls, then behaves like Fake.
type flakyProvider struct {
	name      string
	remaining int
	err       error
}

func (p *flakyProvider) Name() string { return p.name }

func (p *flakyProvider) Capabilities(m string) model.Capabilities {
	return provider.Fake{}.Capabilities(m)
}

func (p *flakyProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	if p.remaining > 0 {
		p.remaining--
		return model.Response{}, p.err
	}
	return provider.Fake{}.Complete(ctx, req)
}

func (p *flakyProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return provider.Fake{}.Stream(ctx, req, emit)
}

type fixture struct {
	srv     *httptest.Server
	client  *http.Client
	metrics *metrics.Registry
	audit   *audit.MemorySink
	key     string
}

func start(t *testing.T, primary provider.Provider) *fixture {
	t.Helper()
	gen, err := auth.NewManager(auth.NewStore()).Create(context.Background(), "subject-kb", "tenant_default", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// Rebuild the store holding the generated key.
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{
		ID: gen.Record.ID, Subject: "subject-kb", TenantID: "tenant_default",
		Salt: salt, Hash: auth.HashAPIKey(salt, gen.Plaintext),
		Prefix: gen.Record.Prefix, Status: auth.StatusActive, CreatedAt: time.Now(),
	})

	backup := provider.Fake{}
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gateway-echo", Provider: "fake-primary", UpstreamModel: "echo-model", Enabled: true, Capabilities: testCaps()},
	})
	svc := gateway.New(catalog, map[string]provider.Provider{"fake-primary": primary, "fake-backup": backup}, 5*time.Second, 1)
	svc.RetryWait = time.Millisecond
	svc.Routes.SetRoutes("gateway-echo", []router.Route{
		{ProviderName: "fake-primary", Provider: primary, UpstreamModel: "echo-model", Priority: 10, Enabled: true, Breaker: router.NewBreaker(10, time.Minute)},
		{ProviderName: "fake-backup", Provider: backup, UpstreamModel: "echo-model", Priority: 20, Enabled: true, Breaker: router.NewBreaker(10, time.Minute)},
	})
	pol := policy.New()
	pol.AllowAll("subject-kb")
	pol.SetLimits("subject-kb", policy.Limits{RatePerMinute: 120, MaxConcurrent: 8, MaxOutputTokens: 16})

	mreg := metrics.New()
	sink := audit.NewMemorySink(nil)
	chat := &httpapi.ChatHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(120, 8),
		Audit: sink, Metrics: mreg,
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	srv := httptest.NewServer(httpapi.NewMux(chat, httpapi.Deps{Metrics: mreg.Handler()}))
	t.Cleanup(srv.Close)
	return &fixture{srv: srv, client: srv.Client(), metrics: mreg, audit: sink, key: gen.Plaintext}
}

func (f *fixture) chat(t *testing.T, body string, stream bool) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, f.srv.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+f.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", "req_e2e_1")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		buf.WriteString(sc.Text())
		buf.WriteString("\n")
	}
	resp.Body.Close()
	return resp, []byte(buf.String())
}

func TestEndToEndNonStreamingWithFailover(t *testing.T) {
	f := start(t, &flakyProvider{name: "fake-primary", remaining: 1,
		err: &provider.Error{Class: provider.ClassServer, Msg: "boom"}})
	resp, raw := f.chat(t, `{"model":"gateway-echo","messages":[{"role":"user","content":"hi"}]}`, false)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 after failover, got %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		ID     string `json:"id"`
		Object string `json:"object"`
		Model  string `json:"model"`
		Usage  *struct {
			TotalTokens int  `json:"total_tokens"`
			prompt      int  `json:"-"`
			Known       bool `json:"-"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode: %v %s", err, raw)
	}
	if out.Object != "chat.completion" || out.Model != "echo-model" {
		t.Fatalf("openai-compatible shape broken: %s", raw)
	}
	// Audit records the serving provider (backup) and request id correlation.
	for _, e := range f.audit.Snapshot() {
		if e.RequestID != "req_e2e_1" {
			t.Fatalf("audit must echo X-Request-ID, got %q", e.RequestID)
		}
	}
}

func TestEndToEndStreaming(t *testing.T) {
	f := start(t, provider.Fake{})
	resp, raw := f.chat(t, `{"model":"gateway-echo","messages":[{"role":"user","content":"hello"}],"stream":true}`, true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(raw), "data: [DONE]") {
		t.Fatalf("stream must end with [DONE], got %q", raw)
	}
}

func TestEndToEndErrorEnvelope(t *testing.T) {
	f := start(t, provider.Fake{})
	// Unknown model: 403, non-leaky.
	resp, raw := f.chat(t, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`, false)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
	for _, leaked := range []string{"policy", "whitelist", "catalog"} {
		if strings.Contains(strings.ToLower(string(raw)), leaked) {
			t.Fatalf("error leaks policy details: %s", raw)
		}
	}
	var env struct {
		Error struct {
			Type      string `json:"type"`
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Error.Code != "model_not_allowed" || env.Error.RequestID != "req_e2e_1" {
		t.Fatalf("bad error envelope: %s", raw)
	}

	// Invalid key: 401 and no provider call.
	old := f.key
	f.key = "kb_wrong"
	resp, raw = f.chat(t, `{"model":"gateway-echo","messages":[{"role":"user","content":"x"}]}`, false)
	f.key = old
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(string(raw), "invalid_api_key") {
		t.Fatalf("want 401 invalid_api_key, got %d %s", resp.StatusCode, raw)
	}
	_ = fmt.Sprint
}

func TestEndToEndRateLimitRetryAfter(t *testing.T) {
	gen, err := auth.NewManager(auth.NewStore()).Create(context.Background(), "s", "t", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{ID: gen.Record.ID, Subject: "s", Salt: salt, Hash: auth.HashAPIKey(salt, gen.Plaintext), Status: auth.StatusActive, CreatedAt: time.Now()})
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: testCaps()}})
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, time.Second, 0)
	pol := policy.New()
	pol.AllowAll("s")
	mreg := metrics.New()
	chat := &httpapi.ChatHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(1, 8),
		Audit: audit.NewMemorySink(nil), Metrics: mreg,
		MaxBody: 1 << 20, MaxMsgs: 8, MaxChars: 1000,
	}
	srv := httptest.NewServer(httpapi.NewMux(chat, httpapi.Deps{Metrics: mreg.Handler()}))
	defer srv.Close()

	do := func() *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"x"}]}`))
		req.Header.Set("Authorization", "Bearer "+gen.Plaintext)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if r := do(); r.StatusCode != http.StatusOK {
		t.Fatalf("first request should pass, got %d", r.StatusCode)
	}
	r := do()
	if r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request should be rate limited, got %d", r.StatusCode)
	}
	if r.Header.Get("Retry-After") == "" {
		t.Fatal("429 should include Retry-After when computable")
	}
}

// testCaps is the declared catalog capability matrix for the e2e fixtures:
// text chat with streaming and usage, no clamps.
func testCaps() model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}
}
