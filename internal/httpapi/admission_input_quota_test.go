package httpapi

// Input-quota admission tests (issue #1 R1/R2, AC1): requests whose
// deterministic input estimate (chars/4) exceeds the model's declared
// ContextTokens or the subject's persisted MaxInputTokens ceiling are
// rejected with the stable invalid_request envelope before any provider
// invocation and before the token-quota reservation.

import (
	"encoding/json"
	"fmt"
	"net/http"
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

// newInputCeilingHandler wires a ChatHandler with the given capability matrix
// and subject limits, a token-quota gate, and a provider call counter.
func newInputCeilingHandler(t *testing.T, caps model.Capabilities, limits policy.Limits) (*ChatHandler, *countingProvider) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-ic", Subject: "subject-ic", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.Allow("subject-ic", "gpt-test")
	if limits != (policy.Limits{}) {
		pol.SetLimits("subject-ic", limits)
	}
	counting := &countingProvider{inner: provider.Fake{}}
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": counting}, 2*time.Second, 0)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(),
		Audit: audit.NewMemorySink(nil), Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, counting
}

// chatInputBody builds a chat request whose single user message carries
// exactly n content characters.
func chatInputBody(n int) string {
	return fmt.Sprintf(`{"model":"gpt-test","messages":[{"role":"user","content":"%s"}]}`, strings.Repeat("a", n))
}

// TestChatModelContextCeilingRejectsBeforeProvider pins R1/AC1: an input
// estimate above the model's declared ContextTokens is a 400 invalid_request
// that never reaches the provider, while an input exactly at the ceiling is
// still admitted.
func TestChatModelContextCeilingRejectsBeforeProvider(t *testing.T) {
	caps := testCaps()
	caps.ContextTokens = 10 // 40 content chars are 10 tokens
	h, p := newInputCeilingHandler(t, caps, policy.Limits{})

	rec := doChat(t, h, chatInputBody(40), testKey, "req-ctx-ok")
	if rec.Code != http.StatusOK {
		t.Fatalf("input at the context ceiling must pass, got %d: %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("at-ceiling request provider calls = %d, want 1", p.calls)
	}

	rec = doChat(t, h, chatInputBody(41), testKey, "req-ctx-over")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Type != "invalid_request_error" || env.Error.Code != "invalid_request" {
		t.Fatalf("bad envelope: %+v", env.Error)
	}
	if strings.Contains(rec.Body.String(), "10") {
		t.Fatalf("rejection must not echo the ceiling value: %s", rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("context-ceiling rejection reached the provider: calls = %d", p.calls)
	}
	if env.Error.RequestID != "req-ctx-over" {
		t.Fatalf("request id correlation missing: %+v", env.Error)
	}
}

// TestChatSubjectInputCeilingRejectsBeforeQuotaReserve pins R2/AC1: an input
// estimate above the subject's MaxInputTokens is rejected before the token
// quota reservation — the budget is untouched, so the next valid request is
// still admitted.
func TestChatSubjectInputCeilingRejectsBeforeQuotaReserve(t *testing.T) {
	// Daily budget admits exactly one valid "hello" request (4098 tokens).
	// If the oversized rejection reserved anything, the valid request below
	// would be denied 429 instead of 200.
	budget := policy.Limits{
		MaxInputTokens: 5, // 20 content chars are 5 tokens
		DailyTokens:    helloEstimate(),
	}
	h, p := newInputCeilingHandler(t, testCaps(), budget)

	rec := doChat(t, h, chatInputBody(21), testKey, "req-subj-cap")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("envelope must be invalid_request: %s", rec.Body.String())
	}
	if p.calls != 0 {
		t.Fatalf("subject-ceiling rejection reached the provider: calls = %d", p.calls)
	}

	// hello = 5 chars = 2 tokens, inside the 5-token subject ceiling.
	rec = doChat(t, h, chatInputBody(5), testKey, "req-subj-ok")
	if rec.Code != http.StatusOK {
		t.Fatalf("valid request after rejection must pass: %d %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("provider calls = %d, want 1 (rejection must not consume quota)", p.calls)
	}
}

// TestResponsesContextCeilingRejectsBeforeProvider pins AC1 for /v1/responses:
// the shared admission pipeline enforces the model context ceiling for both
// protocols before any provider invocation.
func TestResponsesContextCeilingRejectsBeforeProvider(t *testing.T) {
	caps := fullCaps
	caps.ContextTokens = 10 // 40 input chars are 10 tokens
	f := newResponsesFixture(t, caps, provider.Fake{})

	rec := doResponses(t, f, fmt.Sprintf(`{"model":"full-model","input":"%s"}`, strings.Repeat("a", 41)), testKey, "req-resp-ctx")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("envelope must be invalid_request: %s", rec.Body.String())
	}
	if *f.calls != 0 {
		t.Fatalf("context-ceiling rejection reached the provider: calls = %d", *f.calls)
	}

	rec = doResponses(t, f, fmt.Sprintf(`{"model":"full-model","input":"%s"}`, strings.Repeat("a", 40)), testKey, "req-resp-ctx-ok")
	if rec.Code != http.StatusOK {
		t.Fatalf("input at the context ceiling must pass, got %d: %s", rec.Code, rec.Body.String())
	}
	if *f.calls != 1 {
		t.Fatalf("at-ceiling request provider calls = %d, want 1", *f.calls)
	}
}

// TestInputCeilingWithoutLimitsKeepsCurrentBehavior guards the compatibility
// contract: catalogs without ContextTokens and subjects without MaxInputTokens
// keep admitting exactly as before (R3 — no accidental rejections).
func TestInputCeilingWithoutLimitsKeepsCurrentBehavior(t *testing.T) {
	h, p := newInputCeilingHandler(t, testCaps(), policy.Limits{})
	for i := 0; i < 3; i++ {
		if rec := doChat(t, h, chatInputBody(2000), testKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d body = %s", i, rec.Code, rec.Body.String())
		}
	}
	if p.calls != 3 {
		t.Fatalf("provider calls = %d, want 3", p.calls)
	}
}
