package httpapi

// Pins the authentication ordering contract (error-handling.md): invalid,
// expired, and revoked keys receive their 401 before ANY body read, on both
// protocols — an unauthenticated caller must not drive bounded parse work,
// and an invalid key plus a malformed or oversized body must never surface
// as 400. Valid-key requests keep the bounded-decode 400 behavior.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// newOrderChatHandler wires a chat handler with a bounded body limit.
func newOrderChatHandler(t *testing.T, maxBody int64) (*ChatHandler, *int, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-o", Subject: "subject-o", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "m", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: testCaps()},
	})
	pol := policy.New()
	pol.AllowAll("subject-o")
	calls := 0
	counting := &callCounter{inner: provider.Fake{}, calls: &calls}
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": counting}, 2*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Audit: sink, Metrics: metrics.New(),
		MaxBody: maxBody, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, &calls, sink
}

func TestChatInvalidKeyWithMalformedBodyIs401BeforeBodyRead(t *testing.T) {
	h, calls, sink := newOrderChatHandler(t, 1<<20)
	// Malformed, oversized, and hostile bodies must make no difference: the
	// key is rejected before a single byte of the body is read.
	bodies := []string{
		`{not json at all`,
		`{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("a", 100_000) + `"}]}`,
		strings.Repeat("\x00", 4096),
	}
	for i, body := range bodies {
		rec := doChat(t, h, body, "sk-wrong-key", "req-order-1")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("body %d: status = %d, want 401 (auth must precede body read)", i, rec.Code)
		}
		var env APIError
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("body %d: bad envelope: %v", i, err)
		}
		if env.Error.Type != "authentication_error" || env.Error.Code != "invalid_api_key" {
			t.Fatalf("body %d: envelope = %+v", i, env.Error)
		}
		if env.Error.RequestID != "req-order-1" {
			t.Fatalf("body %d: request id not echoed: %+v", i, env.Error)
		}
	}
	if *calls != 0 {
		t.Fatalf("provider called %d times for unauthenticated requests", *calls)
	}
	// The denial is still audited (metadata only, empty principal).
	events := sink.Snapshot()
	if len(events) != len(bodies) {
		t.Fatalf("audit events = %d, want %d", len(events), len(bodies))
	}
	for _, e := range events {
		if e.SubjectID != "" || e.Model != "" {
			t.Fatalf("denial audit must carry no principal/model detail: %+v", e)
		}
	}
}

func TestChatValidKeyOversizedBodyIsBoundedDecode400(t *testing.T) {
	// Valid key, body larger than the configured bound: the bounded decode
	// still rejects with the stable 400 invalid_request envelope.
	h, calls, _ := newOrderChatHandler(t, 64)
	big := `{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("a", 4096) + `"}]}`
	rec := doChat(t, h, big, testKey, "req-order-2")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for oversized body", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if *calls != 0 {
		t.Fatalf("oversized body reached the provider (%d calls)", *calls)
	}
}

func TestResponsesInvalidKeyWithMalformedBodyIs401BeforeBodyRead(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	bodies := []string{
		`{definitely not json`,
		`{"model":"full-model","input":"` + strings.Repeat("a", 100_000) + `"}`,
		`{"unknown_field":` + strings.Repeat("[", 2048),
	}
	for i, body := range bodies {
		rec := doResponses(t, f, body, "sk-wrong-key", "req-order-3")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("body %d: status = %d, want 401 (auth must precede body read)", i, rec.Code)
		}
		var env APIError
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("body %d: bad envelope: %v", i, err)
		}
		if env.Error.Type != "authentication_error" || env.Error.Code != "invalid_api_key" {
			t.Fatalf("body %d: envelope = %+v", i, env.Error)
		}
	}
	if *f.calls != 0 {
		t.Fatalf("provider called %d times for unauthenticated requests", *f.calls)
	}
	// A valid key plus a malformed body keeps the bounded-decode 400.
	rec := doResponses(t, f, `{definitely not json`, testKey, "req-order-4")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("valid key + malformed body: status = %d, want 400", rec.Code)
	}
	if *f.calls != 0 {
		t.Fatal("malformed body reached the provider")
	}
}
