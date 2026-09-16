package httpapi

// Tests for the subject default-model feature: protocol-aware backfill in
// admission (chat/responses → default_model, embeddings →
// default_embedding_model), audit correlation with the resolved model, the
// non-leaky 400 when neither default nor model is configured, and the admin
// default-model mutation with its management-audit path.

import (
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
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// defaultModelFixture wires chat/responses/embeddings handlers over one
// catalog with two models (chat and embeddings) and one subject.
type defaultModelFixture struct {
	chat      *ChatHandler
	responses *ResponsesHandler
	embed     *EmbeddingsHandler
	sink      *audit.MemorySink
	pol       *policy.Policy
}

func newDefaultModelFixture(t *testing.T, defaults policy.Limits, grants map[string]string) *defaultModelFixture {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-d", Subject: "subject-d", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up-chat", Enabled: true, Capabilities: testCaps()},
		{PublicName: embedModel, Provider: "fake", UpstreamModel: "up-embed", Enabled: true, Capabilities: embedCaps},
		{PublicName: "other-model", Provider: "fake", UpstreamModel: "up-other", Enabled: true, Capabilities: testCaps()},
	})
	pol := policy.New()
	for model := range grants {
		pol.Allow("subject-d", model)
	}
	pol.SetLimits("subject-d", defaults)
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, 2*time.Second, 0)
	svc.Routes.SetRoutes("gpt-test", []router.Route{{ProviderName: "fake", Provider: provider.Fake{}, UpstreamModel: "up-chat", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)}})
	svc.Routes.SetRoutes(embedModel, []router.Route{{ProviderName: "fake", Provider: provider.Fake{}, UpstreamModel: "up-embed", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)}})
	svc.Routes.SetRoutes("other-model", []router.Route{{ProviderName: "fake", Provider: provider.Fake{}, UpstreamModel: "up-other", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)}})
	sink := audit.NewMemorySink(nil)
	shared := func() admissionDepsFields {
		return admissionDepsFields{
			Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
			Quota: quota.NewMemory(), Audit: sink, Metrics: metrics.New(),
		}
	}
	f := &defaultModelFixture{sink: sink, pol: pol}
	cf := shared()
	f.chat = &ChatHandler{Auth: cf.Auth, Service: cf.Service, Policy: cf.Policy, Limiter: cf.Limiter,
		Quota: cf.Quota, Audit: cf.Audit, Metrics: cf.Metrics,
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000}
	rf := shared()
	f.responses = &ResponsesHandler{Auth: rf.Auth, Service: rf.Service, Policy: rf.Policy, Limiter: rf.Limiter,
		Quota: rf.Quota, Audit: rf.Audit, Metrics: rf.Metrics,
		MaxBody: 1 << 20, MaxItems: 128, MaxChars: 32_000}
	ef := shared()
	f.embed = &EmbeddingsHandler{Auth: ef.Auth, Service: ef.Service, Policy: ef.Policy, Limiter: ef.Limiter,
		Quota: ef.Quota, Audit: ef.Audit, Metrics: ef.Metrics,
		MaxBody: 1 << 20, MaxItems: 128, MaxChars: 32_000}
	return f
}

// admissionDepsFields mirrors admissionDeps without importing the type twice.
type admissionDepsFields struct {
	Auth    Authenticator
	Service *gateway.Service
	Policy  *policy.Policy
	Limiter limiter.Gate
	Quota   quota.Gate
	Audit   audit.Sink
	Metrics *metrics.Registry
}

func doResponsesRaw(t *testing.T, h *ResponsesHandler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestDefaultModelBackfillChat(t *testing.T) {
	f := newDefaultModelFixture(t, policy.Limits{DefaultModel: "gpt-test"}, map[string]string{"gpt-test": "x"})
	// No model in the request: backfilled from the subject default.
	rec := doChat(t, f.chat, `{"messages":[{"role":"user","content":"hello"}]}`, testKey, "req-backfill-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Model != "gpt-test" {
		t.Fatalf("audit must record the resolved model: %+v", events)
	}
}

func TestDefaultModelBackfillResponses(t *testing.T) {
	f := newDefaultModelFixture(t, policy.Limits{DefaultModel: "gpt-test"}, map[string]string{"gpt-test": "x"})
	rec := doResponsesRaw(t, f.responses, `{"input":"hello"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Model != "gpt-test" || events[0].Protocol != "responses" {
		t.Fatalf("audit = %+v", events)
	}
}

func TestDefaultModelBackfillEmbeddings(t *testing.T) {
	f := newDefaultModelFixture(t, policy.Limits{DefaultEmbeddingModel: embedModel}, map[string]string{embedModel: "x"})
	rec := doEmbeddings(t, f.embed, `{"input":"hello"}`, testKey, "req-backfill-e")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Model != embedModel || events[0].Protocol != "embeddings" {
		t.Fatalf("audit = %+v", events)
	}
}

func TestDefaultModelSlotsAreProtocolAware(t *testing.T) {
	// The embedding default must not satisfy a chat request and vice versa.
	f := newDefaultModelFixture(t, policy.Limits{DefaultEmbeddingModel: embedModel}, map[string]string{embedModel: "x"})
	rec := doChat(t, f.chat, `{"messages":[{"role":"user","content":"hello"}]}`, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("chat must not use the embedding default: status = %d", rec.Code)
	}
	f2 := newDefaultModelFixture(t, policy.Limits{DefaultModel: "gpt-test"}, map[string]string{"gpt-test": "x"})
	rec = doEmbeddings(t, f2.embed, `{"input":"hello"}`, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("embeddings must not use the chat default: status = %d", rec.Code)
	}
}

func TestExplicitModelKeepsCurrentBehavior(t *testing.T) {
	f := newDefaultModelFixture(t, policy.Limits{DefaultModel: "gpt-test"}, map[string]string{"gpt-test": "x", "other-model": "x"})
	rec := doChat(t, f.chat, chatBody("other-model", false), testKey, "req-explicit")
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit model must win: status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Model != "other-model" {
		t.Fatalf("audit = %+v", events)
	}
}

func TestNoDefaultNoModelIsStable400(t *testing.T) {
	f := newDefaultModelFixture(t, policy.Limits{}, map[string]string{"gpt-test": "x"})
	rec := doChat(t, f.chat, `{"messages":[{"role":"user","content":"hello"}]}`, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("chat status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("chat envelope = %s", rec.Body.String())
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Status != 0 || events[0].ErrorClass == "" {
		t.Fatalf("denial must be audited: %+v", events)
	}
}

func TestDefaultModelStillRespectsGrants(t *testing.T) {
	// The backfill resolves the model name, not a permission bypass: a
	// default pointing at an ungranted model stays the non-leaky 403.
	f := newDefaultModelFixture(t, policy.Limits{DefaultModel: "other-model"}, map[string]string{"gpt-test": "x"})
	rec := doChat(t, f.chat, `{"messages":[{"role":"user","content":"hello"}]}`, testKey, "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("envelope = %s", rec.Body.String())
	}
}

// --- Admin default-model mutation ------------------------------------------

func TestAdminSetDefaultModelMutation(t *testing.T) {
	f := newAdminFixture(t)
	rec := doAdmin(f, http.MethodPost, "/admin/policies/subject_default/default-model", adminToken,
		`{"model":"gateway-echo","kind":"chat"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	// The view surfaces the default on the subject's policy rows.
	rec = doAdmin(f, http.MethodGet, "/admin/policies?subject=subject_default", adminToken, "")
	var out struct {
		Policies []mgmt.PolicyView `json:"policies"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Policies) != 1 || out.Policies[0].DefaultModel != "gateway-echo" {
		t.Fatalf("policies = %+v", out.Policies)
	}
	// The mutation is management-audited.
	ops, err := f.mgmt.Ops(context.Background(), 10)
	if err != nil || len(ops) != 1 {
		t.Fatalf("ops = %+v err=%v", ops, err)
	}
	if ops[0].Action != "default_model_chat" || ops[0].Target != "subject_default" {
		t.Fatalf("op = %+v", ops[0])
	}
	// And the embedding slot is set independently.
	rec = doAdmin(f, http.MethodPost, "/admin/policies/subject_default/default-model", adminToken,
		`{"model":"gateway-echo","kind":"embedding"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("embedding slot: status = %d body = %s", rec.Code, rec.Body.String())
	}
	rec = doAdmin(f, http.MethodGet, "/admin/policies?subject=subject_default", adminToken, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Policies[0].DefaultModel != "gateway-echo" || out.Policies[0].DefaultEmbeddingModel != "gateway-echo" {
		t.Fatalf("both slots must be set independently: %+v", out.Policies[0])
	}
}

func TestAdminSetDefaultModelFailures(t *testing.T) {
	f := newAdminFixture(t)
	cases := []struct {
		name, subject, body string
		want                int
		code                string
	}{
		{"unknown model", "subject_default", `{"model":"no-such","kind":"chat"}`, http.StatusNotFound, "not_found"},
		{"unknown subject", "nobody", `{"model":"gateway-echo","kind":"chat"}`, http.StatusNotFound, "not_found"},
		{"bad kind", "subject_default", `{"model":"gateway-echo","kind":"other"}`, http.StatusBadRequest, "invalid_request"},
		{"missing model", "subject_default", `{"kind":"chat"}`, http.StatusBadRequest, "invalid_request"},
	}
	for _, tc := range cases {
		rec := doAdmin(f, http.MethodPost, "/admin/policies/"+tc.subject+"/default-model", adminToken, tc.body)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (%s)", tc.name, rec.Code, tc.want, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), tc.code) {
			t.Errorf("%s: body = %s, want code %q", tc.name, rec.Body.String(), tc.code)
		}
	}
	// Nothing was audited: failures mean nothing changed.
	ops, _ := f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 0 {
		t.Errorf("failed mutations must not be audited: %+v", ops)
	}
}

func TestAdminSetDefaultModelRefreshBoundary(t *testing.T) {
	f := newAdminFixture(t)
	refreshes := 0
	f.newMux(func(d *AdminDeps) {
		d.ApplyPolicyChange = func(context.Context, string) error {
			refreshes++
			return nil
		}
	})
	rec := doAdmin(f, http.MethodPost, "/admin/policies/subject_default/default-model", adminToken,
		`{"model":"gateway-echo","kind":"chat"}`)
	if rec.Code != http.StatusOK || refreshes != 1 {
		t.Fatalf("status = %d refreshes = %d body = %s", rec.Code, refreshes, rec.Body.String())
	}

	// refresh_failed: the mutation committed (and is audited) but the running
	// process may be divergent — the two failure classes never collapse.
	f.newMux(func(d *AdminDeps) {
		d.ApplyPolicyChange = func(context.Context, string) error { return context.DeadlineExceeded }
	})
	rec = doAdmin(f, http.MethodPost, "/admin/policies/subject_default/default-model", adminToken,
		`{"model":"gateway-echo","kind":"embedding"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("refresh failure status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "refresh_failed") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	ops, _ := f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 2 {
		t.Fatalf("the committed mutation must still be audited: %+v", ops)
	}
}
