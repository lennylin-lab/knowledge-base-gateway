package httpapi

// Tests for the V1.2 management surface: token-gated model/provider/policy/
// audit/usage queries, the model enable/disable switch, and the
// management-operation audit trail. Uses the in-memory management service.

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
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

const adminToken = "admin-secret-token"

type adminFixture struct {
	mux  *http.ServeMux
	mgmt *mgmt.MemoryService
	sink *audit.MemorySink
	svc  *gateway.Service // live resolution path for refresh assertions
	cap  *policy.Catalog
	pol  *policy.Policy // subject grants/limits backing the policies view
	deps AdminDeps      // rebuild the mux with extra wiring via newMux
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	manager := auth.NewManager(auth.NewStore())
	sink := audit.NewMemorySink(nil)
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gateway-echo", Provider: "fake", UpstreamModel: "up", Enabled: true,
			Capabilities: fullCaps, ConfigVersion: 2},
	})
	pol := policy.New()
	pol.Allow("subject_default", "gateway-echo")
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, time.Second, 0)
	svc.Routes.SetRoutes("gateway-echo", []router.Route{
		{ProviderName: "fake", Provider: provider.Fake{}, UpstreamModel: "up", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)},
	})
	service := mgmt.NewMemoryService(catalog, pol, svc.Routes, sink, []mgmt.ProviderView{
		{Name: "fake", Kind: "fake", Enabled: true},
	})
	deps := AdminDeps{Manager: manager, Logger: nil, Token: adminToken, Mgmt: service}
	return &adminFixture{mux: NewAdminMux(deps), mgmt: service, sink: sink, svc: svc, cap: catalog, pol: pol, deps: deps}
}

// newMux replaces the fixture's admin mux with new deps (e.g. a runtime
// refresh boundary) while reusing the same backing services.
func (f *adminFixture) newMux(mutate func(*AdminDeps)) {
	deps := f.deps
	mutate(&deps)
	f.mux = NewAdminMux(deps)
}

func doAdmin(f *adminFixture, method, path, token, body string) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

func TestAdminManagementRequiresToken(t *testing.T) {
	f := newAdminFixture(t)
	cases := []struct{ path, token string }{
		{"/admin/models", ""},
		{"/admin/models", "wrong"},
		{"/admin/providers", "wrong"},
		{"/admin/audit", "wrong"},
		{"/admin/usage", "wrong"},
		{"/admin/policies", "wrong"},
	}
	for _, tc := range cases {
		rec := doAdmin(f, http.MethodGet, tc.path, tc.token, "")
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s with bad token: status = %d", tc.path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), adminToken) {
			t.Errorf("response leaks the token: %s", rec.Body.String())
		}
	}
}

func TestAdminModelsViewIncludesProviderBindings(t *testing.T) {
	f := newAdminFixture(t)
	rec := doAdmin(f, http.MethodGet, "/admin/models", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Models []mgmt.ModelView `json:"models"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Models) != 1 || out.Models[0].PublicName != "gateway-echo" {
		t.Fatalf("models = %+v", out.Models)
	}
	if out.Models[0].Provider != "fake" || out.Models[0].UpstreamModel != "up" {
		t.Errorf("admin view must include bindings: %+v", out.Models[0])
	}
	if !out.Models[0].Capabilities.Responses {
		t.Errorf("capabilities missing: %+v", out.Models[0].Capabilities)
	}
}

func TestAdminModelDisableEnableAuditsOperation(t *testing.T) {
	f := newAdminFixture(t)
	rec := doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/disable", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("disable: %d body = %s", rec.Code, rec.Body.String())
	}
	// The disable must be visible in the catalog and in the audit log.
	models, _ := f.mgmt.Models(context.Background())
	if models[0].Enabled {
		t.Fatal("model must be disabled")
	}
	ops, _ := f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 1 || ops[0].Action != "model_disable" || ops[0].Target != "gateway-echo" {
		t.Fatalf("management ops = %+v", ops)
	}
	if !strings.Contains(string(ops[0].Detail), "false") {
		t.Errorf("op detail = %s", ops[0].Detail)
	}

	rec = doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/enable", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("enable: %d", rec.Code)
	}
	ops, _ = f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 2 || ops[0].Action != "model_enable" {
		t.Fatalf("ops after enable = %+v", ops)
	}

	// Unknown model: 404 and no audit record.
	rec = doAdmin(f, http.MethodPost, "/admin/models/nope/disable", adminToken, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown model: %d", rec.Code)
	}
	ops, _ = f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 2 {
		t.Fatalf("failed operations must not be audited: %+v", ops)
	}
}

func TestAdminAuditAndUsageQueries(t *testing.T) {
	f := newAdminFixture(t)
	// Seed audit events through the sink the service reads.
	prompt := 10
	f.sink.Write(audit.Event{
		RequestID: "req_admin_1", SubjectID: "subject_default", Model: "gateway-echo",
		Provider: "fake", Status: 200, LatencyMillis: 12, CreatedAt: time.Now(),
		PromptTokens: &prompt, CompletionTokens: &prompt, Protocol: "responses",
	})
	f.sink.Write(audit.Event{
		RequestID: "req_admin_2", SubjectID: "subject_other", Model: "gateway-echo",
		Provider: "fake", Status: 500, ErrorClass: "server", LatencyMillis: 40, CreatedAt: time.Now(),
	})

	rec := doAdmin(f, http.MethodGet, "/admin/audit?request_id=req_admin_1", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("audit query: %d", rec.Code)
	}
	var out struct {
		Events []audit.Event `json:"events"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Events) != 1 || out.Events[0].RequestID != "req_admin_1" || out.Events[0].Protocol != "responses" {
		t.Fatalf("audit events = %+v", out.Events)
	}

	rec = doAdmin(f, http.MethodGet, "/admin/usage?subject=subject_other", adminToken, "")
	var usage struct {
		Usage []mgmt.UsageRow `json:"usage"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &usage)
	if len(usage.Usage) != 1 || usage.Usage[0].Requests != 1 || usage.Usage[0].Errors != 1 {
		t.Fatalf("usage = %+v", usage.Usage)
	}

	rec = doAdmin(f, http.MethodGet, "/admin/management-log", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("management log: %d", rec.Code)
	}
}

func TestAdminPoliciesEndpoint(t *testing.T) {
	f := newAdminFixture(t)
	// Effective-limits visibility (issue #8 mitigation): the view must expose
	// the subject's folded ceilings so a tightening multi-row policy is
	// visible to operators instead of silently resizing the subject.
	f.pol.SetLimits("subject_default", policy.Limits{
		RatePerMinute: 30, MaxConcurrent: 2, DailyTokens: 5000, MaxInputTokens: 2048,
	})
	rec := doAdmin(f, http.MethodGet, "/admin/policies?subject=subject_default", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("policies: %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "gateway-echo") {
		t.Fatalf("policy rows missing: %s", rec.Body.String())
	}
	var out struct {
		Policies []mgmt.PolicyView `json:"policies"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Policies) != 1 {
		t.Fatalf("policies = %+v", out.Policies)
	}
	// The effective block mirrors the folded enforcement limits; undeclared
	// caps stay zero (uncapped), never a fabricated value.
	want := mgmt.EffectiveLimits{
		RatePerMinute: 30, MaxConcurrent: 2, DailyTokens: 5000, MaxInputTokens: 2048,
	}
	if got := out.Policies[0].EffectiveLimits; got != want {
		t.Fatalf("effective limits = %+v, want %+v", got, want)
	}
}

func TestAdminManagementDisabledWithoutToken(t *testing.T) {
	manager := auth.NewManager(auth.NewStore())
	mux := NewAdminMux(AdminDeps{Manager: manager, Token: "", Mgmt: mgmt.NewMemoryService(policy.NewCatalog(nil), policy.New(), nil, audit.NewMemorySink(nil), nil)})
	req := httptest.NewRequest(http.MethodGet, "/admin/models", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("admin must be disabled without a token, got %d", rec.Code)
	}
}

// TestAdminMgmtRedaction asserts that management responses never contain
// base URLs, secrets, or prompt-shaped content.
func TestAdminMgmtRedaction(t *testing.T) {
	f := newAdminFixture(t)
	f.sink.Write(audit.Event{RequestID: "req_r", SubjectID: "s", Model: "gateway-echo",
		Provider: "fake", Status: 200, LatencyMillis: 1, CreatedAt: time.Now()})
	for _, path := range []string{"/admin/models", "/admin/providers", "/admin/policies", "/admin/audit", "/admin/usage", "/admin/management-log"} {
		rec := doAdmin(f, http.MethodGet, path, adminToken, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
		lower := strings.ToLower(rec.Body.String())
		for _, leaked := range []string{"bearer ", "internal://", "postgresql://", "api_key", "password", "sk-test"} {
			if strings.Contains(lower, leaked) {
				t.Errorf("%s leaks %q: %s", path, leaked, rec.Body.String())
			}
		}
	}
}
