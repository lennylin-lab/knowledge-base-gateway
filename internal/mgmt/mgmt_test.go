package mgmt

// Unit tests for the development-mode management service: the atomic
// mutation+audit operation, and the provider view enrichment with the recent
// error summary and honest "unknown" runtime defaults.

import (
	"context"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

func newTestService() (*MemoryService, *policy.Catalog, *audit.MemorySink) {
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gateway-echo", Provider: "fake", UpstreamModel: "up", Enabled: true, ConfigVersion: 1},
	})
	sink := audit.NewMemorySink(nil)
	svc := NewMemoryService(catalog, policy.New(), nil, sink, []ProviderView{
		{Name: "fake", Kind: "fake", Enabled: true},
	})
	return svc, catalog, sink
}

// TestMemoryMutationIsAtomicAndLive pins the dev-mode mutation: unknown
// models fail with nothing recorded, known models flip the shared catalog
// (immediately live for resolution) and append exactly one audited op.
func TestMemoryMutationIsAtomicAndLive(t *testing.T) {
	svc, catalog, _ := newTestService()
	ctx := context.Background()

	if err := svc.SetModelEnabledWithAudit(ctx, "nope", false, AdminOp{Action: "model_disable", Target: "nope"}); err != ErrNotFound {
		t.Fatalf("unknown model must return ErrNotFound, got %v", err)
	}
	if ops, _ := svc.Ops(ctx, 10); len(ops) != 0 {
		t.Fatalf("failed mutation must not be audited: %+v", ops)
	}

	if err := svc.SetModelEnabledWithAudit(ctx, "gateway-echo", false, AdminOp{Action: "model_disable", Target: "gateway-echo"}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, ok := catalog.Lookup("gateway-echo"); ok {
		t.Fatal("disable must be live in the shared catalog")
	}
	ops, _ := svc.Ops(ctx, 10)
	if len(ops) != 1 {
		t.Fatalf("ops = %+v, want exactly one", ops)
	}
	if ops[0].AdminSubject != "admin-token" || ops[0].CreatedAt.IsZero() {
		t.Fatalf("op defaults missing: %+v", ops[0])
	}
}

// TestMemoryProvidersEnrichment pins the provider view contract for the
// memory service: unknown runtime state stays explicit, and the recent error
// summary is derived from the audit sink inside the window.
func TestMemoryProvidersEnrichment(t *testing.T) {
	svc, _, sink := newTestService()
	prompt := 3
	sink.Write(audit.Event{RequestID: "r1", Provider: "fake", Status: 503,
		ErrorClass: "server", CreatedAt: time.Now(), PromptTokens: &prompt})
	sink.Write(audit.Event{RequestID: "r2", Provider: "other", Status: 500,
		ErrorClass: "server", CreatedAt: time.Now().Add(-48 * time.Hour)}) // outside window

	providers, err := svc.Providers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 1 {
		t.Fatalf("providers = %+v", providers)
	}
	p := providers[0]
	if p.Health != HealthUnknown || p.BreakerState != BreakerUnknown {
		t.Fatalf("runtime fields must read unknown before overlay, got %+v", p)
	}
	if p.RecentErrors != 1 || p.LastErrorClass != "server" {
		t.Fatalf("recent errors = %d/%s, want 1/server", p.RecentErrors, p.LastErrorClass)
	}
}

// TestApplyRuntime pins the health derivation: disabled wins over runtime,
// any open breaker degrades, all-closed serves, and no live routes report
// breaker "none" while staying serving.
func TestApplyRuntime(t *testing.T) {
	cases := []struct {
		name        string
		enabled     bool
		rt          ProviderRuntime
		wantHealth  string
		wantBreaker string
	}{
		{"disabled", false, ProviderRuntime{BreakerState: BreakerOpen, OpenRoutes: 1, TotalRoutes: 1}, HealthDisabled, BreakerNone},
		{"degraded", true, ProviderRuntime{BreakerState: BreakerOpen, OpenRoutes: 1, TotalRoutes: 2}, HealthDegraded, BreakerOpen},
		{"serving", true, ProviderRuntime{BreakerState: BreakerClosed, TotalRoutes: 2}, HealthServing, BreakerClosed},
		{"no routes", true, ProviderRuntime{}, HealthServing, BreakerNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := ProviderView{Name: "p", Enabled: tc.enabled}
			v.ApplyRuntime(tc.rt)
			if v.Health != tc.wantHealth || v.BreakerState != tc.wantBreaker {
				t.Fatalf("got %s/%s, want %s/%s", v.Health, v.BreakerState, tc.wantHealth, tc.wantBreaker)
			}
		})
	}
}
