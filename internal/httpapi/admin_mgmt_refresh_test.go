package httpapi

// Tests for the management mutation contract: the audited model enable/disable
// switch must affect live model resolution through the injected runtime
// refresh boundary, a refresh failure after persistence must be reported as
// exactly that (with the audit evidence left in place), and a failed mutation
// must change nothing and record nothing.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// failingMutationService wraps a mgmt.Service and fails the atomic
// mutation+audit operation, simulating a store whose transaction could not
// commit (e.g. the audit insert rejected the row and rolled everything back).
type failingMutationService struct {
	mgmt.Service
	err error
}

func (s *failingMutationService) SetModelEnabledWithAudit(ctx context.Context, publicModel string, enabled bool, op mgmt.AdminOp) error {
	return s.err
}

// resolveOK reports whether the fixture's live gateway still resolves the
// model for the granted subject.
func resolveOK(f *adminFixture, model string) error {
	_, err := f.svc.Resolve("subject_default", model)
	return err
}

// TestAdminModelToggleAffectsLiveResolution pins AC1 at the handler boundary:
// after the audited disable, the same running process must refuse the model,
// and after re-enable it must serve again — no restart. The fixture wires the
// same injected refresh boundary the process wiring uses.
func TestAdminModelToggleAffectsLiveResolution(t *testing.T) {
	f := newAdminFixture(t)
	f.newMux(func(d *AdminDeps) {
		d.ApplyModelChange = func(_ context.Context, name string, enabled bool) error {
			if !f.cap.SetEnabled(name, enabled) {
				return errors.New("model missing from runtime catalog")
			}
			return nil
		}
	})

	if err := resolveOK(f, "gateway-echo"); err != nil {
		t.Fatalf("model must resolve before the toggle: %v", err)
	}

	rec := doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/disable", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("disable: %d body = %s", rec.Code, rec.Body.String())
	}
	if err := resolveOK(f, "gateway-echo"); !errors.Is(err, gateway.ErrUnknownModel) {
		t.Fatalf("disabled model must fail live resolution, got %v", err)
	}

	rec = doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/enable", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("enable: %d body = %s", rec.Code, rec.Body.String())
	}
	if err := resolveOK(f, "gateway-echo"); err != nil {
		t.Fatalf("re-enabled model must resolve again, got %v", err)
	}

	// The toggles stay management-audited.
	ops, _ := f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 2 || ops[1].Action != "model_disable" || ops[0].Action != "model_enable" {
		t.Fatalf("ops = %+v", ops)
	}
}

// TestAdminToggleRefreshFailureKeepsAuditEvidence pins the refresh-failure
// semantics: the mutation and its audit record are committed, so the handler
// must report refresh_failed (not a generic update failure) and the evidence
// must stay in place for operators.
func TestAdminToggleRefreshFailureKeepsAuditEvidence(t *testing.T) {
	f := newAdminFixture(t)
	f.newMux(func(d *AdminDeps) {
		d.ApplyModelChange = func(_ context.Context, _ string, _ bool) error {
			return errors.New("route table unavailable")
		}
	})
	rec := doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/disable", adminToken, "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || env.Error.Code != "refresh_failed" {
		t.Fatalf("error envelope = %s (err %v)", rec.Body.String(), err)
	}
	// Evidence: the operation is audited and the persisted state is disabled.
	ops, _ := f.mgmt.Ops(context.Background(), 10)
	if len(ops) != 1 || ops[0].Action != "model_disable" {
		t.Fatalf("audit evidence missing after refresh failure: %+v", ops)
	}
	models, _ := f.mgmt.Models(context.Background())
	if models[0].Enabled {
		t.Fatal("persisted state must stay disabled after a refresh failure")
	}
}

// TestAdminToggleMutationFailureChangesNothing pins the atomic-mutation
// contract: when the store cannot commit the mutation plus its audit record,
// the handler reports a failed operation, the model keeps its previous state,
// and no management-operation record exists.
func TestAdminToggleMutationFailureChangesNothing(t *testing.T) {
	f := newAdminFixture(t)
	inner := f.mgmt
	f.newMux(func(d *AdminDeps) {
		d.Mgmt = &failingMutationService{Service: inner, err: errors.New("audit insert rejected")}
	})

	rec := doAdmin(f, http.MethodPost, "/admin/models/gateway-echo/disable", adminToken, "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || env.Error.Code != "update_failed" {
		t.Fatalf("error envelope = %s (err %v)", rec.Body.String(), err)
	}

	models, _ := inner.Models(context.Background())
	if !models[0].Enabled {
		t.Fatal("failed mutation must leave the model enabled")
	}
	ops, _ := inner.Ops(context.Background(), 10)
	if len(ops) != 0 {
		t.Fatalf("failed mutation must not be audited: %+v", ops)
	}
	if err := resolveOK(f, "gateway-echo"); err != nil {
		t.Fatalf("failed mutation must not affect live resolution: %v", err)
	}
}

// TestAdminProvidersRuntimeOverlayAndRecentErrors pins the operational
// provider contract: registry rows gain live breaker/health state through the
// injected runtime overlay, and the recent error summary comes from the audit
// trail. Providers the process has no routes for report breaker "none".
func TestAdminProvidersRuntimeOverlayAndRecentErrors(t *testing.T) {
	f := newAdminFixture(t)
	f.sink.Write(auditEvent("req_p1", "fake", 500, "server", time.Now()))
	f.sink.Write(auditEvent("req_p2", "fake", 200, "", time.Now()))
	f.sink.Write(auditEvent("req_p3", "fake", 429, "rate_limited", time.Now().Add(-48*time.Hour))) // outside window

	f.newMux(func(d *AdminDeps) {
		d.ProviderRuntime = func(name string) (mgmt.ProviderRuntime, bool) {
			if name == "fake" {
				return mgmt.ProviderRuntime{BreakerState: mgmt.BreakerOpen, TotalRoutes: 2, OpenRoutes: 1}, true
			}
			return mgmt.ProviderRuntime{}, true
		}
	})
	rec := doAdmin(f, http.MethodGet, "/admin/providers", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("providers: %d body = %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Providers []mgmt.ProviderView `json:"providers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Providers) != 1 {
		t.Fatalf("providers = %+v", out.Providers)
	}
	p := out.Providers[0]
	if p.Health != mgmt.HealthDegraded || p.BreakerState != mgmt.BreakerOpen {
		t.Fatalf("health/breaker = %s/%s, want degraded/open", p.Health, p.BreakerState)
	}
	// In-window errors: req_p1 only (req_p2 succeeded; req_p3 is 48h old).
	if p.RecentErrors != 1 || p.LastErrorClass != "server" {
		t.Fatalf("recent errors = %d/%s, want 1/server", p.RecentErrors, p.LastErrorClass)
	}
}

// TestAdminProvidersWithoutRuntimeWiring pins the honest default: without the
// runtime overlay the breaker fields say "unknown" instead of masquerading as
// measured state.
func TestAdminProvidersWithoutRuntimeWiring(t *testing.T) {
	f := newAdminFixture(t)
	rec := doAdmin(f, http.MethodGet, "/admin/providers", adminToken, "")
	var out struct {
		Providers []mgmt.ProviderView `json:"providers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Providers[0].Health != mgmt.HealthUnknown || out.Providers[0].BreakerState != mgmt.BreakerUnknown {
		t.Fatalf("unwired provider = %+v, want unknown health/breaker", out.Providers[0])
	}
}

// TestAdminUsageErrorRateAndStagedFields pins the usage metric contract:
// error_rate is computed, and the staged first-token/cost fields are present
// and null (never fabricated numbers under misleading names).
func TestAdminUsageErrorRateAndStagedFields(t *testing.T) {
	f := newAdminFixture(t)
	prompt := 5
	f.sink.Write(audit.Event{
		RequestID: "req_u1", SubjectID: "subject_default", Model: "gateway-echo",
		Provider: "fake", Status: 200, LatencyMillis: 10, CreatedAt: time.Now(),
		PromptTokens: &prompt, CompletionTokens: &prompt, Protocol: "chat",
	})
	f.sink.Write(audit.Event{
		RequestID: "req_u2", SubjectID: "subject_default", Model: "gateway-echo",
		Provider: "fake", Status: 500, ErrorClass: "server", LatencyMillis: 30, CreatedAt: time.Now(),
	})

	rec := doAdmin(f, http.MethodGet, "/admin/usage", adminToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("usage: %d", rec.Code)
	}
	var raw struct {
		Usage []struct {
			mgmt.UsageRow
			FirstTokenP50 *int64 `json:"first_token_p50_ms"`
			FirstTokenP95 *int64 `json:"first_token_p95_ms"`
			CostMicros    *int64 `json:"cost_micros"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Usage) != 1 {
		t.Fatalf("usage rows = %d", len(raw.Usage))
	}
	row := raw.Usage[0]
	if row.Requests != 2 || row.Errors != 1 {
		t.Fatalf("requests/errors = %d/%d, want 2/1", row.Requests, row.Errors)
	}
	if row.ErrorRate != 0.5 {
		t.Fatalf("error_rate = %v, want 0.5", row.ErrorRate)
	}
	if raw.Usage[0].FirstTokenP50 != nil || raw.Usage[0].FirstTokenP95 != nil || raw.Usage[0].CostMicros != nil {
		t.Fatalf("staged fields must stay null until recorded: %+v", raw.Usage[0])
	}
	for _, name := range []string{"first_token_p50_ms", "first_token_p95_ms", "cost_micros"} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Errorf("staged field %s must be present (as null) in the contract", name)
		}
	}
}

// auditEvent is a small helper for provider-summary fixtures.
func auditEvent(requestID, providerName string, status int, errorClass string, at time.Time) audit.Event {
	return audit.Event{
		RequestID: requestID, SubjectID: "subject_default", Model: "gateway-echo",
		Provider: providerName, Status: status, ErrorClass: errorClass,
		LatencyMillis: 7, CreatedAt: at,
	}
}
