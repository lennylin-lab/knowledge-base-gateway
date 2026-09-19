package httpapi

// Admin API cost-governance surface: /admin/prices and /admin/budgets are
// disabled (404) without an AccountingAdmin, mutations validate the payload
// against the schema's invariants, and /admin/usage carries the budgets
// section only when the surface is wired (additive evolution).

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

type accountingAdminStub struct {
	prices  []mgmt.PriceView
	budgets []mgmt.BudgetView
	usage   []mgmt.BudgetUsageView
	fail    bool
}

func (s *accountingAdminStub) ListPrices(context.Context) ([]mgmt.PriceView, error) {
	if s.fail {
		return nil, errors.New("boom")
	}
	return s.prices, nil
}

func (s *accountingAdminStub) UpsertPrice(_ context.Context, in mgmt.PriceInput, _ mgmt.AdminOp) error {
	if s.fail {
		return errors.New("boom")
	}
	s.prices = append(s.prices, mgmt.PriceView{
		Provider: in.Provider, PublicModel: in.PublicModel, PriceVersion: in.PriceVersion,
		Currency: in.Currency, InputMicrosPerToken: in.InputMicrosPerToken,
		OutputMicrosPerToken: in.OutputMicrosPerToken, EffectiveFrom: in.EffectiveFrom,
	})
	return nil
}

func (s *accountingAdminStub) ListBudgets(_ context.Context, _ string) ([]mgmt.BudgetView, error) {
	return s.budgets, nil
}

func (s *accountingAdminStub) UpsertBudget(_ context.Context, in mgmt.BudgetInput, _ mgmt.AdminOp) error {
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	s.budgets = append(s.budgets, mgmt.BudgetView{
		Scope: in.Scope, SubjectID: in.SubjectID, TenantID: in.TenantID,
		Period: in.Period, Currency: in.Currency, AmountMicros: in.AmountMicros, Enabled: enabled,
	})
	return nil
}

func (s *accountingAdminStub) BudgetUsage(_ context.Context, _ time.Time, _ string) ([]mgmt.BudgetUsageView, error) {
	return s.usage, nil
}

func adminAccountingRequest(t *testing.T, deps AdminDeps, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Authorization", "Bearer test-admin")
	rec := httptest.NewRecorder()
	NewAdminMux(deps).ServeHTTP(rec, req)
	return rec
}

func accountingTestDeps(stub *accountingAdminStub) AdminDeps {
	// /admin/usage lives under the mgmt surface, so a minimal memory
	// management service is wired alongside the accounting admin.
	mgmtSvc := mgmt.NewMemoryService(
		policy.NewCatalog(nil), policy.New(), nil, audit.NewMemorySink(nil), nil)
	return AdminDeps{
		Token:      "test-admin",
		Manager:    auth.NewManager(auth.NewStore()),
		Mgmt:       mgmtSvc,
		Accounting: stub,
	}
}

func TestAdminAccountingDisabledWithoutWiring(t *testing.T) {
	deps := AdminDeps{Token: "test-admin", Manager: auth.NewManager(auth.NewStore())}
	for _, path := range []string{"/admin/prices", "/admin/budgets"} {
		if rec := adminAccountingRequest(t, deps, http.MethodGet, path, ""); rec.Code != http.StatusNotFound {
			t.Fatalf("%s without wiring: %d", path, rec.Code)
		}
	}
}

func TestAdminPriceUpsertValidationAndList(t *testing.T) {
	stub := &accountingAdminStub{}
	deps := accountingTestDeps(stub)

	// Invalid payloads are rejected before any store call.
	for _, body := range []string{
		`{"provider":"","public_model":"m","price_version":1,"currency":"USD"}`,
		`{"provider":"p","public_model":"m","price_version":0,"currency":"USD"}`,
		`{"provider":"p","public_model":"m","price_version":1,"currency":"usd"}`,
		`{"provider":"p","public_model":"m","price_version":1,"currency":"USD","input_micros_per_token":-1}`,
		`{"provider":"p","public_model":"m","price_version":1,"currency":"USD","effective_from":"not-a-time"}`,
	} {
		if rec := adminAccountingRequest(t, deps, http.MethodPost, "/admin/prices", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("invalid price %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	// A valid upsert lands and lists.
	rec := adminAccountingRequest(t, deps, http.MethodPost, "/admin/prices",
		`{"provider":"fake","public_model":"gateway-echo","price_version":2,"currency":"USD",
		  "input_micros_per_token":10,"output_micros_per_token":20,
		  "reasoning_micros_per_token":5,"effective_from":"2026-01-01T00:00:00Z"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert: %d %s", rec.Code, rec.Body.String())
	}
	if len(stub.prices) != 1 || stub.prices[0].PriceVersion != 2 {
		t.Fatalf("upserted price: %+v", stub.prices)
	}
	// Bad tokens stay rejected (the guard still applies to new endpoints).
	req := httptest.NewRequest(http.MethodGet, "/admin/prices", nil)
	rec2 := httptest.NewRecorder()
	NewAdminMux(deps).ServeHTTP(rec2, req)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("unguarded prices endpoint: %d", rec2.Code)
	}
}

func TestAdminBudgetUpsertValidation(t *testing.T) {
	stub := &accountingAdminStub{}
	deps := accountingTestDeps(stub)
	for _, body := range []string{
		`{"scope":"team","tenant_id":"t","period":"daily","currency":"USD","amount_micros":10}`,
		`{"scope":"subject","tenant_id":"t","period":"daily","currency":"USD","amount_micros":10}`,
		`{"scope":"tenant","subject_id":"s","tenant_id":"t","period":"daily","currency":"USD","amount_micros":10}`,
		`{"scope":"subject","subject_id":"s","tenant_id":"t","period":"weekly","currency":"USD","amount_micros":10}`,
		`{"scope":"subject","subject_id":"s","tenant_id":"t","period":"daily","currency":"USD","amount_micros":0}`,
	} {
		if rec := adminAccountingRequest(t, deps, http.MethodPost, "/admin/budgets", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("invalid budget %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	rec := adminAccountingRequest(t, deps, http.MethodPost, "/admin/budgets",
		`{"scope":"subject","subject_id":"s1","tenant_id":"t1","period":"daily","currency":"USD","amount_micros":1000000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert: %d %s", rec.Code, rec.Body.String())
	}
	if len(stub.budgets) != 1 || stub.budgets[0].AmountMicros != 1_000_000 || !stub.budgets[0].Enabled {
		t.Fatalf("upserted budget: %+v", stub.budgets)
	}
}

func TestAdminUsageIncludesBudgetsWhenWired(t *testing.T) {
	stub := &accountingAdminStub{usage: []mgmt.BudgetUsageView{{
		Scope: "subject", SubjectID: "s1", TenantID: "t1", Period: "daily",
		Currency: "USD", LimitMicros: 100, UsedMicros: 40, UnknownCostSettlements: 2,
	}}}
	deps := accountingTestDeps(stub)
	rec := adminAccountingRequest(t, deps, http.MethodGet, "/admin/usage", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("usage: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"budgets"`) || !strings.Contains(rec.Body.String(), `"unknown_cost_settlements":2`) {
		t.Fatalf("budgets section missing: %s", rec.Body.String())
	}

	// Without the accounting surface the section is absent (additive).
	bare := AdminDeps{
		Token:   "test-admin",
		Manager: auth.NewManager(auth.NewStore()),
		Mgmt: mgmt.NewMemoryService(
			policy.NewCatalog(nil), policy.New(), nil, audit.NewMemorySink(nil), nil),
	}
	if rec := adminAccountingRequest(t, bare, http.MethodGet, "/admin/usage", ""); rec.Code != http.StatusOK {
		t.Fatalf("bare usage: %d", rec.Code)
	} else if strings.Contains(rec.Body.String(), `"budgets"`) {
		t.Fatalf("unwired usage must omit budgets: %s", rec.Body.String())
	}
}
