package httpapi

// Synchronous-path cost governance: monetary budgets deny before any
// provider work (429 budget_exceeded with the UTC Retry-After), a configured
// budget without an applicable price refuses as pricing_unavailable before
// provider work, enforcement off keeps serving while the ledger still
// records, and successful requests settle the one ledger row with the
// computed cost and price version.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// accountingSyncFixture is a minimal chat handler wired with the accounting
// gate over the ledgerStub double.
type accountingSyncFixture struct {
	handler *ChatHandler
	ledger  *ledgerStub
	calls   *int32
}

func newAccountingSyncFixture(t *testing.T, enforcement bool, price accounting.Price, hasPrice bool, budgets accounting.BudgetLimits) *accountingSyncFixture {
	t.Helper()
	ledger := &ledgerStub{price: price, hasPrice: hasPrice, budgetLimits: budgets, reserved: map[string]bool{}}
	gate := &accounting.Gate{
		Store: ledger, Budgets: accounting.NewMemoryBudget(),
		Enforcement: enforcement, Now: time.Now,
	}
	keyStore := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	keyStore.Put(auth.KeyRecord{ID: "key-a", Subject: "subject-r", Salt: salt,
		Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-r", "full-model")
	calls := int32(0)
	counting := &countingCalls{inner: provider.Fake{}, calls: &calls}
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": counting}, 5*time.Second, 0)
	handler := &ChatHandler{
		Auth: keyStore, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
		Quota: nil, Accounting: gate, Audit: audit.NewMemorySink(nil), Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return &accountingSyncFixture{handler: handler, ledger: ledger, calls: &calls}
}

func (f *accountingSyncFixture) post(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) (code string, body map[string]any) {
	t.Helper()
	var parsed struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode error envelope: %v (%s)", err, rec.Body.String())
	}
	var raw map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	return parsed.Error.Code, raw
}

// TestSyncBudgetExceededDeniesBeforeProvider: true exhaustion is a 429 with
// the stable code and a UTC Retry-After, and the provider is never called.
func TestSyncBudgetExceededDeniesBeforeProvider(t *testing.T) {
	// Budget of 1 micro: any estimate denies.
	f := newAccountingSyncFixture(t, true,
		accounting.Price{Version: 1, Currency: "USD", InputPerToken: 1, OutputPerToken: 1},
		true, accounting.BudgetLimits{Currency: "USD", SubjectDaily: 1})
	rec := f.post(t, `{"model":"full-model","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	if code, _ := decodeError(t, rec); code != "budget_exceeded" {
		t.Fatalf("code: %s", code)
	}
	if ra := rec.Header().Get("Retry-After"); ra == "" {
		t.Fatal("budget denial must carry a UTC Retry-After")
	}
	if got := *f.calls; got != 0 {
		t.Fatalf("the provider must not be called on a budget denial: %d", got)
	}
	if len(f.ledger.reserved) != 0 {
		t.Fatal("a denied request must not write a ledger row")
	}
}

// TestSyncPricingUnavailableRefusesBeforeProvider: a configured budget with
// no applicable price is the stable pricing_unavailable refusal, before any
// provider work.
func TestSyncPricingUnavailableRefusesBeforeProvider(t *testing.T) {
	f := newAccountingSyncFixture(t, true,
		accounting.Price{Version: 1, Currency: "USD", InputPerToken: 1, OutputPerToken: 1},
		false, accounting.BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000})
	rec := f.post(t, `{"model":"full-model","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	if code, _ := decodeError(t, rec); code != "pricing_unavailable" {
		t.Fatalf("code: %s", code)
	}
	if got := *f.calls; got != 0 {
		t.Fatalf("the provider must not be called when pricing is unavailable: %d", got)
	}
}

// TestSyncEnforcementOffStillCapturesLedger: with enforcement disabled the
// request is never denied (no price is required either), but the ledger
// still records the lifecycle — and the cost stays unknown because no price
// exists (never fabricated as zero).
func TestSyncEnforcementOffStillCapturesLedger(t *testing.T) {
	f := newAccountingSyncFixture(t, false,
		accounting.Price{Version: 1, Currency: "USD", InputPerToken: 1, OutputPerToken: 1},
		false, accounting.BudgetLimits{Currency: "USD", SubjectDaily: 1})
	rec := f.post(t, `{"model":"full-model","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(f.ledger.settled) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond) // settlement runs on a detached context
	}
	if len(f.ledger.settled) != 1 {
		t.Fatalf("ledger capture must continue: %d settled", len(f.ledger.settled))
	}
	q := f.ledger.settled[0]
	if q.CostMicros != nil {
		t.Fatalf("cost without a price must stay unknown, got %d", *q.CostMicros)
	}
	if q.Usage.PromptTokens == nil {
		t.Fatal("reported usage must still be recorded")
	}
}

// TestSyncSuccessSettlesCostAndVersion: a served request settles exactly one
// ledger row with the computed cost, price version, and reported usage.
func TestSyncSuccessSettlesCostAndVersion(t *testing.T) {
	price := accounting.Price{Version: 5, Currency: "USD", InputPerToken: 10, OutputPerToken: 30}
	f := newAccountingSyncFixture(t, true, price, true, accounting.BudgetLimits{}) // no budgets: capture only
	rec := f.post(t, `{"model":"full-model","messages":[{"role":"user","content":"abc"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(f.ledger.settled) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(f.ledger.settled) != 1 {
		t.Fatalf("exactly one settlement: %d", len(f.ledger.settled))
	}
	q := f.ledger.settled[0]
	if q.Identity.RequestID == "" {
		t.Fatal("sync settlements are keyed by request ID")
	}
	// The fake provider reports prompt=10, completion=len("echo: abc")=9.
	wantCost := 10*price.InputPerToken + 9*price.OutputPerToken
	if q.CostMicros == nil || *q.CostMicros != wantCost {
		t.Fatalf("cost: want %d, got %+v", wantCost, q.CostMicros)
	}
	if q.PriceVersion == nil || *q.PriceVersion != 5 || q.Currency != "USD" {
		t.Fatalf("price version: %+v", q)
	}
}
