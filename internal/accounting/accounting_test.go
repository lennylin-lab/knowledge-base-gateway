package accounting

// Unit tests for the cost-governance domain: price arithmetic (integer
// micros, checked overflow, charged-only-when-reported token classes), the
// min-of-declared budget fold, and the Gate orchestration against a stub
// store — reserve pricing gates, exactly-once settle/release, unknown-cost
// preservation, and the enforcement-off ledger-capture rollback posture.

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
)

func i64(v int64) *int64 { return &v }

func testUsage(prompt, completion int64) Usage {
	return Usage{PromptTokens: i64(prompt), CompletionTokens: i64(completion)}
}

// TestPriceCostArithmeticCases pins the charged-class rules: (prompt -
// cached) at the input price, cached only when reported, reasoning only when
// reported, completion at the output price; an unpriced reported class keeps
// the cost unknown, and every known cost is recomputable exactly from tokens
// and price version.
func TestPriceCostArithmeticCases(t *testing.T) {
	price := Price{
		Version: 7, Currency: "USD",
		InputPerToken: 15, OutputPerToken: 60,
		ReasoningPerToken:   i64(30),
		CachedInputPerToken: i64(7),
	}
	freeReasoning := Price{Version: 1, Currency: "USD", InputPerToken: 10, OutputPerToken: 0, ReasoningPerToken: i64(0)}
	// Same base prices but with the optional classes unpriced (NULL columns).
	noDetails := Price{Version: 8, Currency: "USD", InputPerToken: 15, OutputPerToken: 60}

	t.Run("plain", func(t *testing.T) {
		got, ok := price.Cost(testUsage(100, 50))
		if !ok || got != 100*15+50*60 {
			t.Fatalf("got %d ok=%v, want %d", got, ok, 100*15+50*60)
		}
	})
	t.Run("empty usage", func(t *testing.T) {
		got, ok := price.Cost(Usage{})
		if !ok || got != 0 {
			t.Fatalf("got %d ok=%v, want 0 true", got, ok)
		}
	})
	t.Run("cached discounted", func(t *testing.T) {
		u := Usage{PromptTokens: i64(100), CompletionTokens: i64(10), CachedInputTokens: i64(40)}
		got, ok := price.Cost(u)
		if !ok || got != (100-40)*15+40*7+10*60 {
			t.Fatalf("got %d ok=%v", got, ok)
		}
	})
	t.Run("reasoning charged when reported", func(t *testing.T) {
		u := Usage{PromptTokens: i64(10), CompletionTokens: i64(10), ReasoningTokens: i64(5)}
		got, ok := price.Cost(u)
		if !ok || got != 10*15+10*60+5*30 {
			t.Fatalf("got %d ok=%v", got, ok)
		}
	})
	t.Run("reasoning reported without price stays unknown", func(t *testing.T) {
		u := Usage{PromptTokens: i64(10), ReasoningTokens: i64(5)}
		if _, ok := noDetails.Cost(u); ok {
			t.Fatal("unpriced reported reasoning class must keep the cost unknown")
		}
	})
	t.Run("cached reported without price stays unknown", func(t *testing.T) {
		u := Usage{PromptTokens: i64(10), CachedInputTokens: i64(1)}
		if _, ok := noDetails.Cost(u); ok {
			t.Fatal("unpriced reported cached class must keep the cost unknown")
		}
	})
	t.Run("cached exceeds prompt is inconsistent", func(t *testing.T) {
		u := Usage{PromptTokens: i64(10), CachedInputTokens: i64(11)}
		if _, ok := price.Cost(u); ok {
			t.Fatal("cached > prompt must keep the cost unknown")
		}
	})
	t.Run("zero price is an explicitly free class", func(t *testing.T) {
		u := Usage{PromptTokens: i64(4), ReasoningTokens: i64(9)}
		got, ok := freeReasoning.Cost(u)
		if !ok || got != 40 {
			t.Fatalf("got %d ok=%v, want 40 true (zero price is known-free, not unknown)", got, ok)
		}
	})
	t.Run("overflow keeps the cost unknown", func(t *testing.T) {
		huge := Price{Version: 1, Currency: "USD", InputPerToken: math.MaxInt64}
		if _, ok := huge.Cost(testUsage(2, 0)); ok {
			t.Fatal("overflowing multiplication must not silently produce a cost")
		}
	})
}

// TestFoldBudgetRows pins the min-of-declared fold and the one-currency rule.
func TestFoldBudgetRows(t *testing.T) {
	limits, err := FoldBudgetRows([]BudgetRow{
		{Scope: ScopeSubject, Period: "daily", Currency: "USD", AmountMicros: 5_000_000},
		{Scope: ScopeSubject, Period: "monthly", Currency: "USD", AmountMicros: 90_000_000},
		{Scope: ScopeTenant, Period: "daily", Currency: "USD", AmountMicros: 40_000_000},
	})
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if limits.Currency != "USD" || limits.SubjectDaily != 5_000_000 ||
		limits.SubjectMonthly != 90_000_000 || limits.TenantDaily != 40_000_000 || limits.TenantMonthly != 0 {
		t.Fatalf("unexpected fold: %+v", limits)
	}
	if !limits.Configured() {
		t.Fatal("folded limits must report configured")
	}

	// Min-of-declared when a defensive fold ever sees duplicates.
	limits, err = FoldBudgetRows([]BudgetRow{
		{Scope: ScopeSubject, Period: "daily", Currency: "USD", AmountMicros: 5_000_000},
		{Scope: ScopeSubject, Period: "daily", Currency: "USD", AmountMicros: 3_000_000},
	})
	if err != nil || limits.SubjectDaily != 3_000_000 {
		t.Fatalf("min-of-declared fold: %v %+v", err, limits)
	}

	// Mixed currencies are a configuration error, never a silent pick.
	if _, err := FoldBudgetRows([]BudgetRow{
		{Scope: ScopeSubject, Period: "daily", Currency: "USD", AmountMicros: 1},
		{Scope: ScopeTenant, Period: "daily", Currency: "EUR", AmountMicros: 1},
	}); !errors.Is(err, ErrBudgetConfig) {
		t.Fatalf("mixed currencies: want ErrBudgetConfig, got %v", err)
	}

	if _, err := FoldBudgetRows(nil); err != nil {
		t.Fatalf("no rows: %v", err)
	}
}

// stubStore is the in-memory Store double. Prices are keyed
// provider/model; failures are injectable per operation.
type stubStore struct {
	mu         sync.Mutex
	price      Price
	hasPrice   bool
	budgets    BudgetLimits
	reserved   []LedgerReservation
	settled    []SettleQuery
	released   []Identity
	reserveErr error
	settleErr  error
}

func (s *stubStore) EffectivePrice(context.Context, string, string, time.Time) (Price, bool, error) {
	if s.hasPrice {
		return s.price, true, nil
	}
	return Price{}, false, nil
}

func (s *stubStore) BudgetLimits(context.Context, string, string) (BudgetLimits, error) {
	return s.budgets, nil
}

func (s *stubStore) ReserveLedger(_ context.Context, row LedgerReservation) error {
	if s.reserveErr != nil {
		return s.reserveErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved = append(s.reserved, row)
	return nil
}

func (s *stubStore) SettleLedger(_ context.Context, q SettleQuery) (SettleOutcome, error) {
	if s.settleErr != nil {
		return SettleOutcome{}, s.settleErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled = append(s.settled, q)
	return SettleOutcome{Settled: true}, nil
}

func (s *stubStore) ReleaseLedger(_ context.Context, id Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, id)
	return nil
}

func newTestGate(store *stubStore, enforcement bool) *Gate {
	return &Gate{Store: store, Budgets: NewMemoryBudget(), Enforcement: enforcement, Now: func() time.Time { return time.Now() }}
}

func reserveInput(identity Identity) ReserveInput {
	in := i64(1000)
	out := i64(500)
	return ReserveInput{
		Identity: identity, SubjectID: "s1", TenantID: "t1",
		Protocol: "chat", PublicModel: "m1", Provider: "p1",
		Estimate: Usage{PromptTokens: in, CompletionTokens: out},
		Now:      time.Now(),
	}
}

// TestGateWithoutStoreIsNoop: accounting off keeps the pre-V1.4 behavior.
func TestGateWithoutStoreIsNoop(t *testing.T) {
	var g *Gate
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res != Done {
		t.Fatal("nil gate must return Done")
	}
	if err := res.Settle("p1", testUsage(1, 1)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := res.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// TestLedgerCaptureContinuesWithoutEnforcement pins the rollback posture:
// enforcement off never writes budget counters but still records the ledger
// lifecycle, and no price is required.
func TestLedgerCaptureContinuesWithoutEnforcement(t *testing.T) {
	store := &stubStore{budgets: BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000},
		hasPrice: true, price: testPrice}
	g := newTestGate(store, false) // enforcement off
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if len(store.reserved) != 1 {
		t.Fatalf("ledger capture must continue: %d rows", len(store.reserved))
	}
	if err := res.Settle("p1", testUsage(10, 5)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if len(store.settled) != 1 {
		t.Fatalf("settlement must land: %d", len(store.settled))
	}
	q := store.settled[0]
	if q.CostMicros == nil {
		t.Fatal("cost must be computed when a price exists")
	}
	if q.CostMicros != nil && *q.CostMicros != 10*testPrice.InputPerToken+5*testPrice.OutputPerToken {
		t.Fatalf("wrong cost: %d", *q.CostMicros)
	}
	if q.PriceVersion == nil || *q.PriceVersion != testPrice.Version || q.Currency != testPrice.Currency {
		t.Fatalf("settlement must snapshot the used price version: %+v", q)
	}
}

var testPrice = Price{Version: 3, Currency: "USD", InputPerToken: 20, OutputPerToken: 80, ReasoningPerToken: i64(40), CachedInputPerToken: i64(10)}

// TestEnforcementRequiresPrice: a configured budget with no applicable price
// (or a price in another currency) fails before provider work as
// pricing_unavailable, with no ledger row and no charged counters.
func TestEnforcementRequiresPrice(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *stubStore
	}{
		{"no price", &stubStore{budgets: BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000}}},
		{"currency mismatch", &stubStore{budgets: BudgetLimits{Currency: "EUR", SubjectDaily: 1_000_000},
			hasPrice: true, price: testPrice}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGate(tc.store, true)
			_, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
			if !errors.Is(err, ErrPricingUnavailable) {
				t.Fatalf("want ErrPricingUnavailable, got %v", err)
			}
			if len(tc.store.reserved) != 0 {
				t.Fatal("a refused request must not write a ledger row")
			}
		})
	}
}

// TestBudgetDenialLeavesNoResidue: the denial is a *Error with the UTC
// Retry-After, both dimensions are enforced, and nothing is charged.
func TestBudgetDenialLeavesNoResidue(t *testing.T) {
	store := &stubStore{budgets: BudgetLimits{Currency: "USD", SubjectDaily: 50_000}, hasPrice: true, price: testPrice}
	g := newTestGate(store, true)
	// estimate = 1000*20 + 500*80 = 60_000 micros; a subject daily budget of
	// 50_000 denies before the tenant counters are touched.
	_, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	denial, ok := err.(*Error)
	if !ok || denial.Code != CodeBudgetExceeded {
		t.Fatalf("want budget denial, got %v", err)
	}
	if denial.Scope != ScopeSubject {
		t.Fatalf("denial scope: %s", denial.Scope)
	}
	if denial.RetryAfter <= 0 || denial.RetryAfter > 24*time.Hour {
		t.Fatalf("retry-after must point at the UTC day boundary: %v", denial.RetryAfter)
	}
	if len(store.reserved) != 0 {
		t.Fatal("a denied request must not write a ledger row")
	}
}

// TestTenantDimensionEnforced: a subject pass with a full tenant budget
// denies on the tenant scope.
func TestTenantDimensionEnforced(t *testing.T) {
	store := &stubStore{budgets: BudgetLimits{Currency: "USD", TenantMonthly: 59_999}, hasPrice: true, price: testPrice}
	g := newTestGate(store, true)
	_, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	denial, ok := err.(*Error)
	if !ok || denial.Scope != ScopeTenant {
		t.Fatalf("want tenant-scope denial, got %v", err)
	}
	if denial.RetryAfter <= 0 || denial.RetryAfter > 31*24*time.Hour {
		t.Fatalf("retry-after must point at the UTC month boundary: %v", denial.RetryAfter)
	}
}

// TestSettleUnknownUsageKeepsUnknownCost: usage reported as unknown settles
// with NULL cost and no price version, and the conservative reservation is
// retained by the counters.
func TestSettleUnknownUsageKeepsUnknownCost(t *testing.T) {
	store := &stubStore{budgets: BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000}, hasPrice: true, price: testPrice}
	g := newTestGate(store, true)
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := res.Settle("p1", Usage{}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if len(store.settled) != 1 {
		t.Fatal("settlement must land")
	}
	q := store.settled[0]
	if q.CostMicros != nil || q.PriceVersion != nil || q.Currency != "" {
		t.Fatalf("unknown usage must stay unknown: %+v", q)
	}
	if q.Usage.PromptTokens != nil || q.Usage.CompletionTokens != nil {
		t.Fatal("no tokens may be fabricated")
	}
}

// TestSettleKnownUsageWithoutPriceKeepsCostUnknown: tokens are recorded, the
// cost stays NULL, and no price version is named (none was used).
func TestSettleKnownUsageWithoutPriceKeepsCostUnknown(t *testing.T) {
	store := &stubStore{hasPrice: false} // no budget configured, no price
	g := newTestGate(store, true)
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := res.Settle("p1", testUsage(10, 5)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	q := store.settled[0]
	if q.CostMicros != nil {
		t.Fatalf("missing price must keep the cost NULL, got %d", *q.CostMicros)
	}
	if q.Usage.PromptTokens == nil || *q.Usage.PromptTokens != 10 {
		t.Fatalf("usage tokens must still be recorded: %+v", q.Usage)
	}
}

// TestSettleExactlyOnceAndSettleAfterRelease: repeated finalization is a
// no-op and settle-after-release never resurrects a released reservation.
func TestSettleExactlyOnceAndSettleAfterRelease(t *testing.T) {
	store := &stubStore{hasPrice: true, price: testPrice,
		budgets: BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000}}
	g := newTestGate(store, true)
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := res.Settle("p1", testUsage(10, 5)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := res.Settle("p1", testUsage(99, 99)); err != nil {
		t.Fatalf("repeat settle: %v", err)
	}
	if err := res.Release(); err != nil {
		t.Fatalf("release after settle: %v", err)
	}
	if len(store.settled) != 1 || len(store.released) != 0 {
		t.Fatalf("exactly-once settle: %d settled, %d released", len(store.settled), len(store.released))
	}

	// Release first: settle afterwards must be a no-op.
	store2 := &stubStore{hasPrice: true, price: testPrice}
	g2 := newTestGate(store2, true)
	res2, err := g2.Reserve(context.Background(), reserveInput(Identity{RequestID: "r2"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := res2.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := res2.Settle("p1", testUsage(10, 5)); err != nil {
		t.Fatalf("settle after release: %v", err)
	}
	if len(store2.settled) != 0 || len(store2.released) != 1 {
		t.Fatalf("settle-after-release must be a no-op: %d settled, %d released",
			len(store2.settled), len(store2.released))
	}
}

// TestSettlementFailureIsRetryable: a failed settlement returns an error
// (unavailable-class), keeps the ledger row reserved, and a retry succeeds.
func TestSettlementFailureIsRetryable(t *testing.T) {
	store := &stubStore{hasPrice: true, price: testPrice, settleErr: errors.New("db down")}
	g := newTestGate(store, true)
	res, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := res.Settle("p1", testUsage(10, 5)); err == nil {
		t.Fatal("a failed settlement must be reported, never silently successful")
	} else if !errors.Is(err, limiter.ErrUnavailable) {
		t.Fatalf("settlement failures are infrastructure-class: %v", err)
	}
	store.mu.Lock()
	store.settleErr = nil
	store.mu.Unlock()
	if err := res.Settle("p1", testUsage(10, 5)); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if len(store.settled) != 1 {
		t.Fatalf("retry must land exactly one settlement: %d", len(store.settled))
	}
}

// TestReserveLedgerFailureFailsClosed: a ledger write failure refunds the
// charged counters and surfaces as unavailable — never as a denial.
func TestReserveLedgerFailureFailsClosed(t *testing.T) {
	store := &stubStore{hasPrice: true, price: testPrice,
		budgets:    BudgetLimits{Currency: "USD", SubjectDaily: 1_000_000},
		reserveErr: errors.New("db down")}
	g := newTestGate(store, true)
	_, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"}))
	if !errors.Is(err, limiter.ErrUnavailable) {
		t.Fatalf("want unavailable-class, got %v", err)
	}
	if strings.Contains(err.Error(), "budget_exceeded") {
		t.Fatal("an outage must never look like a denial")
	}
	// The counters were refunded: the same reservation now succeeds.
	store.mu.Lock()
	store.reserveErr = nil
	store.mu.Unlock()
	if _, err := g.Reserve(context.Background(), reserveInput(Identity{RequestID: "r1"})); err != nil {
		t.Fatalf("retry after refund: %v", err)
	}
}

// TestMemoryBudgetExactlyOnceAndBothDimensions exercises the in-memory
// counter engine directly, including the UTC boundary keys.
func TestMemoryBudgetExactlyOnceAndBothDimensions(t *testing.T) {
	mb := NewMemoryBudget()
	limits := BudgetLimits{Currency: "USD", SubjectDaily: 100, TenantDaily: 150}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	r1, err := mb.Reserve(context.Background(), "s1", "t1", limits, 60, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := mb.Reserve(context.Background(), "s1", "t1", limits, 60, now); err == nil {
		t.Fatal("subject daily budget must deny the second reserve")
	} else if _, ok := err.(*Error); !ok {
		t.Fatalf("denial type: %v", err)
	}
	// A different subject shares the tenant budget: 60+60 <= 150 passes the
	// tenant counter but this subject is fresh — passes, then tenant denies.
	r2, err := mb.Reserve(context.Background(), "s2", "t1", limits, 60, now)
	if err != nil {
		t.Fatalf("second subject within tenant budget: %v", err)
	}
	if _, err := mb.Reserve(context.Background(), "s3", "t1", limits, 60, now); err == nil {
		t.Fatal("tenant daily budget must deny once exhausted across subjects")
	}
	// Settle r1 up to the exact cost (90): delta +30 applied once.
	r1.Settle(i64(90))
	// Release r2; repeated release is a no-op.
	r2.Release()
	r2.Release()
	// After r2's release the tenant counter is 60 again, so 60 fits.
	if _, err := mb.Reserve(context.Background(), "s3", "t1", limits, 60, now); err != nil {
		t.Fatalf("release must free the tenant budget: %v", err)
	}
	// The counters reset at the UTC day boundary.
	nextDay := now.Add(24 * time.Hour)
	if _, err := mb.Reserve(context.Background(), "s1", "t1", limits, 100, nextDay); err != nil {
		t.Fatalf("period rollover must reset the counters: %v", err)
	}
	// Uncapped periods are never charged.
	if _, err := mb.Reserve(context.Background(), "s1", "t1", BudgetLimits{Currency: "USD", SubjectMonthly: 1000}, 60, now); err != nil {
		t.Fatalf("uncapped periods must not constrain: %v", err)
	}
}
