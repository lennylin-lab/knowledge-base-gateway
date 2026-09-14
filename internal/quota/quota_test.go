package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
)

// gates returns one instance of every Gate implementation so the behavior
// suite below pins identical semantics for the in-memory development gate
// and the Redis multi-instance gate.
func gates(t *testing.T) map[string]Gate {
	t.Helper()
	out := map[string]Gate{"memory": NewMemory()}
	out["redis"] = newMiniredisGate(t)
	return out
}

// usedOn reads the charged amount of one period directly; used by tests to
// verify exactly-once settlement without going through admission. The key
// argument must come from DayKey/MonthKey with an empty prefix; gate
// implementations contribute their own namespace.
func usedOn(t *testing.T, g Gate, key string) int64 {
	t.Helper()
	switch impl := g.(type) {
	case *Memory:
		impl.mu.Lock()
		defer impl.mu.Unlock()
		return impl.used[key]
	case *RedisQuota:
		v, err := impl.Client.Get(context.Background(), impl.Prefix+key).Int64()
		if err != nil {
			return 0
		}
		return v
	default:
		t.Fatalf("unknown gate implementation %T", g)
		return 0
	}
}

func TestEstimate(t *testing.T) {
	mt := 500
	zero := 0
	cases := []struct {
		name            string
		maxTokens       *int
		policyMaxOutput int
		inputChars      int
		want            int64
	}{
		{"declared max_tokens wins", &mt, 9000, 80, 520},
		{"policy ceiling when omitted", nil, 9000, 80, 9020},
		{"conservative default", nil, 0, 80, 4096 + 20},
		{"zero declared behaves as omitted", &zero, 300, 0, 300},
		{"input rounding up", nil, 0, 1, 4096 + 1},
		{"empty input", nil, 0, 0, 4096},
	}
	for _, tc := range cases {
		if got := Estimate(tc.maxTokens, tc.policyMaxOutput, tc.inputChars); got != tc.want {
			t.Errorf("%s: Estimate = %d, want %d", tc.name, got, tc.want)
		}
	}
	// Deterministic: identical requests reserve identical amounts.
	a := Estimate(&mt, 0, 1234)
	b := Estimate(&mt, 0, 1234)
	if a != b {
		t.Errorf("estimate must be deterministic: %d vs %d", a, b)
	}
}

func TestLimitsConfigured(t *testing.T) {
	if (Limits{}).Configured() {
		t.Error("zero limits must be unlimited (not configured)")
	}
	if !(Limits{DailyTokens: 100}).Configured() {
		t.Error("daily limit must count as configured")
	}
	if !(Limits{MonthlyTokens: 100}).Configured() {
		t.Error("monthly limit must count as configured")
	}
}

func TestGateDeniesWhenDailyExhausted(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000}
		if _, err := g.Reserve(context.Background(), "s", limits, 600, now); err != nil {
			t.Fatalf("%s: first reserve: %v", name, err)
		}
		res, err := g.Reserve(context.Background(), "s", limits, 400, now)
		if err != nil {
			t.Fatalf("%s: second reserve filling the budget: %v", name, err)
		}
		denied, qErr := g.Reserve(context.Background(), "s", limits, 1, now)
		if denied != nil || qErr == nil {
			t.Fatalf("%s: exhausted daily budget must deny, got res=%v err=%v", name, denied, qErr)
		}
		var qe *Error
		if !errors.As(qErr, &qe) || qe.Code != CodeQuotaExceeded {
			t.Fatalf("%s: denial must be *Error with code quota_exceeded, got %v", name, qErr)
		}
		if qe.RetryAfter <= 0 || qe.RetryAfter > 24*time.Hour {
			t.Fatalf("%s: daily Retry-After must point at UTC midnight, got %v", name, qe.RetryAfter)
		}
		if errors.Is(qErr, limiter.ErrUnavailable) {
			t.Fatalf("%s: a denial must never be classified as limiter outage", name)
		}
		// Denied calls leave counters untouched.
		if got := usedOn(t, g, DayKey("", "s", now)); got != 1000 {
			t.Fatalf("%s: denied reserve changed the counter: %d", name, got)
		}
		res.Release()
	}
}

func TestGateDeniesWhenMonthlyExhausted(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{MonthlyTokens: 500}
		if _, err := g.Reserve(context.Background(), "s", limits, 500, now); err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		_, qErr := g.Reserve(context.Background(), "s", limits, 1, now)
		var qe *Error
		if !errors.As(qErr, &qe) || qe.Code != CodeQuotaExceeded {
			t.Fatalf("%s: exhausted monthly budget must deny, got %v", name, qErr)
		}
		if qe.RetryAfter <= 0 || qe.RetryAfter > 31*24*time.Hour {
			t.Fatalf("%s: monthly Retry-After must point at the next UTC month, got %v", name, qe.RetryAfter)
		}
	}
}

func TestGateDailyAndMonthlyIndependent(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		// Daily exhausted, monthly still open: monthly-only admission works.
		limits := Limits{DailyTokens: 100}
		if _, err := g.Reserve(context.Background(), "s", limits, 100, now); err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		if _, qErr := g.Reserve(context.Background(), "s", limits, 1, now); qErr == nil {
			t.Fatalf("%s: daily must deny independently", name)
		}
		monthlyOnly := Limits{MonthlyTokens: 1000}
		res, err := g.Reserve(context.Background(), "s", monthlyOnly, 400, now)
		if err != nil {
			t.Fatalf("%s: monthly-only admission must pass: %v", name, err)
		}
		// Both counters reflect only their own charges.
		if got := usedOn(t, g, DayKey("", "s", now)); got != 100 {
			t.Fatalf("%s: daily counter = %d, want 100", name, got)
		}
		if got := usedOn(t, g, MonthKey("", "s", now)); got != 400 {
			t.Fatalf("%s: monthly counter = %d, want 400", name, got)
		}
		res.Release()
	}
}

func TestGateUnlimitedWhenZero(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		res, err := g.Reserve(context.Background(), "s", Limits{}, 1<<30, now)
		if err != nil {
			t.Fatalf("%s: unlimited reserve: %v", name, err)
		}
		res.Settle(nil)
		res.Release()
		if got := usedOn(t, g, DayKey("", "s", now)); got != 0 {
			t.Fatalf("%s: unlimited period must not be charged, got %d", name, got)
		}
	}
}

func TestGateSettleAdjustsExactlyOnce(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 10_000}
		res, err := g.Reserve(context.Background(), "s", limits, 800, now)
		if err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		total := int64(300)
		res.Settle(&total)
		res.Settle(&total) // second settle must be a no-op
		if got := usedOn(t, g, DayKey("", "s", now)); got != 300 {
			t.Fatalf("%s: settled usage = %d, want 300 (reported total exactly once)", name, got)
		}
	}
}

func TestGateSettleUpwardAdjustment(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000}
		res, _ := g.Reserve(context.Background(), "s", limits, 100, now)
		total := int64(950)
		res.Settle(&total)
		// Usage above the estimate must count against the budget.
		if _, qErr := g.Reserve(context.Background(), "s", limits, 100, now); qErr == nil {
			t.Fatalf("%s: settled 950 of 1000 must leave only 50 tokens", name)
		}
	}
}

func TestGateUnknownUsageRetainsEstimate(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000}
		res, _ := g.Reserve(context.Background(), "s", limits, 700, now)
		res.Settle(nil) // usage unknown: conservative reservation stays
		if got := usedOn(t, g, DayKey("", "s", now)); got != 700 {
			t.Fatalf("%s: unknown usage must retain the estimate, got %d", name, got)
		}
		// Budget is genuinely constrained while retained.
		if _, qErr := g.Reserve(context.Background(), "s", limits, 301, now); qErr == nil {
			t.Fatal("retained reservation must still constrain admission")
		}
		res.Release()
	}
}

func TestGateReleaseIdempotent(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000, MonthlyTokens: 1000}
		res, _ := g.Reserve(context.Background(), "s", limits, 400, now)
		res.Release()
		res.Release()
		res.Release()
		if got := usedOn(t, g, DayKey("", "s", now)); got != 0 {
			t.Fatalf("%s: daily after release = %d, want 0 (idempotent)", name, got)
		}
		if got := usedOn(t, g, MonthKey("", "s", now)); got != 0 {
			t.Fatalf("%s: monthly after release = %d, want 0 (idempotent)", name, got)
		}
		// Freed budget is admission-ready again.
		if _, err := g.Reserve(context.Background(), "s", limits, 1000, now); err != nil {
			t.Fatalf("%s: full budget must be available after release: %v", name, err)
		}
	}
}

func TestGateSettleAfterReleaseIsNoop(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000}
		res, _ := g.Reserve(context.Background(), "s", limits, 400, now)
		res.Release()
		total := int64(900)
		res.Settle(&total) // finalized by release; must not re-charge
		if got := usedOn(t, g, DayKey("", "s", now)); got != 0 {
			t.Fatalf("%s: settle after release changed the counter: %d", name, got)
		}
	}
}

func TestGatePeriodRollover(t *testing.T) {
	for name, g := range gates(t) {
		late := time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC)
		limits := Limits{DailyTokens: 500, MonthlyTokens: 500}
		if _, err := g.Reserve(context.Background(), "s", limits, 500, late); err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		if _, qErr := g.Reserve(context.Background(), "s", limits, 1, late); qErr == nil {
			t.Fatal("exhausted budget must deny before the boundary")
		}
		// Next UTC calendar day: both periods reset without manual cleanup.
		next := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
		if _, err := g.Reserve(context.Background(), "s", limits, 500, next); err != nil {
			t.Fatalf("%s: budget must reset at the UTC boundary: %v", name, err)
		}
		// Old period keys are untouched by new charges.
		if got := usedOn(t, g, DayKey("", "s", late)); got != 500 {
			t.Fatalf("%s: previous day key = %d, want 500", name, got)
		}
	}
}

func TestGateMonthlyRolloverIndependentOfDay(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{MonthlyTokens: 500}
		if _, err := g.Reserve(context.Background(), "s", limits, 500, now); err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		// Next day, same month: monthly budget stays exhausted...
		if _, qErr := g.Reserve(context.Background(), "s", limits, 1,
			time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)); qErr == nil {
			t.Fatalf("%s: monthly budget must persist across days", name)
		}
		// ...until the UTC month rolls over.
		if _, err := g.Reserve(context.Background(), "s", limits, 1,
			time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("%s: monthly budget must reset at the UTC month boundary: %v", name, err)
		}
	}
}

func TestGateSettleAcrossBoundaryTargetsChargedPeriod(t *testing.T) {
	for name, g := range gates(t) {
		late := time.Date(2026, 9, 15, 23, 59, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 10_000}
		res, err := g.Reserve(context.Background(), "s", limits, 800, late)
		if err != nil {
			t.Fatalf("%s: reserve: %v", name, err)
		}
		after := time.Date(2026, 9, 16, 0, 0, 30, 0, time.UTC)
		_ = after
		total := int64(200)
		res.Settle(&total)
		if got := usedOn(t, g, DayKey("", "s", late)); got != 200 {
			t.Fatalf("%s: straddling settle must adjust the charged day, got %d", name, got)
		}
		if got := usedOn(t, g, DayKey("", "s", after)); got != 0 {
			t.Fatalf("%s: the new day must not inherit the settlement, got %d", name, got)
		}
	}
}

func TestGateConcurrentReserveNeverOversubscribes(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 1000}
		const workers, each = 16, 10
		var admitted sync.Map
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < each; i++ {
					res, err := g.Reserve(context.Background(), "s", limits, 100, now)
					if err == nil {
						admitted.Store(fmt.Sprintf("%d-%d", w, i), res)
					}
				}
			}(w)
		}
		wg.Wait()
		count := 0
		admitted.Range(func(_, _ any) bool {
			count++
			return true
		})
		if count > 10 {
			t.Fatalf("%s: %d reservations admitted against a 1000-token budget of 100 each", name, count)
		}
		if got := usedOn(t, g, DayKey("", "s", now)); got != int64(count)*100 {
			t.Fatalf("%s: counter = %d, want exactly %d", name, got, count*100)
		}
		admitted.Range(func(_, v any) bool {
			v.(Reservation).Release()
			return true
		})
		if got := usedOn(t, g, DayKey("", "s", now)); got != 0 {
			t.Fatalf("%s: counter after releases = %d, want 0", name, got)
		}
	}
}

func TestGateSubjectsAreIsolated(t *testing.T) {
	for name, g := range gates(t) {
		now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
		limits := Limits{DailyTokens: 500}
		if _, err := g.Reserve(context.Background(), "a", limits, 500, now); err != nil {
			t.Fatalf("%s: reserve a: %v", name, err)
		}
		if _, err := g.Reserve(context.Background(), "b", limits, 500, now); err != nil {
			t.Fatalf("%s: subject b must have its own budget: %v", name, err)
		}
	}
}

func TestPeriodKeysAndRetryWindows(t *testing.T) {
	now := time.Date(2026, 9, 15, 10, 30, 0, 0, time.UTC)
	if got, want := DayKey("gw", "s", now), "gw:quota:d:s:20260915"; got != want {
		t.Errorf("DayKey = %q, want %q", got, want)
	}
	if got, want := MonthKey("gw", "s", now), "gw:quota:m:s:202609"; got != want {
		t.Errorf("MonthKey = %q, want %q", got, want)
	}
	if d := retryAfterDay(now); d != 13*time.Hour+30*time.Minute {
		t.Errorf("retryAfterDay = %v", d)
	}
	if m := retryAfterMonth(now); m != 15*24*time.Hour+13*time.Hour+30*time.Minute {
		t.Errorf("retryAfterMonth = %v", m)
	}
	// TTLs outlive their period by the grace window.
	if ttl := dayTTL(now); ttl != 13*time.Hour+30*time.Minute+keyGrace {
		t.Errorf("dayTTL = %v", ttl)
	}
	// Non-UTC input must normalize to UTC periods.
	berlin := time.Date(2026, 9, 15, 23, 30, 0, 0, mustLoc(t, "Europe/Berlin"))
	if got := DayKey("", "s", berlin); got != ":quota:d:s:20260915" {
		t.Errorf("non-UTC time must normalize to the UTC day, got %q", got)
	}
	// 23:30 Berlin is 21:30 UTC: two and a half hours to UTC midnight.
	if d := retryAfterDay(berlin); d != 2*time.Hour+30*time.Minute {
		t.Errorf("retryAfterDay(berlin) = %v, want 2h30m to UTC midnight", d)
	}
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}
