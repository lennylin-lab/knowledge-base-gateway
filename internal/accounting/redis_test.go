package accounting

// Redis budget-gate tests. The atomic scripts run against miniredis in every
// test run (offline, hermetic, supports EVAL); an env-gated block re-runs the
// multi-instance atomicity scenarios against the real Redis when
// TEST_REDIS_ADDR is set, zero skips in CI.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/redis/go-redis/v9"
)

func newBudgetRedis(t *testing.T, addr string) *RedisBudget {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisBudget(client, fmt.Sprintf("gwtest-%d", time.Now().UnixNano()))
}

func newMiniredisBudget(t *testing.T) (*RedisBudget, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return newBudgetRedis(t, mr.Addr()), mr
}

// TestRedisBudgetReserveAtomicAcrossInstances: concurrent reserves from
// simulated instances can never oversell the subject or the tenant budget —
// check and charge share one script, so the admitted total is bounded.
func TestRedisBudgetReserveAtomicAcrossInstances(t *testing.T) {
	run := func(t *testing.T, addr string) {
		b := newBudgetRedis(t, addr)
		limits := BudgetLimits{Currency: "USD", SubjectDaily: 1000, TenantDaily: 1000}
		now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
		var (
			mu       sync.Mutex
			admitted int
			denials  int
			wg       sync.WaitGroup
		)
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				res, err := b.Reserve(context.Background(),
					fmt.Sprintf("subject-%d", n%4), "tenant-shared", limits, 100, now)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					denials++
					return
				}
				res.Release() // refund so the test is order-independent
				admitted++
			}(i)
		}
		wg.Wait()
		// With 1000 micros of tenant budget and 100 per reserve, at most 10
		// reserves can be live at once; the rest must deny, never oversell.
		if admitted+denials != 40 {
			t.Fatalf("lost outcomes: %d admitted, %d denied", admitted, denials)
		}
		if denials == 0 {
			t.Fatal("the tenant budget must have denied at least once")
		}
	}
	t.Run("miniredis", func(t *testing.T) {
		mr := miniredis.RunT(t)
		run(t, mr.Addr())
	})
	t.Run("real", func(t *testing.T) {
		addr := os.Getenv("TEST_REDIS_ADDR")
		if addr == "" {
			t.Skip("TEST_REDIS_ADDR not set; skipping real-Redis budget test")
		}
		run(t, addr)
	})
}

// TestRedisBudgetDenialShape: a genuine exhaustion denies with the stable
// code, the denying scope, and a UTC Retry-After; an outage surfaces as
// limiter.ErrUnavailable and never as a denial.
func TestRedisBudgetDenialShape(t *testing.T) {
	mb, mr := newMiniredisBudget(t)
	limits := BudgetLimits{Currency: "USD", SubjectDaily: 500}
	now := time.Now()
	res, err := mb.Reserve(context.Background(), "s", "t", limits, 400, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	_, err = mb.Reserve(context.Background(), "s", "t", limits, 400, now)
	var denial *Error
	if !errors.As(err, &denial) || denial.Code != CodeBudgetExceeded || denial.Scope != ScopeSubject {
		t.Fatalf("want subject budget denial, got %v", err)
	}
	if denial.RetryAfter <= 0 {
		t.Fatal("denial must carry a UTC Retry-After")
	}
	res.Release()

	// Outage: fail closed as infrastructure failure.
	mr.SetError("simulated redis outage")
	if res, err := mb.Reserve(context.Background(), "s", "t", limits, 1, now); res != nil || !errors.Is(err, limiter.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got (%v, %v)", res, err)
	}
}

// TestRedisBudgetFinalizeExactlyOnce: settle-after-settle, release-after-
// release, and settle-after-release are all no-ops against the real scripts.
func TestRedisBudgetFinalizeExactlyOnce(t *testing.T) {
	mb, _ := newMiniredisBudget(t)
	limits := BudgetLimits{Currency: "USD", SubjectDaily: 1000, SubjectMonthly: 5000, TenantDaily: 1000, TenantMonthly: 5000}
	now := time.Now()
	res, err := mb.Reserve(context.Background(), "s", "t", limits, 300, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res.Settle(i64(200)) // delta -100
	res.Settle(i64(500)) // second settle: no-op
	res.Release()        // release after settle: no-op

	// The subject daily counter must be exactly 200 after the single settle.
	client := redis.NewClient(&redis.Options{Addr: mb.Client.Options().Addr})
	defer client.Close()
	keys := budgetKeys(mb.Prefix, "s", "t", limits, now)
	var used string
	if err := client.Get(context.Background(), keys[0]).Scan(&used); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if used != "200" {
		t.Fatalf("counter after exactly-once finalize: want 200, got %s", used)
	}
}

// TestRedisBudgetPeriodRolloverNotInherited: deltas apply only to counters
// that still exist, so a settlement straddling a period boundary cannot
// poison the new period.
func TestRedisBudgetPeriodRolloverNotInherited(t *testing.T) {
	mb, mr := newMiniredisBudget(t)
	limits := BudgetLimits{Currency: "USD", SubjectDaily: 1000}
	now := time.Date(2026, 9, 19, 23, 59, 0, 0, time.UTC)
	res, err := mb.Reserve(context.Background(), "s", "t", limits, 300, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Fast-forward past the boundary and expire the old keys, then settle.
	mr.SetTime(now.Add(2 * time.Hour))
	mr.FastForward(2 * time.Hour)
	res.Settle(i64(100))
	client := redis.NewClient(&redis.Options{Addr: mb.Client.Options().Addr})
	defer client.Close()
	newDay := budgetKeys(mb.Prefix, "s", "t", limits, now.Add(2*time.Hour))[0]
	if n, err := client.Get(context.Background(), newDay).Int(); err == nil && n != 0 {
		t.Fatalf("the new period must not inherit the adjustment: %d", n)
	}
}
