package quota

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/redis/go-redis/v9"
)

// newMiniredisGate builds an in-process Redis double so the atomic scripts
// are exercised in every test run, without a live Redis.
func newMiniredisGate(t *testing.T) *RedisQuota {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedis(client, "gwtest")
}

// TestRedisReserveFailsClosedOnRedisError: a broken Redis must surface as
// ErrUnavailable (the shared 503 contract), never as a quota denial.
func TestRedisReserveFailsClosedOnRedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	q := NewRedis(client, "gwtest")
	mr.SetError("simulated redis outage")

	res, err := q.Reserve(context.Background(), "s", Limits{DailyTokens: 100}, 10, time.Now())
	if res != nil {
		t.Fatal("infrastructure failure must not admit")
	}
	if !errors.Is(err, limiter.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	var qe *Error
	if errors.As(err, &qe) {
		t.Fatal("infrastructure failure must not look like a quota denial")
	}
}

// TestRedisReserveHonorsContextCancellation: a canceled request fails closed
// as infrastructure failure before anything is charged.
func TestRedisReserveHonorsContextCancellation(t *testing.T) {
	g := newMiniredisGate(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := g.Reserve(ctx, "s", Limits{DailyTokens: 100}, 10, time.Now())
	if res != nil {
		t.Fatal("canceled reserve must not admit")
	}
	if !errors.Is(err, limiter.ErrUnavailable) {
		t.Fatalf("canceled Redis call must surface as ErrUnavailable, got %v", err)
	}
}

// TestRedisPeriodKeysExpire pins that counters carry a TTL past the period
// boundary: budgets expire without any manual cleanup job.
func TestRedisPeriodKeysExpire(t *testing.T) {
	g := newMiniredisGate(t)
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	res, err := g.Reserve(context.Background(), "s", Limits{DailyTokens: 100, MonthlyTokens: 100}, 10, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	for _, key := range []string{DayKey("gwtest", "s", now), MonthKey("gwtest", "s", now)} {
		ttl, err := g.Client.PTTL(context.Background(), key).Result()
		if err != nil {
			t.Fatalf("pttl %s: %v", key, err)
		}
		if ttl <= 0 {
			t.Fatalf("counter %s must expire at the period boundary, ttl=%v", key, ttl)
		}
		if ttl > 31*24*time.Hour+keyGrace+time.Minute {
			t.Fatalf("counter %s ttl %v unreasonably long", key, ttl)
		}
	}
	res.Release()
	// Release must not leave counter keys resurrected or marker leaks blocking.
	if v, _ := g.Client.Get(context.Background(), DayKey("gwtest", "s", now)).Int64(); v != 0 {
		t.Fatalf("released day counter = %d, want 0", v)
	}
}

// TestRedisFinalizeMarkerExpires ensures finalize markers never leak forever.
func TestRedisFinalizeMarkerExpires(t *testing.T) {
	g := newMiniredisGate(t)
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	res, err := g.Reserve(context.Background(), "s", Limits{DailyTokens: 100}, 10, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res.Release()
	keys, err := g.Client.Keys(context.Background(), "gwtest:quota:fin:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("want exactly one finalize marker, got %v", keys)
	}
	ttl, err := g.Client.PTTL(context.Background(), keys[0]).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 {
		t.Fatalf("finalize marker must expire, ttl=%v", ttl)
	}
}

// TestRedisUnlimitedPeriodNeverWritesKeys: zero limits charge nothing.
func TestRedisUnlimitedPeriodNeverWritesKeys(t *testing.T) {
	g := newMiniredisGate(t)
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	res, err := g.Reserve(context.Background(), "s", Limits{DailyTokens: 0, MonthlyTokens: 100}, 10, now)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res.Settle(nil)
	res.Release()
	if n, _ := g.Client.Exists(context.Background(), DayKey("gwtest", "s", now)).Result(); n != 0 {
		t.Fatal("unlimited daily period must never be written")
	}
}

// TestRedisAllowSharedClientSmoke is a self-check that miniredis executes the
// scripts (guards against silent skip when Lua support changes).
func TestRedisScriptsExecuteOnMiniredis(t *testing.T) {
	g := newMiniredisGate(t)
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	if _, err := g.Reserve(context.Background(), "s", Limits{DailyTokens: 100}, 10, now); err != nil {
		t.Fatalf("miniredis must execute the quota scripts: %v", err)
	}
}

// TestRedisQuotaReal exercises the atomic scripts against a real Redis.
// Skipped unless TEST_REDIS_ADDR is set (e.g. 127.0.0.1:6379).
func TestRedisQuotaReal(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping real-Redis quota test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	g := NewRedis(client, "gwquotatest")
	now := time.Now()
	limits := Limits{DailyTokens: 1000, MonthlyTokens: 5000}
	// Unique subject per run: real Redis keeps state across test runs.
	subject := "s-" + strconv.FormatInt(now.UnixNano(), 36)

	res, err := g.Reserve(context.Background(), subject, limits, 600, now)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	res2, err := g.Reserve(context.Background(), subject, limits, 400, now)
	if err != nil {
		t.Fatalf("second reserve filling the budget: %v", err)
	}
	_, denied := g.Reserve(context.Background(), subject, limits, 1, now)
	var qe *Error
	if !errors.As(denied, &qe) || qe.Code != CodeQuotaExceeded || qe.RetryAfter <= 0 {
		t.Fatalf("exhausted budget must deny with Retry-After, got %v", denied)
	}

	total := int64(400)
	res.Settle(&total)
	res.Settle(&total) // exactly once
	res2.Release()
	res2.Release() // idempotent

	if got, err := client.Get(context.Background(), DayKey("gwquotatest", subject, now)).Int64(); err != nil || got != 400 {
		t.Fatalf("settled day counter = %d err=%v, want 400", got, err)
	}
	if got, err := client.Get(context.Background(), MonthKey("gwquotatest", subject, now)).Int64(); err != nil || got != 400 {
		t.Fatalf("settled month counter = %d err=%v, want 400 (release refunds both periods)", got, err)
	}

	// Admission reflects the settled state: only 600 tokens remain.
	if _, err := g.Reserve(context.Background(), subject, limits, 601, now); err == nil {
		t.Fatal("reserve past settled usage must deny")
	}
	if r, err := g.Reserve(context.Background(), subject, limits, 600, now); err != nil {
		t.Fatalf("reserve within settled budget must pass: %v", err)
	} else {
		r.Release()
	}
}
