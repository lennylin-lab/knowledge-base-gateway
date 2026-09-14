package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alicebob/miniredis/v2"
)

// newTestRedis starts an in-process Redis double supporting the EVAL-based
// scripts, so admission/fault behavior is tested without a live Redis.
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// TestRedisAllowPrunesStaleLeases pins the atomic stale-lease cleanup: a
// concurrency lease older than the lease TTL must not block admission, and
// must be gone from the sorted set afterwards.
func TestRedisAllowPrunesStaleLeases(t *testing.T) {
	_, client := newTestRedis(t)
	rl := NewRedis(client, "gwtest", 100, 1)

	now := time.Now()
	staleScore := float64(now.Add(-2 * DefaultLeaseTTL).UnixMilli())
	if err := client.ZAdd(context.Background(), "gwtest:conc:s",
		redis.Z{Score: staleScore, Member: "abandoned"}).Err(); err != nil {
		t.Fatalf("seed stale lease: %v", err)
	}

	ok, _, release, err := rl.Allow(context.Background(), "s", now)
	if err != nil || !ok {
		t.Fatalf("stale lease must be pruned and admit, ok=%v err=%v", ok, err)
	}
	release()

	card, err := client.ZCard(context.Background(), "gwtest:conc:s").Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if card != 0 {
		t.Fatalf("pruned set must hold only fresh leases, got %d members", card)
	}
}

// TestRedisAllowFreshLeaseStillCounts ensures the pruning never removes
// live leases: a fresh lease with a full concurrency cap still denies.
func TestRedisAllowFreshLeaseStillCounts(t *testing.T) {
	_, client := newTestRedis(t)
	rl := NewRedis(client, "gwtest", 100, 1)

	now := time.Now()
	ok, _, _, err := rl.Allow(context.Background(), "s", now)
	if err != nil || !ok {
		t.Fatalf("first request must pass, ok=%v err=%v", ok, err)
	}
	ok, _, _, err = rl.Allow(context.Background(), "s", now.Add(time.Second))
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if ok {
		t.Fatal("fresh lease must still consume concurrency capacity")
	}
}

// TestRedisAllowFailsClosedOnRedisError covers infrastructure failure
// injection: a broken Redis must surface as ErrUnavailable, never as a
// plain denial, and the returned release must be a safe no-op.
func TestRedisAllowFailsClosedOnRedisError(t *testing.T) {
	mr, client := newTestRedis(t)
	rl := NewRedis(client, "gwtest", 10, 5)
	mr.SetError("simulated redis outage")

	ok, retryAfter, release, err := rl.Allow(context.Background(), "s", time.Now())
	if ok || retryAfter != 0 {
		t.Fatalf("infrastructure failure must not look like a denial (ok=%v retryAfter=%v)", ok, retryAfter)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	release()
	release() // idempotent no-op
}

// TestRedisAllowHonorsContextCancellation covers cancellation: an
// already-canceled context must return promptly with ErrUnavailable and a
// no-op release.
func TestRedisAllowHonorsContextCancellation(t *testing.T) {
	_, client := newTestRedis(t)
	rl := NewRedis(client, "gwtest", 10, 5)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, _, release, err := rl.Allow(ctx, "s", time.Now())
	if ok {
		t.Fatal("canceled request must not be admitted")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("canceled Redis call must surface as ErrUnavailable, got %v", err)
	}
	release()
}

// TestRedisReleaseIdempotent pins that releasing the same lease twice is
// safe: the second ZREM removes nothing and must not error or deny others.
func TestRedisReleaseIdempotent(t *testing.T) {
	_, client := newTestRedis(t)
	rl := NewRedis(client, "gwtest", 100, 1)
	now := time.Now()

	ok, _, release, err := rl.Allow(context.Background(), "s", now)
	if err != nil || !ok {
		t.Fatalf("first request must pass, ok=%v err=%v", ok, err)
	}
	release()
	release()

	ok, _, _, err = rl.Allow(context.Background(), "s", now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("admission after double release must pass, ok=%v err=%v", ok, err)
	}
}
