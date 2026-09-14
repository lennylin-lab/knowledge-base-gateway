package limiter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisAllow exercises the atomic Lua rate/concurrency script against a
// real Redis. Skipped unless TEST_REDIS_ADDR is set (e.g. 127.0.0.1:6379).
func TestRedisAllow(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis limiter test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}

	rl := NewRedis(client, "gwtest", 2, 1)
	now := time.Now()

	ok, _, release, err := rl.Allow(context.Background(), "s", now)
	if err != nil {
		t.Fatalf("healthy redis must not surface infra error: %v", err)
	}
	if !ok {
		t.Fatal("first request must pass")
	}
	// Concurrency cap: a second in-flight request is denied.
	ok, _, release2, _ := rl.Allow(context.Background(), "s", now)
	if ok {
		t.Fatal("second concurrent request must be denied")
	}
	// Rate cap after releasing the concurrency lease.
	release()
	ok, _, release3, _ := rl.Allow(context.Background(), "s", now)
	if !ok {
		t.Fatal("request after lease release must pass")
	}
	ok, retryAfter, _, _ := rl.Allow(context.Background(), "s", now)
	if ok {
		t.Fatal("third request in the window must be denied")
	}
	if retryAfter <= 0 {
		t.Fatal("rate denial should carry a computable Retry-After")
	}
	release3()

	// Cancellation-safe: releasing twice must not error.
	release3()
	release3()

	// Namespace isolation.
	if ok, _, _, _ := rl.Allow(context.Background(), "other", now); !ok {
		t.Fatal("other subject must have its own counters")
	}
	_ = release2
}
