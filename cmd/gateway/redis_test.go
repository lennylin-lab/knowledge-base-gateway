package main

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// TestRedisOptionsURLForms pins the only accepted GATEWAY_REDIS_ADDR shape:
// a redis:// or rediss:// URL that carries credentials and connection
// options.
func TestRedisOptionsURLForms(t *testing.T) {
	t.Run("redis URL carries password and database", func(t *testing.T) {
		opts, err := redisOptions("redis://:s3cret@redis.internal:6380/2")
		if err != nil {
			t.Fatalf("redisOptions: %v", err)
		}
		if opts.Addr != "redis.internal:6380" {
			t.Errorf("Addr = %q, want redis.internal:6380", opts.Addr)
		}
		if opts.Password != "s3cret" {
			t.Errorf("Password not parsed from URL")
		}
		if opts.Username != "" {
			t.Errorf("Username = %q, want empty for password-only URL", opts.Username)
		}
		if opts.DB != 2 {
			t.Errorf("DB = %d, want 2", opts.DB)
		}
	})

	t.Run("redis URL carries ACL username", func(t *testing.T) {
		opts, err := redisOptions("redis://gateway:s3cret@redis.internal:6379")
		if err != nil {
			t.Fatalf("redisOptions: %v", err)
		}
		if opts.Username != "gateway" || opts.Password != "s3cret" {
			t.Errorf("ACL credentials not parsed: username=%q", opts.Username)
		}
	})

	t.Run("rediss URL enables TLS", func(t *testing.T) {
		opts, err := redisOptions("rediss://:s3cret@redis.internal:6380")
		if err != nil {
			t.Fatalf("redisOptions: %v", err)
		}
		if opts.TLSConfig == nil {
			t.Fatal("rediss URL must configure TLS")
		}
		if opts.TLSConfig.ServerName != "redis.internal" {
			t.Errorf("TLS ServerName = %q, want redis.internal", opts.TLSConfig.ServerName)
		}
	})
}

// TestRedisOptionsRejectsNonURL pins the breaking change: bare host:port and
// other schemes are no longer accepted.
func TestRedisOptionsRejectsNonURL(t *testing.T) {
	for _, raw := range []string{
		"127.0.0.1:6379",
		"redis.internal:6379",
		"unix:///tmp/redis.sock",
		"http://redis.internal:6379",
	} {
		if _, err := redisOptions(raw); err == nil {
			t.Fatalf("redisOptions(%q) succeeded, want a redis:// URL error", raw)
		} else if !strings.Contains(err.Error(), "redis://") {
			t.Fatalf("redisOptions(%q) error = %v, want a message naming redis://", raw, err)
		}
	}
}

// TestRedisOptionsParseErrorDoesNotLeakSecret pins the redaction boundary:
// net/url errors embed the raw URL, so the parse failure must be classified
// instead of wrapped.
func TestRedisOptionsParseErrorDoesNotLeakSecret(t *testing.T) {
	const password = "super-secret-password"
	for _, raw := range []string{
		"redis://:" + password + "@127.0.0.1:not-a-port",
		"rediss://:" + password + "@127.0.0.1:not-a-port",
	} {
		_, err := redisOptions(raw)
		if err == nil {
			t.Fatalf("redisOptions(%q) succeeded, want parse error", raw)
		}
		if strings.Contains(err.Error(), password) {
			t.Fatalf("parse error leaks the password: %v", err)
		}
		if strings.Contains(err.Error(), "not-a-port") {
			t.Fatalf("parse error leaks the raw URL: %v", err)
		}
	}
}

// TestRedisReadyURLPassword proves readiness authenticates with the URL form
// against a password-protected server, and that a wrong password still fails.
func TestRedisReadyURLPassword(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cret")

	if err := redisReady(context.Background(), "redis://:s3cret@"+mr.Addr()); err != nil {
		t.Fatalf("redisReady with URL password: %v", err)
	}
	if err := redisReady(context.Background(), "redis://:wrong@"+mr.Addr()); err == nil {
		t.Fatal("redisReady accepted a wrong password")
	}
}
