package httpapi

// Readiness tests: named checks, short independent deadlines, and the
// degradation-vs-denial distinction — a required check failing withdraws
// readiness (503, the client-facing denial), a non-required check failing
// reports "degraded" while staying ready, and the body always names the
// failing dependency.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func decodeReadiness(t *testing.T, rec *httptest.ResponseRecorder) readinessBody {
	t.Helper()
	var body readinessBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("readiness body %q: %v", rec.Body.String(), err)
	}
	return body
}

// TestReadinessAllHealthy: every check ok renders ready with per-check state.
func TestReadinessAllHealthy(t *testing.T) {
	r := NewReadiness(nil)
	r.Register("database", true, func(context.Context) error { return nil })
	r.Register("redis", true, func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeReadiness(t, rec)
	if body.Status != "ready" {
		t.Fatalf("status field = %q, want ready", body.Status)
	}
	for _, name := range []string{"database", "redis"} {
		if body.Checks[name] != "ok" {
			t.Fatalf("check %s = %q, want ok", name, body.Checks[name])
		}
	}
}

// TestReadinessRequiredFailureNamesDependency: each required dependency
// failing flips readiness to unavailable and is named in the body.
func TestReadinessRequiredFailureNamesDependency(t *testing.T) {
	for _, tc := range []struct {
		name     string
		register func(*Readiness)
	}{
		{"database", func(r *Readiness) {
			r.Register("database", true, func(context.Context) error { return errors.New("ping failed") })
		}},
		{"redis", func(r *Readiness) {
			r.Register("redis", true, func(context.Context) error { return errors.New("timeout") })
		}},
		{"queue", func(r *Readiness) {
			r.Register("queue", true, func(context.Context) error { return errors.New("claim query failed") })
		}},
		{"worker", func(r *Readiness) {
			r.Register("worker", true, func(context.Context) error { return errors.New("pool not accepting") })
		}},
		{"settlement", func(r *Readiness) {
			r.Register("settlement", true, func(context.Context) error { return errors.New("backlog 900 exceeds threshold 100") })
		}},
	} {
		r := NewReadiness(nil)
		tc.register(r)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status = %d, want 503", tc.name, rec.Code)
		}
		body := decodeReadiness(t, rec)
		if body.Status != "unavailable" {
			t.Fatalf("%s: status field = %q, want unavailable", tc.name, body.Status)
		}
		if body.Checks[tc.name] != "failed" {
			t.Fatalf("%s: check state = %q, want failed", tc.name, body.Checks[tc.name])
		}
	}
}

// TestReadinessOptionalDegradationStaysReady: a non-required check failing
// reports degraded without withdrawing readiness.
func TestReadinessOptionalDegradationStaysReady(t *testing.T) {
	r := NewReadiness(nil)
	r.Register("database", true, func(context.Context) error { return nil })
	r.Register("export", false, func(context.Context) error { return errors.New("sink slow") })

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("optional degradation must stay ready, got %d", rec.Code)
	}
	body := decodeReadiness(t, rec)
	if body.Status != "ready" || body.Checks["export"] != "degraded" || body.Checks["database"] != "ok" {
		t.Fatalf("unexpected body %+v", body)
	}
}

// TestReadinessIndependentDeadlines: one hung dependency must not consume
// the others' budget — every check gets its own short deadline.
func TestReadinessIndependentDeadlines(t *testing.T) {
	r := NewReadiness(nil)
	r.Register("hung", true, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r.Register("fast", true, func(context.Context) error { return nil })

	start := time.Now()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	elapsed := time.Since(start)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("hung dependency must fail readiness, got %d", rec.Code)
	}
	if elapsed > checkDeadline+time.Second {
		t.Fatalf("checks must be bounded by %v, took %v", checkDeadline, elapsed)
	}
	if body := decodeReadiness(t, rec); body.Checks["hung"] != "failed" || body.Checks["fast"] != "ok" {
		t.Fatalf("fast check must still answer beside the hung one: %+v", body.Checks)
	}
}

// TestLivenessStaysProcessOnly pins the /healthz contract via the mux: it
// never runs dependency probes (no readiness handler attached affects it).
func TestLivenessStaysProcessOnly(t *testing.T) {
	r := NewReadiness(nil)
	r.Register("database", true, func(context.Context) error { return errors.New("down") })
	mux := NewMux(http.NotFoundHandler(), Deps{Ready: r, Metrics: http.NotFoundHandler()})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness must stay process-only, got %d", rec.Code)
	}
}
