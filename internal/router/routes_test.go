package router

// Concurrency tests for the all-routes-open behavior: Available must never
// bypass a breaker permit, must admit no traffic while every breaker is open,
// and must let exactly one half-open probe through per breaker after the
// cool-down.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runBurst calls r.Available concurrently and returns the total number of
// routes admitted across all callers, plus per-provider admission counts.
func runBurst(r *Routes, model string, callers int) (int32, map[string]int) {
	var total atomic.Int32
	var mu sync.Mutex
	admitted := map[string]int{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, rt := range r.Available(model) {
				total.Add(1)
				mu.Lock()
				admitted[rt.ProviderName]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return total.Load(), admitted
}

// TestAvailableAdmitsNoTrafficWhileAllOpen pins AC4: with every route breaker
// open and no cool-down elapsed, concurrent callers get no route at all —
// the old forced-probe fallback is gone.
func TestAvailableAdmitsNoTrafficWhileAllOpen(t *testing.T) {
	r := NewRoutes()
	r.SetRoutes("m", []Route{
		{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, 50*time.Millisecond)},
		{ProviderName: "b", Priority: 20, Enabled: true, Breaker: NewBreaker(1, 50*time.Millisecond)},
	})
	r.Record("m", "p", false)
	r.Record("m", "b", false)

	total, admitted := runBurst(r, "m", 32)
	if total != 0 {
		t.Fatalf("all-open routes must admit no traffic, got %d admissions (%+v)", total, admitted)
	}
}

// TestAvailableSingleHalfOpenProbeAfterCooldown pins the recovery contract:
// exactly one probe is admitted among concurrent callers after the cool-down,
// the pending probe blocks further admission until it is recorded, and a
// successful probe closes the breaker again.
func TestAvailableSingleHalfOpenProbeAfterCooldown(t *testing.T) {
	r := NewRoutes()
	r.SetRoutes("m", []Route{
		{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, 25*time.Millisecond)},
	})
	r.Record("m", "p", false)

	if total, _ := runBurst(r, "m", 32); total != 0 {
		t.Fatalf("open breaker must admit nothing before the cool-down, got %d", total)
	}
	time.Sleep(60 * time.Millisecond)
	if total, _ := runBurst(r, "m", 32); total != 1 {
		t.Fatalf("exactly one half-open probe must be admitted, got %d", total)
	}
	// The admitted probe holds the only permit until Record reports it.
	if total, _ := runBurst(r, "m", 32); total != 0 {
		t.Fatalf("pending half-open probe must block further admission, got %d", total)
	}
	// A successful probe closes the breaker and restores normal admission.
	r.Record("m", "p", true)
	got := r.Available("m")
	if len(got) != 1 || got[0].ProviderName != "p" {
		t.Fatalf("successful probe must close the breaker, got %+v", got)
	}
}

// TestAvailableEachBreakerProbesOnce pins per-breaker probe semantics for a
// primary/backup pair that failed together: after the shared cool-down each
// breaker admits exactly one probe across all concurrent callers, so a
// failover plan can carry both probes but no additional traffic.
func TestAvailableEachBreakerProbesOnce(t *testing.T) {
	r := NewRoutes()
	r.SetRoutes("m", []Route{
		{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, 25*time.Millisecond)},
		{ProviderName: "b", Priority: 20, Enabled: true, Breaker: NewBreaker(1, 25*time.Millisecond)},
	})
	r.Record("m", "p", false)
	r.Record("m", "b", false)
	time.Sleep(60 * time.Millisecond)

	total, admitted := runBurst(r, "m", 32)
	if total != 2 || admitted["p"] != 1 || admitted["b"] != 1 {
		t.Fatalf("each breaker must admit exactly one probe, got total=%d %+v", total, admitted)
	}
}

// TestProviderBreakersSummarizesWorstState pins the management health
// summary: ProviderBreakers aggregates route counts and the worst breaker
// state per provider across every model, and omits providers with no routes.
func TestProviderBreakersSummarizesWorstState(t *testing.T) {
	r := NewRoutes()
	r.SetRoutes("m", []Route{
		{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, time.Hour)},
		{ProviderName: "b", Priority: 20, Enabled: true, Breaker: NewBreaker(1, time.Hour)},
	})
	r.SetRoutes("n", []Route{
		{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, time.Hour)},
	})

	got := r.ProviderBreakers()
	if len(got) != 2 {
		t.Fatalf("providers = %+v, want p and b", got)
	}
	if s := got["p"]; s.TotalRoutes != 2 || s.OpenRoutes != 0 || s.State != "closed" {
		t.Fatalf("p summary = %+v, want 2 routes all closed", s)
	}

	// Trip p's route on model m; the summary must report the open route and
	// the worst state across both of p's routes.
	r.Record("m", "p", false)
	got = r.ProviderBreakers()
	if s := got["p"]; s.TotalRoutes != 2 || s.OpenRoutes != 1 || s.State != "open" {
		t.Fatalf("p summary after trip = %+v, want 1 of 2 routes open", s)
	}
	if s := got["b"]; s.TotalRoutes != 1 || s.State != "closed" {
		t.Fatalf("b summary = %+v, want closed", s)
	}
}
