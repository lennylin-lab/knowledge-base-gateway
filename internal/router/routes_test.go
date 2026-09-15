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
