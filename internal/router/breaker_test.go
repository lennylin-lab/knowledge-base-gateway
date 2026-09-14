package router

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerOpensAndRecovers(t *testing.T) {
	b := NewBreaker(2, 30*time.Millisecond)
	if !b.Allow() {
		t.Fatal("closed breaker must allow")
	}
	b.Record(false)
	if !b.Allow() {
		t.Fatal("one failure must not open")
	}
	b.Record(false)
	if b.Allow() {
		t.Fatal("breaker must open after threshold consecutive failures")
	}
	// Half-open probe after cool-down.
	time.Sleep(60 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("half-open must admit one probe")
	}
	if b.Allow() {
		t.Fatal("half-open admits only one probe at a time")
	}
	b.Record(false)
	if b.Allow() {
		t.Fatal("failed probe must re-open")
	}
	// Successful probe after another cool-down closes the breaker.
	time.Sleep(60 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("probe admitted after cool-down")
	}
	b.Record(true)
	if !b.Allow() {
		t.Fatal("successful probe must close the breaker")
	}
	if s := b.State(); s != "closed" {
		t.Fatalf("want closed after successful probe, got %s", s)
	}
}

// TestBreakerSuccessInterruptsFailureStreak pins consecutive-failure
// semantics: a success in the middle of the failure window must prevent the
// breaker from opening.
func TestBreakerSuccessInterruptsFailureStreak(t *testing.T) {
	b := NewBreaker(2, time.Minute)
	b.Record(false)
	b.Record(true)
	b.Record(false)
	if !b.Allow() {
		t.Fatal("non-consecutive failures must not open the breaker")
	}
}

// TestBreakerConcurrentHalfOpen admits exactly one probe among many
// concurrent callers once the cool-down has elapsed.
func TestBreakerConcurrentHalfOpen(t *testing.T) {
	b := NewBreaker(1, 25*time.Millisecond)
	b.Record(false)
	if b.Allow() {
		t.Fatal("breaker must be open")
	}
	time.Sleep(60 * time.Millisecond)

	const callers = 32
	var admitted atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if b.Allow() {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := admitted.Load(); got != 1 {
		t.Fatalf("exactly one half-open probe must be admitted, got %d", got)
	}
}

func TestNilAndDisabledBreakerAlwaysAllow(t *testing.T) {
	var nilBreaker *Breaker
	if !nilBreaker.Allow() {
		t.Fatal("nil breaker must allow")
	}
	nilBreaker.Record(false) // must not panic
	disabled := NewBreaker(0, time.Minute)
	if !disabled.Allow() {
		t.Fatal("zero threshold must disable the breaker")
	}
	disabled.Record(false)
	if !disabled.Allow() {
		t.Fatal("disabled breaker must keep allowing")
	}
}

func TestAvailablePrefersPriorityAndSkipsOpen(t *testing.T) {
	r := NewRoutes()
	primary := Route{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, time.Minute)}
	backup := Route{ProviderName: "b", Priority: 20, Enabled: true, Breaker: NewBreaker(1, time.Minute)}
	r.SetRoutes("m", []Route{primary, backup})

	got := r.Available("m")
	if len(got) != 2 || got[0].ProviderName != "p" {
		t.Fatalf("want primary first, got %+v", got)
	}
	// Open the primary.
	r.Record("m", "p", false)
	got = r.Available("m")
	if len(got) != 1 || got[0].ProviderName != "b" {
		t.Fatalf("want backup only while primary open, got %+v", got)
	}
	// All open: half-open probe of the best route is forced.
	r.Record("m", "b", false) // opens backup too (threshold 1)
	got = r.Available("m")
	if len(got) != 1 || got[0].ProviderName != "p" {
		t.Fatalf("want forced probe of primary, got %+v", got)
	}
	// Disabled routes never appear.
	r.SetRoutes("m", []Route{primary, {ProviderName: "x", Priority: 1, Enabled: false}})
	if got = r.Available("m"); len(got) != 1 || got[0].ProviderName != "p" {
		t.Fatalf("disabled route must be skipped, got %+v", got)
	}
}
