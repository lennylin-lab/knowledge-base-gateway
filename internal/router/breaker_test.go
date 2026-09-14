package router

import (
	"testing"
	"time"
)

func TestBreakerOpensAndRecovers(t *testing.T) {
	now := time.Now()
	b := NewBreaker(2, time.Second)
	if !b.Allow(now) {
		t.Fatal("closed breaker must allow")
	}
	b.Record(now, false)
	if !b.Allow(now) {
		t.Fatal("one failure must not open")
	}
	b.Record(now, false)
	if b.Allow(now) {
		t.Fatal("breaker must open after threshold failures")
	}
	// Half-open probe after cool-down.
	later := now.Add(2 * time.Second)
	if !b.Allow(later) {
		t.Fatal("half-open must admit one probe")
	}
	if b.Allow(later) {
		t.Fatal("half-open admits only one probe at a time")
	}
	b.Record(later, false)
	if b.Allow(later) {
		t.Fatal("failed probe must re-open")
	}
	recovered := now.Add(5 * time.Second)
	if !b.Allow(recovered) {
		t.Fatal("probe admitted after cool-down")
	}
	b.Record(recovered, true)
	if !b.Allow(recovered) {
		t.Fatal("successful probe must close the breaker")
	}
}

func TestAvailablePrefersPriorityAndSkipsOpen(t *testing.T) {
	r := NewRoutes()
	primary := Route{ProviderName: "p", Priority: 10, Enabled: true, Breaker: NewBreaker(1, time.Minute)}
	backup := Route{ProviderName: "b", Priority: 20, Enabled: true, Breaker: NewBreaker(1, time.Minute)}
	r.SetRoutes("m", []Route{primary, backup})

	now := time.Now()
	got := r.Available("m", now)
	if len(got) != 2 || got[0].ProviderName != "p" {
		t.Fatalf("want primary first, got %+v", got)
	}
	// Open the primary.
	r.Record("m", "p", now, false)
	got = r.Available("m", now)
	if len(got) != 1 || got[0].ProviderName != "b" {
		t.Fatalf("want backup only while primary open, got %+v", got)
	}
	// All open: half-open probe of the best route is forced.
	r.Record("m", "b", now, false) // opens backup too (threshold 1)
	got = r.Available("m", now)
	if len(got) != 1 || got[0].ProviderName != "p" {
		t.Fatalf("want forced probe of primary, got %+v", got)
	}
	// Disabled routes never appear.
	r.SetRoutes("m", []Route{primary, {ProviderName: "x", Priority: 1, Enabled: false}})
	if got = r.Available("m", now); len(got) != 1 || got[0].ProviderName != "p" {
		t.Fatalf("disabled route must be skipped, got %+v", got)
	}
}
