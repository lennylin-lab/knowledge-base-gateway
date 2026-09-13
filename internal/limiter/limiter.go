// Package limiter implements per-subject rate and concurrency limiting.
// This in-memory implementation is development-only; production multi-instance
// deployments must use the Redis-backed limiter behind the same interface.
package limiter

import (
	"sync"
	"time"
)

// Error reports a limit denial.
type Error struct{ Code string }

func (e *Error) Error() string { return "limiter: " + e.Code }

// Limiter is a development-mode fixed-window rate limiter with concurrency caps.
type Limiter struct {
	mu      sync.Mutex
	window  time.Duration
	rate    int
	maxConc int
	counts  map[string]*windowCount
	active  map[string]int
}

type windowCount struct {
	windowStart time.Time
	count       int
}

// New builds a limiter allowing rate requests per minute and maxConcurrent
// in-flight requests per subject.
func New(ratePerMinute, maxConcurrent int) *Limiter {
	return &Limiter{
		window:  time.Minute,
		rate:    ratePerMinute,
		maxConc: maxConcurrent,
		counts:  map[string]*windowCount{},
		active:  map[string]int{},
	}
}

// Allow reports whether one more request is permitted for the subject this
// minute, and acquires a concurrency slot. release must be called when the
// request finishes.
func (l *Limiter) Allow(subject string, now time.Time) (ok bool, release func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	wc := l.counts[subject]
	if wc == nil || now.Sub(wc.windowStart) >= l.window {
		wc = &windowCount{windowStart: now}
		l.counts[subject] = wc
	}
	if wc.count >= l.rate {
		return false, func() {}
	}
	if l.active[subject] >= l.maxConc {
		return false, func() {}
	}
	wc.count++
	l.active[subject]++
	return true, func() {
		l.mu.Lock()
		l.active[subject]--
		l.mu.Unlock()
	}
}
