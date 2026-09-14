// Package limiter implements per-subject rate and concurrency limiting.
// This in-memory implementation is development-only; production multi-instance
// deployments must use the Redis-backed limiter behind the same interface.
package limiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Error reports a limit denial.
type Error struct{ Code string }

func (e *Error) Error() string { return "limiter: " + e.Code }

// ErrUnavailable signals a limiter infrastructure failure (e.g. Redis
// unreachable in multi-instance mode). Callers must map it to a 503-class
// upstream failure, never to a rate-limit 429.
var ErrUnavailable = errors.New("limiter unavailable")

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

// Gate is the rate/concurrency boundary implemented by the in-memory
// development limiter and the Redis multi-instance limiter.
type Gate interface {
	// Allow consumes one rate slot and acquires a concurrency slot. On
	// denial, retryAfter reports when a slot frees up (zero when unknown).
	// A non-nil err reports limiter infrastructure failure; in that case ok
	// is false, release is a no-op, and retryAfter is meaningless.
	Allow(ctx context.Context, subject string, now time.Time) (ok bool, retryAfter time.Duration, release func(), err error)
}

// Allow reports whether one more request is permitted for the subject this
// minute, and acquires a concurrency slot. release must be called when the
// request finishes.
func (l *Limiter) allowLegacy(subject string, now time.Time) (ok bool, release func()) {
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

// Allow implements Gate for the in-memory development limiter. The in-memory
// limiter has no infrastructure failure mode, so err is always nil.
func (l *Limiter) Allow(_ context.Context, subject string, now time.Time) (bool, time.Duration, func(), error) {
	ok, release := l.allowLegacy(subject, now)
	if !ok {
		return false, l.retryAfter(subject, now), func() {}, nil
	}
	return true, 0, release, nil
}

// retryAfter estimates when the next rate slot frees; zero when unknown.
func (l *Limiter) retryAfter(subject string, now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	wc, ok := l.counts[subject]
	if !ok {
		return 0
	}
	d := l.window - now.Sub(wc.windowStart)
	if d < 0 {
		return 0
	}
	return d
}
