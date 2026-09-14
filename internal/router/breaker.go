// Package router owns provider selection, health state (circuit breakers),
// and ordered primary/backup route tables for public models.
package router

import (
	"sync"
	"time"
)

// Breaker is a consecutive-failure circuit breaker with a half-open probe.
// Zero thresholds disable it (always closed).
type Breaker struct {
	mu               sync.Mutex
	failureThreshold int
	coolDown         time.Duration

	failures    int
	openedUntil time.Time
	probing     bool
}

// NewBreaker builds a breaker that opens after threshold consecutive
// failures and half-open probes after coolDown.
func NewBreaker(threshold int, coolDown time.Duration) *Breaker {
	return &Breaker{failureThreshold: threshold, coolDown: coolDown}
}

// Allow reports whether one call may proceed. When open it returns false;
// after the cool-down it admits exactly one half-open probe.
func (b *Breaker) Allow(now time.Time) bool {
	if b == nil || b.failureThreshold <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.probing {
		return false // one probe at a time
	}
	if now.Before(b.openedUntil) {
		return false
	}
	if !b.openedUntil.IsZero() {
		b.probing = true // half-open probe
	}
	return true
}

// Record reports the outcome of an admitted call.
func (b *Breaker) Record(now time.Time, success bool) {
	if b == nil || b.failureThreshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		b.failures = 0
		b.openedUntil = time.Time{}
		b.probing = false
		return
	}
	if b.probing {
		// Failed probe: re-open for another cool-down.
		b.probing = false
		b.openedUntil = now.Add(b.coolDown)
		return
	}
	b.failures++
	if b.failures >= b.failureThreshold {
		b.openedUntil = now.Add(b.coolDown)
	}
}
