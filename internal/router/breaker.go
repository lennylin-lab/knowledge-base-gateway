// Package router owns provider selection, health state (circuit breakers),
// and ordered primary/backup route tables for public models.
package router

import (
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

// Breaker is a consecutive-failure circuit breaker with a single half-open
// probe. The state machine (failure window, open/half-open transitions, and
// permit accounting) is delegated to failsafe-go's circuitbreaker; this type
// only adapts it to the router's two-phase admit/record flow, because route
// admission happens in Available before any provider call is attempted.
//
// A zero threshold disables the breaker (always closed). A nil *Breaker is
// likewise always closed.
type Breaker struct {
	cb circuitbreaker.CircuitBreaker[struct{}]
}

// NewBreaker builds a breaker that opens after threshold consecutive
// failures and half-open probes after coolDown. In half-open state exactly
// one probe is admitted: a successful probe closes the breaker, a failed
// probe re-opens it for another cool-down.
func NewBreaker(threshold int, coolDown time.Duration) *Breaker {
	if threshold <= 0 {
		return &Breaker{}
	}
	return &Breaker{
		cb: circuitbreaker.NewBuilder[struct{}]().
			// A failure window the size of the threshold yields
			// consecutive-failure tripping: any success evicts an older
			// failure, so the breaker opens only when the last threshold
			// outcomes all failed.
			WithFailureThreshold(uint(threshold)).
			// One permitted execution in half-open: exactly one probe.
			WithSuccessThreshold(1).
			WithDelay(coolDown).
			Build(),
	}
}

// Allow reports whether one call may proceed. When open it returns false;
// after the cool-down it transitions to half-open and admits exactly one
// probe.
func (b *Breaker) Allow() bool {
	if b == nil || b.cb == nil {
		return true
	}
	return b.cb.TryAcquirePermit()
}

// Record reports the outcome of an admitted call. In half-open state a
// successful probe closes the breaker and a failed probe re-opens it for
// another cool-down.
func (b *Breaker) Record(success bool) {
	if b == nil || b.cb == nil {
		return
	}
	if success {
		b.cb.RecordSuccess()
		return
	}
	b.cb.RecordFailure()
}

// State reports the breaker state name ("closed", "open", "half-open") for
// diagnostics. Disabled breakers report "closed".
func (b *Breaker) State() string {
	if b == nil || b.cb == nil {
		return "closed"
	}
	return b.cb.State().String()
}
