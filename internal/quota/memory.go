package quota

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Memory is the single-process quota tracker for development mode. It mirrors
// the Redis-backed gate semantics; multi-instance deployments must use the
// Redis gate because this one only sees its own process.
type Memory struct {
	mu        sync.Mutex
	used      map[string]int64    // period key -> charged tokens
	finalized map[string]struct{} // reservation IDs already settled or released
	nextID    int64
}

// NewMemory builds an empty in-memory quota gate.
func NewMemory() *Memory {
	return &Memory{used: map[string]int64{}, finalized: map[string]struct{}{}}
}

// memoryReservation remembers what was charged so settle/release adjust
// exactly the charged periods by exactly the charged amounts.
type memoryReservation struct {
	g            *Memory
	id           string
	day          string
	month        string
	dayCharged   bool
	monthCharged bool
	estimate     int64
}

// Reserve implements Gate. Limits of zero mean unlimited and are never
// charged. A denial leaves all counters untouched.
func (m *Memory) Reserve(_ context.Context, subject string, limits Limits, estimate int64, now time.Time) (Reservation, error) {
	if estimate <= 0 || !limits.Configured() {
		return Done, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	day, month := DayKey("", subject, now), MonthKey("", subject, now)
	if limits.DailyTokens > 0 && m.used[day]+estimate > limits.DailyTokens {
		return nil, &Error{Code: CodeQuotaExceeded, RetryAfter: retryAfterDay(now)}
	}
	if limits.MonthlyTokens > 0 && m.used[month]+estimate > limits.MonthlyTokens {
		return nil, &Error{Code: CodeQuotaExceeded, RetryAfter: retryAfterMonth(now)}
	}
	m.nextID++
	res := &memoryReservation{
		g: m, id: fmt.Sprintf("%d-%d", now.UnixNano(), m.nextID),
		day: day, month: month, estimate: estimate,
		dayCharged: limits.DailyTokens > 0, monthCharged: limits.MonthlyTokens > 0,
	}
	if res.dayCharged {
		m.used[day] += estimate
	}
	if res.monthCharged {
		m.used[month] += estimate
	}
	return res, nil
}

// finalize applies delta to each charged period exactly once.
func (r *memoryReservation) finalize(delta int64) {
	r.g.mu.Lock()
	defer r.g.mu.Unlock()
	if _, doneBefore := r.g.finalized[r.id]; doneBefore {
		return
	}
	r.g.finalized[r.id] = struct{}{}
	if delta == 0 {
		return
	}
	if r.dayCharged {
		r.g.used[r.day] += delta
		if r.g.used[r.day] <= 0 {
			delete(r.g.used, r.day)
		}
	}
	if r.monthCharged {
		r.g.used[r.month] += delta
		if r.g.used[r.month] <= 0 {
			delete(r.g.used, r.month)
		}
	}
}

// Settle adjusts the estimate to the reported total; nil (unknown usage)
// retains the conservative reservation.
func (r *memoryReservation) Settle(total *int64) {
	if total == nil {
		return
	}
	r.finalize(*total - r.estimate)
}

// Release returns the estimate; repeated calls are no-ops.
func (r *memoryReservation) Release() {
	r.finalize(-r.estimate)
}
