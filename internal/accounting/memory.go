package accounting

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryBudget is the single-process money-budget tracker for development
// mode and tests. It mirrors the Redis-backed gate semantics (one atomic
// check-and-charge across the subject's and the tenant's daily and monthly
// UTC counters, exactly-once finalize); multi-instance deployments must use
// the Redis gate because this one only sees its own process.
type MemoryBudget struct {
	mu        sync.Mutex
	used      map[string]int64    // counter key -> charged micros
	finalized map[string]struct{} // reservation IDs already settled or released
	nextID    int64
}

// NewMemoryBudget builds an empty in-memory budget gate.
func NewMemoryBudget() *MemoryBudget {
	return &MemoryBudget{used: map[string]int64{}, finalized: map[string]struct{}{}}
}

// memoryBudgetReservation remembers what was charged so settle/release
// adjust exactly the charged counters by exactly the charged amounts.
type memoryBudgetReservation struct {
	g        *MemoryBudget
	id       string
	keys     []string // charged counter keys, in BudgetLimits field order
	estimate int64
}

// Reserve implements BudgetGate. Limits of zero mean uncapped and are never
// charged. A denial leaves all counters untouched and both dimensions are
// checked before any counter is charged.
func (m *MemoryBudget) Reserve(_ context.Context, subjectID, tenantID string, limits BudgetLimits, estimate int64, now time.Time) (BudgetReservation, error) {
	if estimate <= 0 || !limits.Configured() {
		return noopBudget{}, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := budgetKeys("", subjectID, tenantID, limits, now)
	charged := make([]string, 0, 4)
	for i, key := range keys {
		if budgetLimitAt(limits, i) <= 0 {
			continue
		}
		if m.used[key]+estimate > budgetLimitAt(limits, i) {
			return nil, denialAt(limits, i, now)
		}
		charged = append(charged, key)
	}
	m.nextID++
	res := &memoryBudgetReservation{
		g: m, id: fmt.Sprintf("%d-%d", now.UnixNano(), m.nextID),
		keys: charged, estimate: estimate,
	}
	for _, key := range charged {
		m.used[key] += estimate
	}
	return res, nil
}

// finalize adjusts each charged counter by delta exactly once.
func (r *memoryBudgetReservation) finalize(delta int64) {
	r.g.mu.Lock()
	defer r.g.mu.Unlock()
	if _, doneBefore := r.g.finalized[r.id]; doneBefore {
		return
	}
	r.g.finalized[r.id] = struct{}{}
	if delta == 0 {
		return
	}
	for _, key := range r.keys {
		r.g.used[key] += delta
		if r.g.used[key] <= 0 {
			delete(r.g.used, key)
		}
	}
}

// Settle adjusts the estimate to the computed cost; nil (unknown cost)
// retains the conservative reservation.
func (r *memoryBudgetReservation) Settle(cost *int64) {
	if cost == nil {
		return
	}
	r.finalize(*cost - r.estimate)
}

// Release refunds the estimate; repeated calls are no-ops.
func (r *memoryBudgetReservation) Release() {
	r.finalize(-r.estimate)
}

// noopBudget is the no-op reservation for requests without configured
// budgets (mirrors quota.Done).
type noopBudget struct{}

func (noopBudget) Settle(*int64) {}
func (noopBudget) Release()      {}

// budgetKeys names the four UTC period counters in BudgetLimits field
// order: subject day, subject month, tenant day, tenant month.
func budgetKeys(prefix, subjectID, tenantID string, limits BudgetLimits, now time.Time) [4]string {
	return [4]string{
		prefix + ":budget:d:s:" + subjectID + ":" + now.UTC().Format("20060102"),
		prefix + ":budget:m:s:" + subjectID + ":" + now.UTC().Format("200601"),
		prefix + ":budget:d:t:" + tenantID + ":" + now.UTC().Format("20060102"),
		prefix + ":budget:m:t:" + tenantID + ":" + now.UTC().Format("200601"),
	}
}

// budgetLimitAt indexes the limits in the same field order as budgetKeys.
func budgetLimitAt(l BudgetLimits, i int) int64 {
	switch i {
	case 0:
		return l.SubjectDaily
	case 1:
		return l.SubjectMonthly
	case 2:
		return l.TenantDaily
	default:
		return l.TenantMonthly
	}
}

// denialAt builds the denial for the counter that rejected: the scope label
// and the UTC boundary Retry-After of the denied period.
func denialAt(_ BudgetLimits, i int, now time.Time) *Error {
	scope := ScopeSubject
	if i >= 2 {
		scope = ScopeTenant
	}
	retryAfter := retryAfterDay(now)
	if i%2 == 1 {
		retryAfter = retryAfterMonth(now)
	}
	return &Error{Code: CodeBudgetExceeded, RetryAfter: retryAfter, Scope: scope}
}
