package accounting

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// budgetReserveScript atomically checks all four UTC money counters and
// charges the estimate. KEYS[1..4]: subject day, subject month, tenant day,
// tenant month. ARGV: estimate, then (limit, ttlMillis) pairs in key order.
// A limit <= 0 means that scope/period is uncapped and never charged. Both
// dimensions share one script, so concurrent gateway instances can never
// oversell a subject or a tenant budget, and subject/tenant checks cannot
// pass independently of their charges. Returns {allowed(0/1), deniedScope(1
// = subject, 2 = tenant), deniedPeriod(1 = day, 2 = month), used} so the
// caller can compute the UTC Retry-After for the denying counter.
var budgetReserveScript = redis.NewScript(`
local estimate = tonumber(ARGV[1])
local limits = {tonumber(ARGV[2]), tonumber(ARGV[4]), tonumber(ARGV[6]), tonumber(ARGV[8])}
local ttls = {tonumber(ARGV[3]), tonumber(ARGV[5]), tonumber(ARGV[7]), tonumber(ARGV[9])}
for i = 1, 4 do
  if limits[i] > 0 then
    local used = tonumber(redis.call('GET', KEYS[i]) or '0')
    if used + estimate > limits[i] then
      local scope = 1
      if i > 2 then scope = 2 end
      local period = 1
      if i % 2 == 0 then period = 2 end
      return {0, scope, period, used}
    end
  end
end
for i = 1, 4 do
  if limits[i] > 0 then
    local used = redis.call('INCRBY', KEYS[i], estimate)
    if used == estimate then
      redis.call('PEXPIRE', KEYS[i], ttls[i])
    end
  end
end
return {1, 0, 0, 0}
`)

// budgetFinalizeScript settles or releases exactly one money reservation.
// KEYS[1..4]: the four counters in key order, KEYS[5]: finalize marker.
// ARGV: delta, the four charged flags, marker TTLMillis. The SET NX marker
// makes the operation exactly-once: settle-after-settle, release-after-
// release, and settle-after-release are all no-ops. Deltas apply only to
// counters that still exist: a period key missing means the budget rolled
// over mid-flight, and the new period must not inherit the adjustment.
var budgetFinalizeScript = redis.NewScript(`
if not redis.call('SET', KEYS[5], '1', 'NX') then
  return 0
end
redis.call('PEXPIRE', KEYS[5], tonumber(ARGV[6]))
local delta = tonumber(ARGV[1])
if delta == 0 then
  return 1
end
for i = 1, 4 do
  if tonumber(ARGV[i + 1]) > 0 and redis.call('EXISTS', KEYS[i]) == 1 then
    redis.call('INCRBY', KEYS[i], delta)
  end
end
return 1
`)

// markerGrace bounds how long a finalize marker survives; it only needs to
// outlive the request that owns the reservation, with margin for retries
// (the same grace the token quota uses).
const markerGrace = 48 * time.Hour

// RedisBudget is the multi-instance money-budget gate backed by Redis.
// Counters are keyed by subject/tenant and UTC period and expire at the
// period boundary plus a grace window, so budgets reset without manual
// cleanup. Redis unavailability fails closed and surfaces as
// limiter.ErrUnavailable.
type RedisBudget struct {
	Client *redis.Client
	Prefix string // key namespace, e.g. "gw"
}

// NewRedisBudget builds a Redis-backed budget gate sharing the limiter's
// key namespace conventions.
func NewRedisBudget(client *redis.Client, prefix string) *RedisBudget {
	return &RedisBudget{Client: client, Prefix: prefix}
}

// redisBudgetReservation remembers the charged counters and estimate so
// settlement adjusts exactly what was reserved.
type redisBudgetReservation struct {
	b        *RedisBudget
	id       string
	keys     [4]string
	charged  [4]bool
	estimate int64
}

// Reserve implements BudgetGate with one atomic script for both dimensions
// and both periods.
func (b *RedisBudget) Reserve(ctx context.Context, subjectID, tenantID string, limits BudgetLimits, estimate int64, now time.Time) (BudgetReservation, error) {
	if estimate <= 0 || !limits.Configured() {
		return noopBudget{}, nil
	}
	// The reservation ID exists before anything is charged: an ID-generation
	// failure therefore cannot leave a charged counter behind.
	id, err := newBudgetReservationID(now)
	if err != nil {
		return nil, unavailable(err)
	}
	keys := budgetKeys(b.Prefix, subjectID, tenantID, limits, now)
	ttls := [4]time.Duration{dayTTL(now), monthTTL(now), dayTTL(now), monthTTL(now)}
	res, err := budgetReserveScript.Run(ctx, b.Client, keys[:],
		estimate,
		limits.SubjectDaily, ttls[0].Milliseconds(),
		limits.SubjectMonthly, ttls[1].Milliseconds(),
		limits.TenantDaily, ttls[2].Milliseconds(),
		limits.TenantMonthly, ttls[3].Milliseconds()).Int64Slice()
	if err != nil {
		// Fail closed as an infrastructure failure: an unavailable Redis
		// must never disable budget enforcement nor look like a 429 denial.
		return nil, unavailable(err)
	}
	if len(res) < 4 || res[0] == 0 {
		retryAfter := retryAfterDay(now)
		if len(res) >= 3 && res[2] == 2 {
			retryAfter = retryAfterMonth(now)
		}
		scope := ScopeSubject
		if len(res) >= 2 && res[1] == 2 {
			scope = ScopeTenant
		}
		return nil, &Error{Code: CodeBudgetExceeded, RetryAfter: retryAfter, Scope: scope}
	}
	r := &redisBudgetReservation{b: b, id: id, keys: keys, estimate: estimate}
	for i, limit := range []int64{limits.SubjectDaily, limits.SubjectMonthly, limits.TenantDaily, limits.TenantMonthly} {
		r.charged[i] = limit > 0
	}
	return r, nil
}

// Settle adjusts the estimate to the computed cost; nil (unknown cost)
// retains the conservative reservation. Uses a detached context so a client
// disconnect after a billable response cannot prevent exact accounting.
func (r *redisBudgetReservation) Settle(cost *int64) {
	if cost == nil {
		return
	}
	r.b.finalize(r.keys, r.charged, r.markerKey(), *cost-r.estimate)
}

// Release refunds the estimate; repeated calls are no-ops.
func (r *redisBudgetReservation) Release() {
	r.b.finalize(r.keys, r.charged, r.markerKey(), -r.estimate)
}

func (r *redisBudgetReservation) markerKey() string {
	return r.b.Prefix + ":budget:fin:" + r.id
}

func (b *RedisBudget) finalize(keys [4]string, charged [4]bool, marker string, delta int64) {
	args := make([]any, 0, 6)
	args = append(args, delta)
	for _, c := range charged {
		if c {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}
	args = append(args, markerGrace.Milliseconds())
	_ = budgetFinalizeScript.Run(context.Background(), b.Client,
		[]string{keys[0], keys[1], keys[2], keys[3], marker}, args...).Err()
}

// newBudgetReservationID mints a globally unique reservation identifier
// across instances: nanotime plus random bytes.
func newBudgetReservationID(now time.Time) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("budget reservation id: %w", err)
	}
	return fmt.Sprintf("%d-%s", now.UnixNano(), hex.EncodeToString(buf[:])), nil
}
