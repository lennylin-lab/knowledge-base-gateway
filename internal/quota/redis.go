package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveScript atomically checks both UTC period budgets and charges the
// estimate. KEYS[1] daily counter, KEYS[2] monthly counter. ARGV: estimate,
// dailyLimit, dayTTLMillis, monthlyLimit, monthTTLMillis. A limit <= 0 means
// that period is unlimited and never charged. Returns
// {allowed(0/1), deniedPeriod(1=day,2=month), used} so the caller can compute
// a Retry-After for the period that denied. Check and increment share one
// script, so concurrent gateway instances can never oversubscribe a budget.
var reserveScript = redis.NewScript(`
local estimate = tonumber(ARGV[1])
local limits = {tonumber(ARGV[2]), tonumber(ARGV[4])}
local ttls = {tonumber(ARGV[3]), tonumber(ARGV[5])}
for i = 1, 2 do
  if limits[i] > 0 then
    local used = tonumber(redis.call('GET', KEYS[i]) or '0')
    if used + estimate > limits[i] then
      return {0, i, used}
    end
  end
end
for i = 1, 2 do
  if limits[i] > 0 then
    local used = redis.call('INCRBY', KEYS[i], estimate)
    if used == estimate then
      redis.call('PEXPIRE', KEYS[i], ttls[i])
    end
  end
end
return {1, 0, 0}
`)

// finalizeScript settles or releases exactly one reservation. KEYS[1] daily
// counter, KEYS[2] monthly counter, KEYS[3] finalize marker. ARGV: delta,
// dailyLimit, monthlyLimit, markerTTLMillis. The SET NX marker makes the
// operation exactly-once: settle-after-settle, release-after-release, and
// settle-after-release are all no-ops. Deltas apply only to counters that
// still exist: a period key missing means the budget rolled over mid-flight,
// and the new period must not inherit the adjustment.
var finalizeScript = redis.NewScript(`
if not redis.call('SET', KEYS[3], '1', 'NX') then
  return 0
end
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[4]))
local delta = tonumber(ARGV[1])
if delta == 0 then
  return 1
end
local limits = {tonumber(ARGV[2]), tonumber(ARGV[3])}
for i = 1, 2 do
  if limits[i] > 0 and redis.call('EXISTS', KEYS[i]) == 1 then
    redis.call('INCRBY', KEYS[i], delta)
  end
end
return 1
`)

// markerGrace bounds how long a finalize marker survives; it only needs to
// outlive the request that owns the reservation, with margin for retries.
const markerGrace = keyGrace

// RedisQuota is the multi-instance quota gate backed by Redis. Counters are
// keyed by subject and UTC period and expire at the period boundary plus a
// grace window, so budgets reset without manual cleanup. Redis
// unavailability fails closed and surfaces as limiter.ErrUnavailable.
type RedisQuota struct {
	Client *redis.Client
	Prefix string // key namespace, e.g. "gw"
}

// NewRedis builds a Redis-backed quota gate sharing the limiter's key
// namespace conventions.
func NewRedis(client *redis.Client, prefix string) *RedisQuota {
	return &RedisQuota{Client: client, Prefix: prefix}
}

// redisReservation remembers the charged periods and estimate so settlement
// adjusts exactly what was reserved.
type redisReservation struct {
	q            *RedisQuota
	id           string
	day          string
	month        string
	dayCharged   bool
	monthCharged bool
	estimate     int64
}

// Reserve implements Gate with one atomic script for both periods.
func (q *RedisQuota) Reserve(ctx context.Context, subject string, limits Limits, estimate int64, now time.Time) (Reservation, error) {
	if estimate <= 0 || !limits.Configured() {
		return Done, nil
	}
	// The reservation ID exists before anything is charged: an ID-generation
	// failure therefore cannot leave a charged counter behind.
	id, err := newReservationID(now)
	if err != nil {
		return nil, unavailable(err)
	}
	day := DayKey(q.Prefix, subject, now)
	month := MonthKey(q.Prefix, subject, now)
	res, err := reserveScript.Run(ctx, q.Client, []string{day, month},
		estimate, limits.DailyTokens, dayTTL(now).Milliseconds(),
		limits.MonthlyTokens, monthTTL(now).Milliseconds()).Int64Slice()
	if err != nil {
		// Fail closed as an infrastructure failure: an unavailable Redis
		// must never disable quota enforcement nor look like a 429 denial.
		return nil, unavailable(err)
	}
	if len(res) < 3 || res[0] == 0 {
		var retryAfter time.Duration
		if len(res) >= 2 && res[1] == 2 {
			retryAfter = retryAfterMonth(now)
		} else {
			retryAfter = retryAfterDay(now)
		}
		return nil, &Error{Code: CodeQuotaExceeded, RetryAfter: retryAfter}
	}
	return &redisReservation{
		q: q, id: id, day: day, month: month, estimate: estimate,
		dayCharged: limits.DailyTokens > 0, monthCharged: limits.MonthlyTokens > 0,
	}, nil
}

// Settle adjusts the estimate to the reported total; nil (unknown usage)
// retains the conservative reservation. Uses a detached context so a client
// disconnect after a billable response cannot prevent exact accounting.
func (r *redisReservation) Settle(total *int64) {
	if total == nil {
		return
	}
	r.q.release(context.Background(), r, *total-r.estimate)
}

// Release refunds the estimate; repeated calls are no-ops.
func (r *redisReservation) Release() {
	r.q.release(context.Background(), r, -r.estimate)
}

func (q *RedisQuota) release(ctx context.Context, r *redisReservation, delta int64) {
	_ = finalizeScript.Run(ctx, q.Client,
		[]string{r.day, r.month, q.markerKey(r.id)},
		delta, q.dailyLimit(r), q.monthlyLimit(r), markerGrace.Milliseconds()).Err()
}

func (q *RedisQuota) markerKey(id string) string {
	return q.Prefix + ":quota:fin:" + id
}

// dailyLimit / monthlyLimit echo which periods the reservation charged, so
// finalize never touches unlimited periods.
func (q *RedisQuota) dailyLimit(r *redisReservation) int64 {
	if r.dayCharged {
		return 1
	}
	return 0
}

func (q *RedisQuota) monthlyLimit(r *redisReservation) int64 {
	if r.monthCharged {
		return 1
	}
	return 0
}

// newReservationID mints a globally unique reservation identifier across
// instances: nanotime plus random bytes.
func newReservationID(now time.Time) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("reservation id: %w", err)
	}
	return fmt.Sprintf("%d-%s", now.UnixNano(), hex.EncodeToString(b[:])), nil
}
