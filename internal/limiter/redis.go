package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisLimitScript atomically checks the fixed-window rate counter and the
// concurrency gauge for one subject. Keys: KEYS[1] rate window, KEYS[2]
// concurrency. ARGV: limit, concurrency, windowMillis, nowMillis, member.
// Returns {allowed(0/1), retryAfterMillis}.
var redisLimitScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local conc = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local member = ARGV[5]

local windowStart = now - (now % window)
local count = redis.call('HGET', KEYS[1], 'count')
local start = redis.call('HGET', KEYS[1], 'start')
if not count or not start or tonumber(start) ~= windowStart then
  redis.call('DEL', KEYS[1])
  redis.call('HSET', KEYS[1], 'count', 0, 'start', windowStart)
  redis.call('PEXPIRE', KEYS[1], window)
  count = '0'
end
if tonumber(count) >= rate then
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 0 then ttl = window end
  return {0, ttl}
end
local active = tonumber(redis.call('ZCARD', KEYS[2]))
if active >= conc then
  return {0, 0}
end
redis.call('HINCRBY', KEYS[1], 'count', 1)
redis.call('ZADD', KEYS[2], now, member)
redis.call('PEXPIRE', KEYS[2], window * 2)
return {1, 0}
`)

// redisReleaseScript removes one concurrency lease; missing leases are fine
// so cancellation and retries stay safe.
var redisReleaseScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`)

// Redis is the multi-instance limiter backed by Redis. The in-memory limiter
// remains development-only; production deployments must use this one and
// readiness must verify Redis before it is enabled.
type Redis struct {
	Client *redis.Client
	Prefix string // key namespace, e.g. "gw"
	Window time.Duration
	// Per-subject limits; policy layer supplies them per request.
	DefaultRate int
	DefaultConc int
	lookup      func(subject string) (rate, conc int)
}

// NewRedis builds a Redis limiter with a key namespace and default ceilings.
func NewRedis(client *redis.Client, prefix string, ratePerMinute, maxConcurrent int) *Redis {
	return &Redis{
		Client: client, Prefix: prefix, Window: time.Minute,
		DefaultRate: ratePerMinute, DefaultConc: maxConcurrent,
	}
}

// SetLookup installs a per-subject limit resolver (persisted policy). When
// unset the defaults apply.
func (r *Redis) SetLookup(fn func(subject string) (rate, conc int)) { r.lookup = fn }

// Allow implements Gate using an atomic Lua script so rate counting and
// concurrency leasing stay consistent across instances. Redis unavailability
// fails closed and surfaces as limiter.ErrUnavailable.
func (r *Redis) Allow(ctx context.Context, subject string, now time.Time) (bool, time.Duration, func(), error) {
	rate, conc := r.DefaultRate, r.DefaultConc
	if r.lookup != nil {
		rate, conc = r.lookup(subject)
	}
	if rate <= 0 {
		rate = r.DefaultRate
	}
	if conc <= 0 {
		conc = r.DefaultConc
	}
	member := fmt.Sprintf("%d", now.UnixNano())
	keys := []string{
		r.Prefix + ":rate:" + subject,
		r.Prefix + ":conc:" + subject,
	}
	res, err := redisLimitScript.Run(ctx, r.Client, keys,
		rate, conc, r.Window.Milliseconds(), now.UnixMilli(), member).Int64Slice()
	if err != nil {
		// Fail closed, but as an infrastructure failure: an unavailable
		// Redis must not silently disable limits in multi-instance mode, and
		// must not be reported to clients as a rate limit.
		return false, 0, func() {}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if len(res) < 2 || res[0] == 0 {
		return false, time.Duration(res[len(res)-1]) * time.Millisecond, func() {}, nil
	}
	release := func() {
		_ = redisReleaseScript.Run(context.Background(), r.Client,
			[]string{keys[1]}, member).Err()
	}
	return true, 0, release, nil
}
