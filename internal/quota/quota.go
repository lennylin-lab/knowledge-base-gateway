// Package quota enforces persisted daily and monthly token budgets per
// subject. Enforcement is reservation-based: a deterministic bounded estimate
// is charged atomically before the provider is invoked, then settled to the
// upstream-reported total after the response, or released when the request
// fails before a billable response. Unknown usage is never fabricated as
// zero; the conservative reservation stays charged. Periods are UTC calendar
// day and UTC calendar month; counters reset at the boundary without manual
// cleanup. A zero limit means the period is unlimited.
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
)

// CodeQuotaExceeded is the stable denial code carried in the error envelope.
const CodeQuotaExceeded = "quota_exceeded"

// charsPerToken is the documented deterministic input approximation used by
// Estimate: roughly four characters of message content per input token.
// Provider-specific tokenizers are out of scope by design.
const charsPerToken = 4

// DefaultOutputReserve is the conservative output reservation used when
// neither the request declares max_tokens nor the policy configures an
// output ceiling.
const DefaultOutputReserve = 4096

// Limits are the persisted per-subject token budgets for one request. Zero
// means the period is unlimited (NULL in access_policies collapses to zero
// in policy.Limits).
type Limits struct {
	DailyTokens   int64
	MonthlyTokens int64
}

// Configured reports whether any period budget applies. Requests without a
// configured budget skip quota entirely and keep their current behavior.
func (l Limits) Configured() bool {
	return l.DailyTokens > 0 || l.MonthlyTokens > 0
}

// Error reports a quota denial. It never carries policy internals (no
// remaining/limit numbers); RetryAfter is the time until the denied UTC
// period rolls over and the counters reset.
type Error struct {
	Code       string
	RetryAfter time.Duration
}

func (e *Error) Error() string { return "quota: " + e.Code }

// Reservation is one charged estimate for a single request. Settle and
// Release are idempotent and mutually exclusive: the first finalization wins
// and every later call is a no-op. Both are best-effort with no error
// return; a lost settlement degrades to retaining the conservative
// reservation until the period rolls over.
type Reservation interface {
	// Settle adjusts the charged estimate to the reported total token usage.
	// A nil total means usage is unknown; the conservative reservation is
	// retained untouched.
	Settle(total *int64)
	// Release returns the full estimate to the budget. Used when a request
	// fails before provider output or is canceled before a billable response.
	Release()
}

// done is the no-op reservation for requests that skip quota.
type done struct{}

func (done) Settle(*int64) {}
func (done) Release()      {}

// Done is a no-op Reservation for requests without a configured budget.
var Done Reservation = done{}

// Gate is the token-quota boundary. Implementations: an in-memory tracker
// for single-process development and a Redis-backed atomic gate for
// multi-instance deployments, mirroring the limiter.Gate conventions.
type Gate interface {
	// Reserve atomically charges estimate against the subject's configured
	// daily and monthly UTC budgets. On denial it returns a *Error; on
	// infrastructure failure (e.g. Redis unreachable) it returns an error
	// wrapping limiter.ErrUnavailable, which callers must map to the 503
	// limiter_unavailable contract, never to a 429.
	Reserve(ctx context.Context, subject string, limits Limits, estimate int64, now time.Time) (Reservation, error)
}

// InputTokens converts the deterministic character count into the input-token
// estimate shared by quota reservation and admission input ceilings: roughly
// charsPerToken characters per token, rounded up. Provider-specific
// tokenizers are out of scope by design.
func InputTokens(inputChars int) int64 {
	return (int64(inputChars) + charsPerToken - 1) / charsPerToken
}

// Estimate computes the deterministic pre-invocation reservation from the
// declared max_tokens, the policy output ceiling, and the total message
// content size. Precedence: declared max_tokens, then the policy output
// ceiling, then DefaultOutputReserve. Inputs are bounded upstream by request
// validation (max_tokens <= 1,000,000, per-message and message-count
// limits), so the estimate is always bounded and identical for identical
// requests.
func Estimate(maxTokens *int, policyMaxOutput int, inputChars int) int64 {
	output := DefaultOutputReserve
	if maxTokens != nil && *maxTokens > 0 {
		output = *maxTokens
	} else if policyMaxOutput > 0 {
		output = policyMaxOutput
	}
	return InputTokens(inputChars) + int64(output)
}

// DayKey / MonthKey are the per-subject UTC period counters.
func DayKey(prefix, subject string, now time.Time) string {
	return prefix + ":quota:d:" + subject + ":" + now.UTC().Format("20060102")
}

// MonthKey names the monthly counter for one subject and UTC month.
func MonthKey(prefix, subject string, now time.Time) string {
	return prefix + ":quota:m:" + subject + ":" + now.UTC().Format("200601")
}

// nextDayUTC returns the upcoming UTC midnight strictly after now.
func nextDayUTC(now time.Time) time.Time {
	t := now.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// nextMonthUTC returns the first day of the next UTC month strictly after now.
func nextMonthUTC(now time.Time) time.Time {
	t := now.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
}

// dayTTL / monthTTL keep period keys alive past the boundary by keyGrace so
// settlements of requests that straddle midnight still find the charged key.
// Admission always computes the current period from now, so a lingering key
// from a previous period is never read again.
const keyGrace = 48 * time.Hour

func dayTTL(now time.Time) time.Duration {
	return nextDayUTC(now).Sub(now) + keyGrace
}

func monthTTL(now time.Time) time.Duration {
	return nextMonthUTC(now).Sub(now) + keyGrace
}

// retryAfterDay / retryAfterMonth are client-facing Retry-After hints: the
// denied budget resets at the UTC boundary.
func retryAfterDay(now time.Time) time.Duration {
	return nextDayUTC(now).Sub(now)
}

func retryAfterMonth(now time.Time) time.Duration {
	return nextMonthUTC(now).Sub(now)
}

// unavailable wraps an infrastructure failure in the shared limiter sentinel
// so every quota outage maps to the existing 503 limiter_unavailable contract.
func unavailable(err error) error {
	return fmt.Errorf("%w: %v", limiter.ErrUnavailable, err)
}
