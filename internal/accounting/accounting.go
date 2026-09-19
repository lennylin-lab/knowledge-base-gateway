// Package accounting implements V1.4 cost governance: versioned model
// pricing, the settlement ledger shared by synchronous and asynchronous
// requests, and subject/tenant monetary budgets.
//
// Money is integer micro units (micros) end to end; floating-point amounts
// never exist. Costs are computed only when every reported token class has a
// price: unknown usage or an unknown price leaves the cost NULL (never
// fabricated as zero, never as a priceless number). Enforcement is
// reservation-based and mirrors the token-quota pattern (internal/quota): a
// deterministic bounded estimate is charged atomically against the subject's
// and the tenant's daily/monthly UTC counters before the provider is
// invoked, then settled to the computed cost or released on outcomes that
// produced no billable response. The PostgreSQL usage_ledger row written at
// reserve time is the durable settlement record: exactly one final
// (settled) row exists per request/job identity, a failed settlement leaves
// the row 'reserved' as retryable evidence, and disabling enforcement
// (Gate.Enforcement) never disables ledger capture.
package accounting

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// CodeBudgetExceeded is the stable denial code carried in the error envelope
// (roadmap: budget_exceeded, HTTP 429).
const CodeBudgetExceeded = "budget_exceeded"

// Errors surfaced across the accounting boundary.
var (
	// ErrPricingUnavailable: a configured monetary budget applies but no
	// effective price (or a price in the budget's currency) exists for the
	// model. The request fails before any provider work — a priceless
	// request must never bypass the budget.
	ErrPricingUnavailable = errors.New("accounting: pricing unavailable")
	// ErrBudgetConfig: the applicable budget policy rows disagree on a
	// currency (one budget currency is allowed per applicable policy). This
	// is a configuration error, not a denial and not an outage.
	ErrBudgetConfig = errors.New("accounting: budget policy currencies conflict")
)

// Error reports a budget denial. It never carries policy internals (no
// remaining/limit numbers); RetryAfter is the time until the denied UTC
// period rolls over, and Scope names the dimension that denied for metrics.
type Error struct {
	Code       string
	RetryAfter time.Duration
	Scope      string // "subject" or "tenant"
}

func (e *Error) Error() string { return "accounting: " + e.Code }

// budgetScopes are the metric label values for Error.Scope.
const (
	ScopeSubject = "subject"
	ScopeTenant  = "tenant"
)

// Usage carries optional token counts per priceable class. A nil count stays
// unknown and is never fabricated as zero; a class is charged only when the
// provider reported it.
type Usage struct {
	PromptTokens      *int64
	CompletionTokens  *int64
	ReasoningTokens   *int64
	CachedInputTokens *int64
}

// Reported reports whether any token class was reported by the upstream.
func (u Usage) Reported() bool {
	return u.PromptTokens != nil || u.CompletionTokens != nil ||
		u.ReasoningTokens != nil || u.CachedInputTokens != nil
}

// UsageFrom converts the domain usage into the NULL-preserving accounting
// shape; unknown usage (Known=false) converts to an empty Usage so nothing
// is fabricated.
func UsageFrom(m *model.Usage) Usage {
	if m == nil || !m.Known {
		return Usage{}
	}
	u := Usage{}
	if m.PromptTokens >= 0 {
		p := int64(m.PromptTokens)
		u.PromptTokens = &p
	}
	if m.CompletionTokens >= 0 {
		c := int64(m.CompletionTokens)
		u.CompletionTokens = &c
	}
	if m.ReasoningTokens != nil {
		r := int64(*m.ReasoningTokens)
		u.ReasoningTokens = &r
	}
	if m.CachedInputTokens != nil {
		ci := int64(*m.CachedInputTokens)
		u.CachedInputTokens = &ci
	}
	return u
}

// Price is one effective price version: micros per token for each class.
// A zero price is a valid, explicitly free price; a nil price means the
// class is unpriced (costs that need it stay unknown).
type Price struct {
	Version             int
	Currency            string
	InputPerToken       int64 // micros per input token
	OutputPerToken      int64 // micros per output token
	ReasoningPerToken   *int64
	CachedInputPerToken *int64
}

// Cost computes the total cost in micros for the reported usage. ok is false
// — and the caller must keep the cost unknown — when a reported class has no
// price, when the usage is internally inconsistent (cached tokens exceed
// prompt tokens), or when the integer arithmetic would overflow. Charged
// classes: (prompt - cached) at the input price, cached at the cached-input
// price when reported, completion at the output price, reasoning at the
// reasoning price when reported.
func (p Price) Cost(u Usage) (int64, bool) {
	if u.ReasoningTokens != nil && p.ReasoningPerToken == nil {
		return 0, false
	}
	if u.CachedInputTokens != nil && p.CachedInputPerToken == nil {
		return 0, false
	}
	prompt := orZero(u.PromptTokens)
	cached := int64(0)
	if u.CachedInputTokens != nil {
		if *u.CachedInputTokens < 0 || *u.CachedInputTokens > prompt {
			return 0, false
		}
		cached = *u.CachedInputTokens
	}
	var total int64
	var ok bool
	if total, ok = mulAdd(total, p.InputPerToken, prompt-cached); !ok {
		return 0, false
	}
	if total, ok = mulAdd(total, p.OutputPerToken, orZero(u.CompletionTokens)); !ok {
		return 0, false
	}
	if u.ReasoningTokens != nil {
		if total, ok = mulAdd(total, *p.ReasoningPerToken, *u.ReasoningTokens); !ok {
			return 0, false
		}
	}
	if u.CachedInputTokens != nil {
		if total, ok = mulAdd(total, *p.CachedInputPerToken, cached); !ok {
			return 0, false
		}
	}
	return total, true
}

func orZero(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// mulAdd accumulates price*tokens into total with checked overflow. Tokens
// and prices are non-negative, so only positive overflow is possible.
func mulAdd(total, price, tokens int64) (int64, bool) {
	if tokens == 0 || price == 0 {
		return total, true
	}
	if tokens > math.MaxInt64/price {
		return 0, false
	}
	sum := total + price*tokens
	if sum < total { // positive overflow of the accumulation
		return 0, false
	}
	return sum, true
}

// BudgetLimits are the folded monetary ceilings for one request: subject-
// and tenant-scoped daily/monthly limits in one currency (micros). Zero
// means the scope/period is uncapped.
type BudgetLimits struct {
	Currency       string
	SubjectDaily   int64
	SubjectMonthly int64
	TenantDaily    int64
	TenantMonthly  int64
}

// Configured reports whether any monetary budget applies. Requests without
// a configured budget skip enforcement entirely and keep ledger capture.
func (l BudgetLimits) Configured() bool {
	return l.SubjectDaily > 0 || l.SubjectMonthly > 0 || l.TenantDaily > 0 || l.TenantMonthly > 0
}

// BudgetRow is one persisted budget_policies row as the fold input. Rows for
// one target (subject or tenant) fold to the minimum declared amount per
// period (min-of-declared, the V1.3 policy convention) — a defensive fold:
// the schema's unique target index already keeps one row per
// target/period/currency, so the fold only ever matters if that invariant is
// ever relaxed.
type BudgetRow struct {
	Scope        string // "subject" or "tenant"
	Period       string // "daily" or "monthly"
	Currency     string
	AmountMicros int64
}

// FoldBudgetRows folds enabled budget rows into the effective limits for one
// request. All applicable rows must share one currency (ErrBudgetConfig
// otherwise — mixing currencies would silently compare incompatible units);
// rows of other subjects/tenants must be filtered out by the source.
func FoldBudgetRows(rows []BudgetRow) (BudgetLimits, error) {
	var out BudgetLimits
	for _, r := range rows {
		switch {
		case out.Currency == "":
			out.Currency = r.Currency
		case out.Currency != r.Currency:
			return BudgetLimits{}, ErrBudgetConfig
		}
		if r.AmountMicros <= 0 {
			continue // a non-positive amount constrains nothing (and the schema rejects it)
		}
		out.minInto(r)
	}
	return out, nil
}

func (l *BudgetLimits) minInto(r BudgetRow) {
	minIn := func(a, b int64) int64 {
		if b <= 0 || (a > 0 && b >= a) {
			return a
		}
		return b
	}
	switch {
	case r.Scope == ScopeSubject && r.Period == "daily":
		l.SubjectDaily = minIn(l.SubjectDaily, r.AmountMicros)
	case r.Scope == ScopeSubject && r.Period == "monthly":
		l.SubjectMonthly = minIn(l.SubjectMonthly, r.AmountMicros)
	case r.Scope == ScopeTenant && r.Period == "daily":
		l.TenantDaily = minIn(l.TenantDaily, r.AmountMicros)
	case r.Scope == ScopeTenant && r.Period == "monthly":
		l.TenantMonthly = minIn(l.TenantMonthly, r.AmountMicros)
	}
}

// Identity names the settlement identity of one request: exactly one of the
// two fields is set (sync requests use RequestID, background jobs JobID).
type Identity struct {
	RequestID string
	JobID     string
}

// LedgerReservation is the durable 'reserved' row written before any
// provider work: the finalize record that makes settlement retryable.
type LedgerReservation struct {
	Identity    Identity
	SubjectID   string
	TenantID    string
	Protocol    string
	PublicModel string
	CreatedAt   time.Time
}

// SettleQuery finalizes the identity's reserved ledger row. CostMicros stays
// nil for unknown cost; PriceVersion/Currency name the price actually used
// when one applied (the schema's CHECK requires them whenever a cost is
// present).
type SettleQuery struct {
	Identity     Identity
	Usage        Usage
	CostMicros   *int64
	PriceVersion *int
	Currency     string
	SettledAt    time.Time
}

// Store is the accounting persistence boundary: the price catalog, the
// budget policy source, and the settlement ledger. PostgreSQL is the
// authoritative implementation (internal/store/pg); tests stub it.
type Store interface {
	// EffectivePrice returns the price version in force for the
	// provider/public model at time at (greatest effective_from <= at; ties
	// break to the highest version). ok is false when no version applies —
	// never an error, so callers can keep the cost unknown.
	EffectivePrice(ctx context.Context, provider, publicModel string, at time.Time) (Price, bool, error)
	// BudgetLimits loads and folds the enabled budget rows applicable to the
	// subject and its tenant. Mixed currencies return ErrBudgetConfig.
	BudgetLimits(ctx context.Context, subjectID, tenantID string) (BudgetLimits, error)
	// ReserveLedger inserts the durable 'reserved' row.
	ReserveLedger(ctx context.Context, row LedgerReservation) error
	// SettleLedger transitions the identity's latest reserved row to settled
	// exactly once. A settled row already existing (or a racing settlement
	// winning) is the no-op outcome Settled=false with a nil error — never a
	// failure.
	SettleLedger(ctx context.Context, q SettleQuery) (SettleOutcome, error)
	// ReleaseLedger transitions the identity's latest reserved row to
	// released; a missing or already-final row is a no-op.
	ReleaseLedger(ctx context.Context, id Identity) error
}

// SettleOutcome reports whether this call performed the settlement.
type SettleOutcome struct {
	Settled bool
}

// BudgetReservation is one charged money estimate for a single request.
// Settle and Release are idempotent and mutually exclusive; the first wins.
type BudgetReservation interface {
	// Settle adjusts the charged estimate to the computed cost. A nil cost
	// (unknown) retains the conservative reservation untouched.
	Settle(cost *int64)
	// Release returns the full estimate. Used when the request produced no
	// billable outcome.
	Release()
}

// BudgetGate is the money-counter engine (Redis for multi-instance atomicity,
// in-memory for development and tests), mirroring the quota.Gate conventions.
type BudgetGate interface {
	// Reserve atomically checks and charges estimate against the subject's
	// and the tenant's daily and monthly UTC counters in one operation. A
	// denial returns *Error; an infrastructure failure returns an error
	// wrapping limiter.ErrUnavailable (503, never a 429).
	Reserve(ctx context.Context, subjectID, tenantID string, limits BudgetLimits, estimate int64, now time.Time) (BudgetReservation, error)
}

// Reservation is the accounting handle for one admitted request. Settle and
// Release are idempotent and mutually exclusive; the first finalization wins
// and later calls are no-ops.
type Reservation interface {
	// Settle finalizes to the reported usage exactly once: it writes the one
	// settled ledger row for the identity (cost NULL when it cannot be
	// computed exactly) and adjusts the budget counters to the computed cost.
	// provider is the provider that actually served the request (pricing is
	// resolved for it at the reservation's as-of time). A returned error
	// means the settlement did NOT land: the ledger row stays 'reserved' as
	// retryable evidence and the conservative counter reservation is kept —
	// never silently marked settled.
	Settle(provider string, u Usage) error
	// Release refunds the estimate and marks the ledger row released.
	// Best-effort: a failed ledger release leaves the 'reserved' row as
	// evidence; the returned error is for observability only.
	Release() error
}

// done is the no-op reservation for requests without accounting.
type noopReservation struct{}

func (noopReservation) Settle(string, Usage) error { return nil }
func (noopReservation) Release() error             { return nil }

// Done is a no-op Reservation used when accounting is not wired.
var Done Reservation = noopReservation{}

// ReserveInput carries the identity and estimate basis for one reservation.
// Estimate holds the deterministic pre-invocation token estimate (the same
// numbers the token quota reserves) as prompt/completion classes.
type ReserveInput struct {
	Identity    Identity
	SubjectID   string
	TenantID    string
	Protocol    string
	PublicModel string
	Provider    string // primary route candidate: the estimate's pricing basis
	Estimate    Usage
	Now         time.Time
}

// Gate orchestrates budget enforcement and ledger settlement. A nil Gate or
// a nil Store disables accounting entirely (Done reservations). With
// Enforcement false (the rollback posture) no budget check runs and no
// request is ever denied, but ledger rows are still written and settled so
// operators retain cost evidence.
type Gate struct {
	Store       Store
	Budgets     BudgetGate // required whenever Enforcement is on and a budget applies
	Enforcement bool
	Now         func() time.Time
	// Metrics counts settlements whose cost stayed unknown (unknown usage or
	// no applicable price) — the unknown-cost-rate signal. Optional; nil
	// disables the counter.
	Metrics *metrics.Registry

	// finalizeTimeout bounds the detached-context settlement operations:
	// settlement must survive a client disconnect but must not hang forever.
	finalizeTimeout time.Duration
}

// finalizeTimeout is the bounded lifetime of settle/release store work.
const finalizeTimeout = 5 * time.Second

func (g *Gate) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

// detachedCtx builds the bounded context for post-response settlement work:
// deliberately detached from any request so a client disconnect cannot lose
// accounting (the same rule the token quota follows). Callers must cancel.
func (g *Gate) detachedCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), g.finalizeTimeoutOr())
}

func (g *Gate) finalizeTimeoutOr() time.Duration {
	if g.finalizeTimeout > 0 {
		return g.finalizeTimeout
	}
	return finalizeTimeout
}

// unavailable wraps an infrastructure failure in the shared limiter sentinel
// so every accounting outage maps to the existing 503 limiter_unavailable
// contract, never to a 429.
func unavailable(err error) error {
	return fmt.Errorf("%w: %v", limiter.ErrUnavailable, err)
}

// --- UTC period helpers (shared by both budget gates) ---------------------

// keyGrace keeps period counters alive past the boundary so settlements of
// requests that straddle midnight still find the charged key. Admission
// always computes the current period from now, so a lingering key from a
// previous period is never read again.
const keyGrace = 48 * time.Hour

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

// Reserve runs the pre-invocation accounting pipeline:
//
//  1. load the folded budget limits (subject + tenant);
//  2. when enforcing and a budget applies, resolve the effective price and
//     charge the deterministic estimate against both dimensions atomically —
//     a configured budget with no applicable price is ErrPricingUnavailable,
//     so the budget can never be bypassed;
//  3. write the durable 'reserved' ledger row (capture continues even when
//     enforcement is disabled);
//  4. return the composite Reservation that finalizes both.
func (g *Gate) Reserve(ctx context.Context, in ReserveInput) (Reservation, error) {
	if g == nil || g.Store == nil {
		return Done, nil
	}
	now := in.Now
	if now.IsZero() {
		now = g.now()
	}
	limits, err := g.Store.BudgetLimits(ctx, in.SubjectID, in.TenantID)
	if err != nil {
		return nil, unavailable(err)
	}

	var charged BudgetReservation
	estimateMicros := int64(0)
	if g.Enforcement && limits.Configured() {
		if g.Budgets == nil {
			// Enforcement without a counter engine is a wiring error: fail
			// closed rather than serve unenforced under a configured budget.
			return nil, unavailable(errors.New("accounting: enforcement enabled but no budget gate wired"))
		}
		price, ok, err := g.Store.EffectivePrice(ctx, in.Provider, in.PublicModel, now)
		if err != nil {
			return nil, unavailable(err)
		}
		if !ok || price.Currency != limits.Currency {
			// No applicable price, or a price in a currency the budget does
			// not denominate: the budget cannot be evaluated, so the request
			// fails before any provider work.
			return nil, ErrPricingUnavailable
		}
		var cok bool
		estimateMicros, cok = price.Cost(in.Estimate)
		if !cok || estimateMicros <= 0 {
			// An uncomputable or zero estimate cannot be enforced against;
			// refuse rather than bypass. (Estimates are bounded, so this is
			// defensive.)
			return nil, ErrPricingUnavailable
		}
		charged, err = g.Budgets.Reserve(ctx, in.SubjectID, in.TenantID, limits, estimateMicros, now)
		if err != nil {
			return nil, err // *Error denial or limiter.ErrUnavailable outage
		}
	}

	if err := g.Store.ReserveLedger(ctx, LedgerReservation{
		Identity: in.Identity, SubjectID: in.SubjectID, TenantID: in.TenantID,
		Protocol: in.Protocol, PublicModel: in.PublicModel, CreatedAt: now,
	}); err != nil {
		if charged != nil {
			charged.Release() // refund the counters: nothing was admitted
		}
		return nil, unavailable(err)
	}
	return &reservation{
		g: g, in: in, asOf: now, charged: charged, estimateMicros: estimateMicros,
	}, nil
}

// reservation is the composite handle: budget counters (when enforcement
// charged them) plus the durable ledger row.
type reservation struct {
	g              *Gate
	in             ReserveInput
	asOf           time.Time
	charged        BudgetReservation
	estimateMicros int64

	mu       sync.Mutex
	settled  bool
	released bool
}

// Settle implements Reservation. The local settled flag commits only after
// the ledger settlement landed, so a failed settlement can be retried by the
// caller without being silently swallowed; idempotency across processes is
// enforced by the store's exactly-once settlement, not by this flag.
func (r *reservation) Settle(provider string, u Usage) error {
	r.mu.Lock()
	if r.settled || r.released {
		r.mu.Unlock()
		return nil // repeated finalization is a no-op; settle-after-release too
	}
	r.mu.Unlock()

	// Compute the cost from the price effective at request time for the
	// provider that actually served. Unknown usage needs no price; a known
	// usage without one keeps the cost NULL (never fabricated).
	var (
		cost     *int64
		version  *int
		currency string
	)
	if u.Reported() {
		ctx, cancel := r.g.detachedCtx()
		price, ok, err := r.g.Store.EffectivePrice(ctx, provider, r.in.PublicModel, r.asOf)
		cancel()
		if err != nil {
			return unavailable(err)
		}
		if ok {
			v := price.Version
			version, currency = &v, price.Currency
			if m, cok := price.Cost(u); cok {
				cost = &m
			}
		}
	}

	ctx, cancel := r.g.detachedCtx()
	defer cancel()
	if _, err := r.g.Store.SettleLedger(ctx, SettleQuery{
		Identity: r.in.Identity, Usage: u, CostMicros: cost,
		PriceVersion: version, Currency: currency, SettledAt: r.g.now(),
	}); err != nil {
		return unavailable(err)
	}
	if cost == nil && r.g.Metrics != nil {
		// The settled row keeps cost NULL: either the upstream reported no
		// usage or no price applied. Unknown stays unknown — never zero —
		// and is counted so the unknown-cost rate is observable.
		r.g.Metrics.IncCostUnknown()
	}
	// The settlement landed (or was already settled by a racer — the same
	// exactly-once outcome). Adjust the counters exactly once.
	r.mu.Lock()
	r.settled = true
	r.mu.Unlock()
	if r.charged != nil {
		// Unknown cost retains the conservative reservation; known cost
		// adjusts by the exact delta.
		r.charged.Settle(cost)
	}
	return nil
}

// Release implements Reservation. Like Settle, the released flag commits
// only after the ledger release landed, so a failed release stays retryable.
func (r *reservation) Release() error {
	r.mu.Lock()
	if r.released || r.settled {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	if r.charged != nil {
		r.charged.Release()
	}
	ctx, cancel := r.g.detachedCtx()
	defer cancel()
	if err := r.g.Store.ReleaseLedger(ctx, r.in.Identity); err != nil {
		return unavailable(err)
	}
	r.mu.Lock()
	r.released = true
	r.mu.Unlock()
	return nil
}
