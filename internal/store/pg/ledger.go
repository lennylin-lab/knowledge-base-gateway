package pg

// PostgreSQL implementation of the accounting persistence boundary
// (internal/accounting.Store) plus the pricing/budget management mutations
// behind the admin API. The usage_ledger row written at reserve time is the
// durable settlement record; exactly one 'settled' row exists per
// request/job identity — enforced here by conditional updates and in the
// schema by the partial unique index (migration 0007), so a racing or
// repeated settlement is always a detected no-op, never a second record.

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// LedgerStore implements accounting.Store and the pricing/budget admin
// surface on top of the shared connection pool.
type LedgerStore struct {
	DB *DB
}

// EffectivePrice implements accounting.Store: the price version in force for
// the provider/public model at time at — the greatest effective_from not
// after at, ties broken to the highest version. No applicable version is the
// (Price{}, false, nil) outcome, so callers keep the cost unknown instead of
// failing the settlement.
func (s LedgerStore) EffectivePrice(ctx context.Context, provider, publicModel string, at time.Time) (accounting.Price, bool, error) {
	row := s.DB.Pool.QueryRow(ctx, `
		SELECT price_version, currency, input_micros_per_token, output_micros_per_token,
		       reasoning_micros_per_token, cached_input_micros_per_token
		FROM pricing_catalog
		WHERE provider = $1 AND public_model = $2 AND effective_from <= $3
		ORDER BY effective_from DESC, price_version DESC
		LIMIT 1`, provider, publicModel, at)
	var p accounting.Price
	var reasoning, cached *int64
	if err := row.Scan(&p.Version, &p.Currency, &p.InputPerToken, &p.OutputPerToken, &reasoning, &cached); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return accounting.Price{}, false, nil
		}
		return accounting.Price{}, false, err
	}
	p.ReasoningPerToken = reasoning
	p.CachedInputPerToken = cached
	return p, true, nil
}

// BudgetLimits implements accounting.Store: the enabled budget rows for the
// subject and ITS tenant, folded through the shared min-of-declared fold.
// The tenant dimension is resolved from the subjects table (the
// authoritative subject→tenant binding), never from the API key's optional
// tenant column, so a tenant-scoped budget is always evaluated against the
// subject's real tenant.
func (s LedgerStore) BudgetLimits(ctx context.Context, subjectID, _ string) (accounting.BudgetLimits, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT scope, period, currency, amount_micros
		FROM budget_policies
		WHERE enabled AND (
		      (scope = 'subject' AND subject_id = $1)
		   OR (scope = 'tenant'  AND tenant_id  = (SELECT tenant_id FROM subjects WHERE id = $1)))`, subjectID)
	if err != nil {
		return accounting.BudgetLimits{}, err
	}
	defer rows.Close()
	var folded []accounting.BudgetRow
	for rows.Next() {
		var r accounting.BudgetRow
		if err := rows.Scan(&r.Scope, &r.Period, &r.Currency, &r.AmountMicros); err != nil {
			return accounting.BudgetLimits{}, err
		}
		folded = append(folded, r)
	}
	if err := rows.Err(); err != nil {
		return accounting.BudgetLimits{}, err
	}
	return accounting.FoldBudgetRows(folded)
}

// ReserveLedger implements accounting.Store. The row's tenant is resolved
// from the subjects table for the same reason as BudgetLimits: the subject's
// persisted tenant is authoritative (an API key may carry no tenant at all),
// and the composite FK would reject anything else. An unknown subject fails
// the reservation — the authentication layer guarantees existence, so this
// is an invariant violation, surfaced as infrastructure failure upstream.
func (s LedgerStore) ReserveLedger(ctx context.Context, row accounting.LedgerReservation) error {
	tag, err := s.DB.Pool.Exec(ctx, `
		INSERT INTO usage_ledger
			(request_id, job_id, subject_id, tenant_id, protocol, public_model, settle_status, created_at)
		SELECT NULLIF($1,''), NULLIF($2,''), s.id, s.tenant_id, $3, $4, 'reserved', $5
		FROM subjects s WHERE s.id = $6`,
		row.Identity.RequestID, row.Identity.JobID, row.Protocol,
		row.PublicModel, row.CreatedAt, row.SubjectID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.New("pg: ledger reservation for unknown subject " + row.SubjectID)
	}
	return nil
}

// identityArgs normalizes the identity into the column-domain shape: an
// empty identity half is NULL (a sync row carries request_id and a NULL
// job_id; an async row the reverse), so IS NOT DISTINCT FROM comparisons
// match rows exactly.
func identityArgs(id accounting.Identity) (any, any) {
	var requestID, jobID any
	if id.RequestID != "" {
		requestID = id.RequestID
	}
	if id.JobID != "" {
		jobID = id.JobID
	}
	return requestID, jobID
}

// SettleLedger implements accounting.Store. The settlement targets the
// identity's latest 'reserved' row as one conditional update; zero rows
// affected means either a racing settlement already won (the checked
// existence probe distinguishes that idempotent no-op from
// release-then-settle, which is also a no-op) or nothing was reserved. A
// unique-violation race (two settlements of different reserved rows for one
// identity) is the same exactly-once outcome, never a failure.
func (s LedgerStore) SettleLedger(ctx context.Context, q accounting.SettleQuery) (accounting.SettleOutcome, error) {
	requestID, jobID := identityArgs(q.Identity)
	tag, err := s.DB.Pool.Exec(ctx, `
		UPDATE usage_ledger u SET settle_status = 'settled', settled_at = $3,
		       prompt_tokens = $4, completion_tokens = $5, reasoning_tokens = $6,
		       cached_input_tokens = $7, price_version = $8, currency = NULLIF($9,''),
		       cost_micros = $10
		WHERE u.id = (
			SELECT id FROM usage_ledger
			WHERE request_id IS NOT DISTINCT FROM $1
			  AND job_id IS NOT DISTINCT FROM $2
			  AND settle_status = 'reserved'
			ORDER BY id DESC LIMIT 1
		)`,
		requestID, jobID, q.SettledAt,
		q.Usage.PromptTokens, q.Usage.CompletionTokens, q.Usage.ReasoningTokens,
		q.Usage.CachedInputTokens, q.PriceVersion, q.Currency, q.CostMicros)
	if err != nil {
		if isUniqueViolation(err) {
			return accounting.SettleOutcome{Settled: false}, nil
		}
		return accounting.SettleOutcome{}, err
	}
	if tag.RowsAffected() == 1 {
		return accounting.SettleOutcome{Settled: true}, nil
	}
	// Nothing settled by this call: already settled by a racer, released
	// earlier, or never reserved — all the documented no-op outcomes.
	var exists bool
	if err := s.DB.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM usage_ledger
			WHERE request_id IS NOT DISTINCT FROM $1
			  AND job_id IS NOT DISTINCT FROM $2
			  AND settle_status = 'settled'
		)`, requestID, jobID).Scan(&exists); err != nil {
		return accounting.SettleOutcome{}, err
	}
	_ = exists // informational only: either way this call settled nothing
	return accounting.SettleOutcome{Settled: false}, nil
}

// ReleaseLedger implements accounting.Store: the identity's latest reserved
// row becomes 'released'; a missing or already-final row is a no-op.
func (s LedgerStore) ReleaseLedger(ctx context.Context, id accounting.Identity) error {
	requestID, jobID := identityArgs(id)
	_, err := s.DB.Pool.Exec(ctx, `
		UPDATE usage_ledger SET settle_status = 'released'
		WHERE id = (
			SELECT id FROM usage_ledger
			WHERE request_id IS NOT DISTINCT FROM $1
			  AND job_id IS NOT DISTINCT FROM $2
			  AND settle_status = 'reserved'
			ORDER BY id DESC LIMIT 1
		)`, requestID, jobID)
	return err
}

// --- Pricing and budget management (admin API surface) --------------------

// UpsertPrice inserts or updates one price version and its management-audit
// record in one transaction. The previous row is read inside the transaction
// for the redacted old/new audit summary (null when the version is new).
func (s LedgerStore) UpsertPrice(ctx context.Context, in mgmt.PriceInput, op mgmt.AdminOp) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var previous any
	if err := tx.QueryRow(ctx, `
		SELECT json_build_object(
			'currency', currency,
			'input_micros_per_token', input_micros_per_token,
			'output_micros_per_token', output_micros_per_token,
			'reasoning_micros_per_token', reasoning_micros_per_token,
			'cached_input_micros_per_token', cached_input_micros_per_token,
			'effective_from', effective_from)
		FROM pricing_catalog
		WHERE provider = $1 AND public_model = $2 AND price_version = $3`,
		in.Provider, in.PublicModel, in.PriceVersion).Scan(&previous); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		previous = nil // new version: nothing to summarize
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pricing_catalog (provider, public_model, price_version, currency,
			input_micros_per_token, output_micros_per_token,
			reasoning_micros_per_token, cached_input_micros_per_token, effective_from)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (provider, public_model, price_version) DO UPDATE SET
			currency = EXCLUDED.currency,
			input_micros_per_token = EXCLUDED.input_micros_per_token,
			output_micros_per_token = EXCLUDED.output_micros_per_token,
			reasoning_micros_per_token = EXCLUDED.reasoning_micros_per_token,
			cached_input_micros_per_token = EXCLUDED.cached_input_micros_per_token,
			effective_from = EXCLUDED.effective_from`,
		in.Provider, in.PublicModel, in.PriceVersion, in.Currency,
		in.InputMicrosPerToken, in.OutputMicrosPerToken,
		in.ReasoningMicrosPerToken, in.CachedInputMicrosPerToken, in.EffectiveFrom); err != nil {
		return err
	}
	op.Detail = mgmt.MergeDetail(op.Detail, map[string]any{"previous": previous})
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListPrices returns the price catalog ordered for display.
func (s LedgerStore) ListPrices(ctx context.Context) ([]mgmt.PriceView, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT provider, public_model, price_version, currency,
		       input_micros_per_token, output_micros_per_token,
		       reasoning_micros_per_token, cached_input_micros_per_token,
		       effective_from, created_at
		FROM pricing_catalog
		ORDER BY provider, public_model, price_version, effective_from`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.PriceView
	for rows.Next() {
		var v mgmt.PriceView
		if err := rows.Scan(&v.Provider, &v.PublicModel, &v.PriceVersion, &v.Currency,
			&v.InputMicrosPerToken, &v.OutputMicrosPerToken,
			&v.ReasoningMicrosPerToken, &v.CachedInputMicrosPerToken,
			&v.EffectiveFrom, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// UpsertBudget inserts or updates one budget row and its management-audit
// record in one transaction. The conflict target is the schema's expression
// index, so an upsert cannot create the duplicate the unique index forbids.
// The previous row is read inside the transaction for the redacted old/new
// audit summary (null when the budget is new).
func (s LedgerStore) UpsertBudget(ctx context.Context, in mgmt.BudgetInput, op mgmt.AdminOp) error {
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var previous any
	if err := tx.QueryRow(ctx, `
		SELECT json_build_object('amount_micros', amount_micros, 'enabled', enabled, 'currency', currency)
		FROM budget_policies
		WHERE scope = $1 AND COALESCE(subject_id,'') = COALESCE($2,'') AND tenant_id = $3
		  AND period = $4 AND currency = $5`,
		in.Scope, in.SubjectID, in.TenantID, in.Period, in.Currency).Scan(&previous); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		previous = nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO budget_policies (scope, subject_id, tenant_id, period, currency, amount_micros, enabled)
		VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7)
		ON CONFLICT (scope, COALESCE(subject_id, ''), tenant_id, period, currency) DO UPDATE SET
			amount_micros = EXCLUDED.amount_micros,
			enabled = EXCLUDED.enabled,
			updated_at = now()`,
		in.Scope, in.SubjectID, in.TenantID, in.Period, in.Currency, in.AmountMicros, enabled); err != nil {
		return err
	}
	op.Detail = mgmt.MergeDetail(op.Detail, map[string]any{"previous": previous})
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListBudgets returns the budget policies ordered for display. A non-empty
// tenant is a mandatory predicate: tenant-bound callers only ever see their
// tenant's rows (subject rows of the tenant plus the tenant row itself).
func (s LedgerStore) ListBudgets(ctx context.Context, tenant string) ([]mgmt.BudgetView, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, scope, COALESCE(subject_id,''), tenant_id, period, currency,
		       amount_micros, enabled, created_at, updated_at
		FROM budget_policies
		WHERE ($1 = '' OR tenant_id = $1)
		ORDER BY scope, tenant_id, COALESCE(subject_id,''), period, currency`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.BudgetView
	for rows.Next() {
		var v mgmt.BudgetView
		if err := rows.Scan(&v.ID, &v.Scope, &v.SubjectID, &v.TenantID, &v.Period,
			&v.Currency, &v.AmountMicros, &v.Enabled, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// BudgetUsage reports current-period utilization for every budget policy
// (bounded to the tenant when one is given): the sum of known settled cost
// and the count of unknown-cost settlements in the policy's current UTC
// period. Windows are computed in Go on UTC boundaries so the query never
// depends on the database session timezone.
func (s LedgerStore) BudgetUsage(ctx context.Context, now time.Time, tenant string) ([]mgmt.BudgetUsageView, error) {
	policies, err := s.ListBudgets(ctx, tenant)
	if err != nil {
		return nil, err
	}
	dayStart := now.UTC().Truncate(24 * time.Hour)
	monthStart := time.Date(now.UTC().Year(), now.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0)

	subjectUsed, err := s.scopeUsage(ctx, "subject_id", dayStart, dayStart.Add(24*time.Hour), monthStart, monthEnd)
	if err != nil {
		return nil, err
	}
	tenantUsed, err := s.scopeUsage(ctx, "tenant_id", dayStart, dayStart.Add(24*time.Hour), monthStart, monthEnd)
	if err != nil {
		return nil, err
	}

	out := make([]mgmt.BudgetUsageView, 0, len(policies))
	for _, p := range policies {
		v := mgmt.BudgetUsageView{
			Scope: p.Scope, SubjectID: p.SubjectID, TenantID: p.TenantID,
			Period: p.Period, Currency: p.Currency, LimitMicros: p.AmountMicros,
		}
		used, ok := tenantUsed[p.TenantID]
		if p.Scope == "subject" {
			used, ok = subjectUsed[p.SubjectID]
		}
		if ok {
			pair := used[0] // daily
			if p.Period == "monthly" {
				pair = used[1]
			}
			v.UsedMicros, v.UnknownCostSettlements = pair[0], pair[1]
		}
		out = append(out, v)
	}
	return out, nil
}

// scopeUsage aggregates settled ledger cost per scope target for the current
// UTC day and month: per key, the known-cost sum and the unknown-cost
// settlement count for each period.
func (s LedgerStore) scopeUsage(ctx context.Context, column string, dayStart, dayEnd, monthStart, monthEnd time.Time) (map[string][2][2]int64, error) {
	// The column name comes only from the two validated call sites (the
	// ledger columns subject_id/tenant_id both exist), never from input.
	if column != "subject_id" && column != "tenant_id" {
		return nil, errors.New("pg: invalid scope column")
	}
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT `+column+`,
		       COALESCE(sum(cost_micros) FILTER (WHERE settled_at >= $1 AND settled_at < $2), 0),
		       count(*) FILTER (WHERE cost_micros IS NULL AND settled_at >= $1 AND settled_at < $2),
		       COALESCE(sum(cost_micros) FILTER (WHERE settled_at >= $3 AND settled_at < $4), 0),
		       count(*) FILTER (WHERE cost_micros IS NULL AND settled_at >= $3 AND settled_at < $4)
		FROM usage_ledger
		WHERE settle_status = 'settled'
		GROUP BY `+column, dayStart, dayEnd, monthStart, monthEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][2][2]int64{} // key -> [daily, monthly][usedSum, unknownCount]
	for rows.Next() {
		var key string
		var daily, monthly [2]int64
		if err := rows.Scan(&key, &daily[0], &daily[1], &monthly[0], &monthly[1]); err != nil {
			return nil, err
		}
		out[key] = [2][2]int64{daily, monthly}
	}
	return out, rows.Err()
}
