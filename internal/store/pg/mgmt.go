package pg

// Management query services backing the admin API. Every query returns
// metadata only: no API key hashes, no provider secrets or base URLs, and no
// prompt/completion content (the schema never stores it). The DB satisfies
// mgmt.Service.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

// Models lists every catalog row including disabled ones.
func (d *DB) Models(ctx context.Context) ([]mgmt.ModelView, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT public_name, provider, upstream_model, enabled, config_version,
		       COALESCE(capabilities, '{}'::jsonb)
		FROM model_catalog ORDER BY public_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.ModelView
	for rows.Next() {
		var v mgmt.ModelView
		var caps []byte
		if err := rows.Scan(&v.PublicName, &v.Provider, &v.UpstreamModel, &v.Enabled,
			&v.ConfigVersion, &caps); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(caps, &v.Capabilities); err != nil {
			return nil, fmt.Errorf("model %s: parse capabilities: %w", v.PublicName, err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetModelEnabledWithAudit persists a management enable/disable decision and
// its management-operation audit record in one transaction: either both land
// or neither does. The previous enabled state is read inside the transaction
// and merged into the audit summary (redacted old/new evidence). Unknown
// models return mgmt.ErrNotFound with nothing written, so a failed operation
// can never be reported as audited-and-done, and a committed mutation can
// never be reported as failed.
func (d *DB) SetModelEnabledWithAudit(ctx context.Context, publicModel string, enabled bool, op mgmt.AdminOp) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	var previous bool
	if err := tx.QueryRow(ctx,
		`SELECT enabled FROM model_catalog WHERE public_name = $1`, publicModel).Scan(&previous); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return mgmt.ErrNotFound
		}
		return err
	}
	ct, err := tx.Exec(ctx,
		`UPDATE model_catalog SET enabled = $2, config_version = config_version + 1 WHERE public_name = $1`,
		publicModel, enabled)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return mgmt.ErrNotFound
	}
	op.Detail = mgmt.MergeDetail(op.Detail, map[string]any{
		"previous_enabled": previous, "enabled": enabled,
	})
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ModelEntry reads one catalog row regardless of enabled state. The runtime
// refresh path uses it to swap fresh rows (capabilities, configuration
// version, and retrieval profile included) into the live catalog; found is
// false for unknown models.
func (d *DB) ModelEntry(ctx context.Context, publicModel string) (policy.ModelInfo, bool, error) {
	row := d.Pool.QueryRow(ctx, `
		SELECT public_name, provider, upstream_model, enabled,
		       COALESCE(capabilities, '{}'::jsonb), config_version, retrieval_profile
		FROM model_catalog WHERE public_name = $1`, publicModel)
	var m policy.ModelInfo
	var caps []byte
	var profile []byte
	if err := row.Scan(&m.PublicName, &m.Provider, &m.UpstreamModel, &m.Enabled, &caps, &m.ConfigVersion, &profile); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return policy.ModelInfo{}, false, nil
		}
		return policy.ModelInfo{}, false, err
	}
	if err := json.Unmarshal(caps, &m.Capabilities); err != nil {
		return policy.ModelInfo{}, false, fmt.Errorf("model %s: parse capabilities: %w", m.PublicName, err)
	}
	if len(profile) > 0 && string(profile) != "null" {
		if !json.Valid(profile) {
			return policy.ModelInfo{}, false, fmt.Errorf("model %s: retrieval_profile is not valid JSON", m.PublicName)
		}
		m.RetrievalProfile = json.RawMessage(profile)
	}
	return m, true, nil
}

// Providers lists the provider registry status without endpoints, enriched
// with a recent (24h) error summary from the audit table. Live breaker state
// is runtime data invisible to the store; the process wiring overlays it
// through mgmt.ProviderView.ApplyRuntime, and until then the fields read
// "unknown".
func (d *DB) Providers(ctx context.Context) ([]mgmt.ProviderView, error) {
	rows, err := d.Pool.Query(ctx, `SELECT name, kind, enabled FROM providers ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.ProviderView
	for rows.Next() {
		var p mgmt.ProviderView
		if err := rows.Scan(&p.Name, &p.Kind, &p.Enabled); err != nil {
			return nil, err
		}
		if p.Enabled {
			p.Health = mgmt.HealthUnknown
			p.BreakerState = mgmt.BreakerUnknown
		} else {
			p.Health = mgmt.HealthDisabled
			p.BreakerState = mgmt.BreakerNone
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	errRows, err := d.Pool.Query(ctx, `
		SELECT provider, count(*), COALESCE((array_agg(error_class ORDER BY created_at DESC))[1], '')
		FROM llm_requests
		WHERE created_at >= now() - interval '24 hours'
		  AND (status >= 400 OR COALESCE(error_class,'') <> '')
		GROUP BY provider`)
	if err != nil {
		return nil, err
	}
	defer errRows.Close()
	byProvider := map[string]mgmt.ProviderView{}
	for errRows.Next() {
		var name string
		var s mgmt.ProviderView
		if err := errRows.Scan(&name, &s.RecentErrors, &s.LastErrorClass); err != nil {
			return nil, err
		}
		byProvider[name] = s
	}
	if err := errRows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if s, ok := byProvider[out[i].Name]; ok {
			out[i].RecentErrors = s.RecentErrors
			out[i].LastErrorClass = s.LastErrorClass
		}
	}
	return out, nil
}

// Policies lists access_policies rows for one subject (or all when empty),
// including the subject's default-model slots. Every row also carries the
// subject's folded effective ceilings (issue #8 mitigation: min-of-declared
// folding must be visible to operators, not silent). The effective block is
// computed through LoadLimits so the view and the enforcement path fold the
// rows through the exact same code. A non-empty tenant is a mandatory query
// predicate resolved through subjects.tenant_id — the authoritative
// subject→tenant binding (the api_keys tenant column is a principal field,
// never the boundary).
func (d *DB) Policies(ctx context.Context, subject, tenant string) ([]mgmt.PolicyView, error) {
	limits, err := d.LoadLimits(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := d.Pool.Query(ctx, `
		SELECT p.subject_id, p.public_model, p.rate_per_minute, p.max_concurrent,
		       COALESCE(p.daily_tokens, 0), COALESCE(p.monthly_tokens, 0),
		       COALESCE(p.default_model, ''), COALESCE(p.default_embedding_model, '')
		FROM access_policies p
		WHERE ($1 = '' OR p.subject_id = $1)
		  AND ($2 = '' OR p.subject_id IN (SELECT id FROM subjects WHERE tenant_id = $2))
		ORDER BY p.subject_id, p.public_model
		LIMIT 500`, subject, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.PolicyView
	for rows.Next() {
		var v mgmt.PolicyView
		if err := rows.Scan(&v.Subject, &v.PublicModel, &v.RatePerMinute, &v.MaxConcurrent,
			&v.DailyTokens, &v.MonthlyTokens, &v.DefaultModel, &v.DefaultEmbeddingModel); err != nil {
			return nil, err
		}
		v.EffectiveLimits = mgmt.EffectiveLimitsFrom(limits[v.Subject])
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetDefaultModelWithAudit persists a default-model decision and its
// management-operation audit record in one transaction: either both land or
// neither does. The slots live on access_policies rows, so every row of the
// subject is updated together and the subject's slots stay uniform. The
// previous slot value is read inside the transaction for the redacted
// old/new summary. A non-empty tenant is an ownership check resolved through
// subjects.tenant_id: a subject outside the caller's boundary answers
// mgmt.ErrNotFound (non-leaky, indistinguishable from absence). Unknown
// subjects (no access_policies rows) and unknown models also return
// mgmt.ErrNotFound with nothing written. kind selects the slot: "chat" sets
// default_model, "embedding" sets default_embedding_model.
func (d *DB) SetDefaultModelWithAudit(ctx context.Context, subject, model, kind string, op mgmt.AdminOp, tenant string) error {
	column := map[string]string{"chat": "default_model", "embedding": "default_embedding_model"}[kind]
	if column == "" {
		return fmt.Errorf("unknown default-model kind %q", kind)
	}
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	// Tenant boundary: the subject must exist inside the caller's tenant
	// (mandatory predicate, not a post-query filter).
	var tenantOK bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM subjects WHERE id = $1 AND ($2 = '' OR tenant_id = $2)
		)`, subject, tenant).Scan(&tenantOK); err != nil {
		return err
	}
	if !tenantOK {
		return mgmt.ErrNotFound
	}
	// The target model must exist in the catalog (the FK would also reject
	// the write, but an explicit check maps to a 404-class answer).
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM model_catalog WHERE public_name = $1)`, model).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return mgmt.ErrNotFound
	}
	var previous string
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(`+column+`, '') FROM access_policies WHERE subject_id = $1 LIMIT 1`, subject).Scan(&previous); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// No policy rows: the UPDATE below decides the ErrNotFound outcome.
		previous = ""
	}
	// Parameterized identifiers are not possible for the column name, so the
	// two known kinds branch on a validated, closed set.
	query := fmt.Sprintf(`UPDATE access_policies SET %s = $2 WHERE subject_id = $1`, column)
	ct, err := tx.Exec(ctx, query, subject, model)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return mgmt.ErrNotFound
	}
	op.Detail = mgmt.MergeDetail(op.Detail, map[string]any{
		"previous_model": previous, "model": model, "kind": kind,
	})
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// QueryAudit returns audit records matching the filter, newest first. A
// non-empty filter tenant is a mandatory predicate through subjects.tenant_id.
func (d *DB) QueryAudit(ctx context.Context, f mgmt.AuditFilter) ([]audit.Event, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := d.Pool.Query(ctx, `
		SELECT request_id, subject_id, key_id, model, provider, status,
		       COALESCE(error_class,''), latency_ms, prompt_tokens, completion_tokens,
		       first_token_millis, streaming, created_at, trace_id, route_attempts,
		       COALESCE(protocol,'chat')
		FROM llm_requests
		WHERE ($1 = '' OR request_id = $1)
		  AND ($2 = '' OR subject_id = $2)
		  AND ($3 = '' OR model = $3)
		  AND ($4::timestamptz IS NULL OR created_at >= $4)
		  AND ($5::timestamptz IS NULL OR created_at <= $5)
		  AND ($6 = '' OR subject_id IN (SELECT id FROM subjects WHERE tenant_id = $6))
		ORDER BY created_at DESC
		LIMIT $7`,
		f.RequestID, f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []audit.Event
	for rows.Next() {
		var e audit.Event
		if err := rows.Scan(&e.RequestID, &e.SubjectID, &e.KeyID, &e.Model, &e.Provider,
			&e.Status, &e.ErrorClass, &e.LatencyMillis, &e.PromptTokens, &e.CompletionTokens,
			&e.FirstTokenMillis, &e.Streaming, &e.CreatedAt, &e.TraceID, &e.RouteAttempts,
			&e.Protocol); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Usage aggregates request counts, errors, tokens, and latency percentiles
// per model and protocol. P50/P95 are true PostgreSQL percentiles over the
// filtered window. First-token percentiles are computed over the rows that
// recorded one (streams); groups without any recorded value report null —
// never a fabricated zero. The V1.4 cost fields come from the settlement
// ledger: CostMicros sums known settled cost (null when none is known),
// UnknownCostRequests counts settled rows whose cost stayed unknown (never
// counted as zero), and PriceVersions names the distinct price versions
// behind the known cost. A non-empty filter tenant is a mandatory predicate:
// llm_requests rows resolve through subjects.tenant_id, ledger rows through
// their tenant_id column.
func (d *DB) Usage(ctx context.Context, f mgmt.AuditFilter) ([]mgmt.UsageRow, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT model, COALESCE(protocol,'chat') AS protocol,
		       count(*) AS requests,
		       count(*) FILTER (WHERE status >= 400 OR COALESCE(error_class,'') <> '') AS errors,
		       COALESCE(sum(prompt_tokens), 0),
		       COALESCE(sum(completion_tokens), 0),
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0),
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY first_token_millis),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY first_token_millis),
		       sum(cost_micros)
		FROM llm_requests
		WHERE ($1 = '' OR subject_id = $1)
		  AND ($2 = '' OR model = $2)
		  AND ($3::timestamptz IS NULL OR created_at >= $3)
		  AND ($4::timestamptz IS NULL OR created_at <= $4)
		  AND ($5 = '' OR subject_id IN (SELECT id FROM subjects WHERE tenant_id = $5))
		GROUP BY model, protocol
		ORDER BY requests DESC
		LIMIT 100`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.UsageRow
	for rows.Next() {
		var s mgmt.UsageRow
		if err := rows.Scan(&s.Model, &s.Protocol, &s.Requests, &s.Errors,
			&s.PromptTokens, &s.OutputTokens, &s.P50Millis, &s.P95Millis,
			&s.FirstTokenP50Millis, &s.FirstTokenP95Millis, &s.CostMicros); err != nil {
			return nil, err
		}
		if s.Requests > 0 {
			s.ErrorRate = float64(s.Errors) / float64(s.Requests)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return d.mergeLedgerCost(ctx, f, out)
}

// mergeLedgerCost overlays the settlement-ledger cost aggregates onto the
// usage rows (matched by model and protocol). Ledger rows for models with no
// audit traffic in the window still appear, so costs stay explainable even
// when the audit row is missing.
func (d *DB) mergeLedgerCost(ctx context.Context, f mgmt.AuditFilter, rows []mgmt.UsageRow) ([]mgmt.UsageRow, error) {
	costRows, err := d.Pool.Query(ctx, `
		SELECT public_model, COALESCE(protocol,'chat') AS protocol,
		       sum(cost_micros),
		       count(*) FILTER (WHERE cost_micros IS NULL),
		       array_agg(DISTINCT price_version) FILTER (WHERE price_version IS NOT NULL)
		FROM usage_ledger
		WHERE settle_status = 'settled'
		  AND ($1 = '' OR subject_id = $1)
		  AND ($2 = '' OR public_model = $2)
		  AND ($3::timestamptz IS NULL OR settled_at >= $3)
		  AND ($4::timestamptz IS NULL OR settled_at <= $4)
		  AND ($5 = '' OR tenant_id = $5)
		GROUP BY public_model, protocol`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant)
	if err != nil {
		return nil, err
	}
	defer costRows.Close()
	type key struct{ model, protocol string }
	ledger := map[key]*mgmt.UsageRow{}
	for costRows.Next() {
		var k key
		var s mgmt.UsageRow
		var versions []int
		if err := costRows.Scan(&k.model, &k.protocol, &s.CostMicros,
			&s.UnknownCostRequests, &versions); err != nil {
			return nil, err
		}
		if len(versions) > 0 {
			slices.Sort(versions)
			s.PriceVersions = versions
		}
		ledger[k] = &s
	}
	if err := costRows.Err(); err != nil {
		return nil, err
	}
	for i := range rows {
		l, ok := ledger[key{rows[i].Model, rows[i].Protocol}]
		if !ok {
			continue
		}
		if l.CostMicros != nil {
			rows[i].CostMicros = l.CostMicros
		}
		rows[i].UnknownCostRequests = l.UnknownCostRequests
		rows[i].PriceVersions = l.PriceVersions
		delete(ledger, key{rows[i].Model, rows[i].Protocol})
	}
	// Ledger-only groups (cost evidence without audit rows in the window)
	// append after the audit-derived rows.
	for k, l := range ledger {
		rows = append(rows, mgmt.UsageRow{
			Model: k.model, Protocol: k.protocol,
			CostMicros: l.CostMicros, UnknownCostRequests: l.UnknownCostRequests,
			PriceVersions: l.PriceVersions,
		})
	}
	return rows, nil
}

// writeOp inserts a management-operation record on an executor that is
// either the pool or an open transaction, so the atomic mutation path can
// share the insert shape.
func writeOp(ctx context.Context, ex executor, op mgmt.AdminOp) error {
	op = mgmt.NormalizeOp(op)
	_, err := ex.Exec(ctx, `
		INSERT INTO admin_audit (created_at, action, target, admin_subject, detail)
		VALUES ($1, $2, $3, $4, $5)`,
		op.CreatedAt, op.Action, op.Target, op.AdminSubject, op.Detail)
	return err
}

// executor is the query surface shared by pgxpool.Pool and pgx.Tx; both are
// safe for one-shot Exec calls under this package's usage.
type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// WriteOp persists a management-operation record. Callers must surface
// failures: a management action without its audit row is an error. Mutations
// that the store persists (model enable/disable) must go through
// SetModelEnabledWithAudit so the record and the mutation commit together.
func (d *DB) WriteOp(ctx context.Context, op mgmt.AdminOp) error {
	return writeOp(ctx, d.Pool, op)
}

// Ops returns recent management operations, newest first.
func (d *DB) Ops(ctx context.Context, limit int) ([]mgmt.AdminOp, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := d.Pool.Query(ctx, `
		SELECT id, created_at, action, target, admin_subject, detail
		FROM admin_audit ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.AdminOp
	for rows.Next() {
		var e mgmt.AdminOp
		if err := rows.Scan(&e.ID, &e.CreatedAt, &e.Action, &e.Target, &e.AdminSubject, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Compile-time assertion that the DB satisfies the management service.
var _ mgmt.Service = (*DB)(nil)
