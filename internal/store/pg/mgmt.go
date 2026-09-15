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
// or neither does. Unknown models return mgmt.ErrNotFound with nothing
// written, so a failed operation can never be reported as audited-and-done,
// and a committed mutation can never be reported as failed.
func (d *DB) SetModelEnabledWithAudit(ctx context.Context, publicModel string, enabled bool, op mgmt.AdminOp) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	ct, err := tx.Exec(ctx,
		`UPDATE model_catalog SET enabled = $2, config_version = config_version + 1 WHERE public_name = $1`,
		publicModel, enabled)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return mgmt.ErrNotFound
	}
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ModelEntry reads one catalog row regardless of enabled state. The runtime
// refresh path uses it to swap fresh rows (capabilities and configuration
// version included) into the live catalog; found is false for unknown models.
func (d *DB) ModelEntry(ctx context.Context, publicModel string) (policy.ModelInfo, bool, error) {
	row := d.Pool.QueryRow(ctx, `
		SELECT public_name, provider, upstream_model, enabled,
		       COALESCE(capabilities, '{}'::jsonb), config_version
		FROM model_catalog WHERE public_name = $1`, publicModel)
	var m policy.ModelInfo
	var caps []byte
	if err := row.Scan(&m.PublicName, &m.Provider, &m.UpstreamModel, &m.Enabled, &caps, &m.ConfigVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return policy.ModelInfo{}, false, nil
		}
		return policy.ModelInfo{}, false, err
	}
	if err := json.Unmarshal(caps, &m.Capabilities); err != nil {
		return policy.ModelInfo{}, false, fmt.Errorf("model %s: parse capabilities: %w", m.PublicName, err)
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

// Policies lists access_policies rows for one subject (or all when empty).
func (d *DB) Policies(ctx context.Context, subject string) ([]mgmt.PolicyView, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT subject_id, public_model, rate_per_minute, max_concurrent,
		       COALESCE(daily_tokens, 0), COALESCE(monthly_tokens, 0)
		FROM access_policies
		WHERE ($1 = '' OR subject_id = $1)
		ORDER BY subject_id, public_model
		LIMIT 500`, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mgmt.PolicyView
	for rows.Next() {
		var v mgmt.PolicyView
		if err := rows.Scan(&v.Subject, &v.PublicModel, &v.RatePerMinute, &v.MaxConcurrent,
			&v.DailyTokens, &v.MonthlyTokens); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// QueryAudit returns audit records matching the filter, newest first.
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
		ORDER BY created_at DESC
		LIMIT $6`,
		f.RequestID, f.Subject, f.Model, nullTime(f.From), nullTime(f.To), limit)
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
// never a fabricated zero. cost_micros sums recorded estimates and stays null
// when no row carries a known cost; pricing configuration does not exist yet,
// so in practice it is staged null.
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
		GROUP BY model, protocol
		ORDER BY requests DESC
		LIMIT 100`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To))
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
	return out, rows.Err()
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
