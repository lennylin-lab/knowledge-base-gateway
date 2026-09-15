package pg

// Management query services backing the admin API. Every query returns
// metadata only: no API key hashes, no provider secrets or base URLs, and no
// prompt/completion content (the schema never stores it). The DB satisfies
// mgmt.Service.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
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

// SetModelEnabled persists a management enable/disable decision and bumps the
// row's configuration version. Unknown models return mgmt.ErrNotFound.
func (d *DB) SetModelEnabled(ctx context.Context, publicModel string, enabled bool) error {
	ct, err := d.Pool.Exec(ctx,
		`UPDATE model_catalog SET enabled = $2, config_version = config_version + 1 WHERE public_name = $1`,
		publicModel, enabled)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return mgmt.ErrNotFound
	}
	return nil
}

// Providers lists the provider registry status without endpoints.
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
		out = append(out, p)
	}
	return out, rows.Err()
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
		       streaming, created_at, trace_id, route_attempts, COALESCE(protocol,'chat')
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
			&e.Streaming, &e.CreatedAt, &e.TraceID, &e.RouteAttempts, &e.Protocol); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Usage aggregates request counts, errors, tokens, and latency percentiles
// per model and protocol. Cost estimation stays staged until pricing
// configuration exists.
func (d *DB) Usage(ctx context.Context, f mgmt.AuditFilter) ([]mgmt.UsageRow, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT model, COALESCE(protocol,'chat') AS protocol,
		       count(*) AS requests,
		       count(*) FILTER (WHERE status >= 400 OR COALESCE(error_class,'') <> '') AS errors,
		       COALESCE(sum(prompt_tokens), 0),
		       COALESCE(sum(completion_tokens), 0),
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0)
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
			&s.PromptTokens, &s.OutputTokens, &s.P50Millis, &s.P95Millis); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// WriteOp persists a management-operation record. Callers must surface
// failures: a management action without its audit row is an error.
func (d *DB) WriteOp(ctx context.Context, op mgmt.AdminOp) error {
	subject := op.AdminSubject
	if subject == "" {
		subject = "admin-token"
	}
	detail := op.Detail
	if len(detail) == 0 {
		detail = json.RawMessage(`{}`)
	}
	created := op.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO admin_audit (created_at, action, target, admin_subject, detail)
		VALUES ($1, $2, $3, $4, $5)`,
		created, op.Action, op.Target, subject, detail)
	return err
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
