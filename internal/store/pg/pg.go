// Package pg implements the persistent stores on PostgreSQL via pgx. It never
// stores provider secrets or plaintext API keys, and all queries are
// parameterized.
package pg

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

// DB wraps the connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// Connect builds a pool from a database URL or DSN and verifies that the
// server actually accepts a connection. Pool creation in pgx v5 is lazy
// (MinConns defaults to 0), so without an explicit ping a bad host or a
// rejected credential would only surface at the first query. Callers may
// treat a Connect error as "database unavailable": it carries no DSN text
// (main sanitizes it before logging).
func Connect(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	// Bound the verification independently of the caller's context: startup
	// must fail fast and classifiably, not hang on a blackholed host.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}
	return &DB{Pool: pool}, nil
}

// Close releases the pool.
func (d *DB) Close() { d.Pool.Close() }

// Ready pings the database for /readyz.
func (d *DB) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}

// --- API keys ------------------------------------------------------------

// KeyRecord is the stored api_keys row projection.
type keyRow struct {
	ID          string
	TenantID    *string
	Subject     string
	Salt        []byte
	Hash        []byte
	Prefix      string
	Status      string
	ExpiresAt   *time.Time
	LastUsed    *time.Time
	CreatedAt   time.Time
	RevokedAt   *time.Time
	RotatedFrom *string
}

func (r keyRow) toAuth() auth.KeyRecord {
	rec := auth.KeyRecord{
		ID: r.ID, Subject: r.Subject, Salt: r.Salt, Hash: r.Hash, Prefix: r.Prefix,
		CreatedAt: r.CreatedAt,
	}
	if r.TenantID != nil {
		rec.TenantID = *r.TenantID
	}
	if r.Status == "revoked" {
		rec.Status = auth.StatusRevoked
	} else {
		rec.Status = auth.StatusActive
	}
	if r.ExpiresAt != nil {
		rec.ExpiresAt = *r.ExpiresAt
	}
	if r.LastUsed != nil {
		rec.LastUsed = *r.LastUsed
	}
	if r.RevokedAt != nil {
		rec.RevokedAt = *r.RevokedAt
	}
	if r.RotatedFrom != nil {
		rec.RotatedFrom = *r.RotatedFrom
	}
	return rec
}

const keyColumns = `id, tenant_id, subject_id, key_salt, key_hash, key_prefix, status, expires_at, last_used_at, created_at, revoked_at, rotated_from`

// CreateKey implements auth.MutationStore.
func (d *DB) CreateKey(ctx context.Context, rec auth.KeyRecord) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO api_keys (id, tenant_id, subject_id, key_salt, key_hash, key_prefix, status, expires_at, created_at, revoked_at, rotated_from)
		VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11,''))`,
		rec.ID, rec.TenantID, rec.Subject, rec.Salt, rec.Hash, rec.Prefix,
		string(rec.Status), nullTime(rec.ExpiresAt), rec.CreatedAt,
		nullTime(rec.RevokedAt), rec.RotatedFrom)
	return err
}

// GetKey implements auth.MutationStore.
func (d *DB) GetKey(ctx context.Context, keyID string) (auth.KeyRecord, error) {
	row := d.Pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM api_keys WHERE id = $1`, keyID)
	var r keyRow
	if err := scanKey(row.Scan, &r); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return auth.KeyRecord{}, auth.ErrNotFound
		}
		return auth.KeyRecord{}, err
	}
	return r.toAuth(), nil
}

// ListKeys implements auth.MutationStore.
func (d *DB) ListKeys(ctx context.Context, subject string) ([]auth.KeyRecord, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT `+keyColumns+` FROM api_keys WHERE subject_id = $1 ORDER BY created_at`, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []auth.KeyRecord
	for rows.Next() {
		var r keyRow
		if err := scanKey(rows.Scan, &r); err != nil {
			return nil, err
		}
		out = append(out, r.toAuth())
	}
	return out, rows.Err()
}

// UpdateKeyStatus implements auth.MutationStore.
func (d *DB) UpdateKeyStatus(ctx context.Context, keyID string, status auth.Status, revokedAt time.Time) error {
	ct, err := d.Pool.Exec(ctx,
		`UPDATE api_keys SET status = $2, revoked_at = $3 WHERE id = $1`, keyID, string(status), nullTime(revokedAt))
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return auth.ErrNotFound
	}
	return nil
}

// GetKeyByHash implements the authentication lookup. Lookup is by the salted
// hash; comparison happens over the index-backed unique hash column, and the
// returned record drives the active/expired/revoked decision.
func (d *DB) GetKeyByHash(ctx context.Context, hash []byte) (auth.KeyRecord, error) {
	row := d.Pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM api_keys WHERE key_hash = $1`, hash)
	var r keyRow
	if err := scanKey(row.Scan, &r); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return auth.KeyRecord{}, auth.ErrInvalid
		}
		return auth.KeyRecord{}, err
	}
	return r.toAuth(), nil
}

// TouchKeyLastUsed records last_used_at asynchronously-friendly (best effort).
func (d *DB) TouchKeyLastUsed(ctx context.Context, keyID string, now time.Time) error {
	_, err := d.Pool.Exec(ctx, `UPDATE api_keys SET last_used_at = $2 WHERE id = $1`, keyID, now)
	return err
}

// ResolveAuth authenticates a plaintext key against the database. Because
// salts are per key, the presented key is hashed against each active key's
// stored salt and compared in constant time.
func (d *DB) ResolveAuth(ctx context.Context, key string, now time.Time) (auth.Principal, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT id, tenant_id, subject_id, key_salt, key_hash, status, expires_at
		 FROM api_keys WHERE status = 'active'`)
	if err != nil {
		return auth.Principal{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, subject, status string
		var tenant *string
		var salt, hash []byte
		var expires *time.Time
		if err := rows.Scan(&id, &tenant, &subject, &salt, &hash, &status, &expires); err != nil {
			return auth.Principal{}, err
		}
		if subtle.ConstantTimeCompare(auth.HashAPIKey(salt, key), hash) != 1 {
			continue
		}
		if expires != nil && now.After(*expires) {
			return auth.Principal{}, auth.ErrExpired
		}
		tenantID := ""
		if tenant != nil {
			tenantID = *tenant
		}
		return auth.Principal{SubjectID: subject, TenantID: tenantID, KeyID: id}, nil
	}
	if err := rows.Err(); err != nil {
		return auth.Principal{}, err
	}
	return auth.Principal{}, auth.ErrInvalid
}

func scanKey(scan func(dest ...any) error, r *keyRow) error {
	return scan(&r.ID, &r.TenantID, &r.Subject, &r.Salt, &r.Hash, &r.Prefix,
		&r.Status, &r.ExpiresAt, &r.LastUsed, &r.CreatedAt, &r.RevokedAt, &r.RotatedFrom)
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// HashHex is a debugging helper for tests; it never reconstructs plaintext.
func HashHex(b []byte) string { return hex.EncodeToString(b) }

// --- Catalog and routes --------------------------------------------------

// RouteConfig is one persisted model route binding.
type RouteConfig struct {
	PublicModel   string
	ProviderName  string
	UpstreamModel string
	Priority      int
	TimeoutMillis int
	Enabled       bool
}

// LoadCatalog reads enabled catalog entries including their declared
// capability matrix and configuration version. Capabilities is the public
// routing constraint; empty JSONB means the adapter-level matrix applies.
func (d *DB) LoadCatalog(ctx context.Context) ([]policy.ModelInfo, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT public_name, provider, upstream_model, enabled, COALESCE(capabilities, '{}'::jsonb), config_version
		 FROM model_catalog WHERE enabled`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []policy.ModelInfo
	for rows.Next() {
		var m policy.ModelInfo
		var caps []byte
		if err := rows.Scan(&m.PublicName, &m.Provider, &m.UpstreamModel, &m.Enabled, &caps, &m.ConfigVersion); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(caps, &m.Capabilities); err != nil {
			return nil, fmt.Errorf("model %s: parse capabilities: %w", m.PublicName, err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// LoadRoutes reads all enabled route bindings ordered by model and priority.
func (d *DB) LoadRoutes(ctx context.Context) ([]RouteConfig, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT public_model, provider, upstream_model, priority, timeout_ms, enabled
		FROM model_routes WHERE enabled ORDER BY public_model, priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RouteConfig
	for rows.Next() {
		var r RouteConfig
		if err := rows.Scan(&r.PublicModel, &r.ProviderName, &r.UpstreamModel,
			&r.Priority, &r.TimeoutMillis, &r.Enabled); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LoadProviders reads the enabled provider registry. Secrets are never here;
// only kind and base URL, with credentials injected from the environment.
type ProviderConfig struct {
	Name    string
	Kind    string
	BaseURL string
}

func (d *DB) LoadProviders(ctx context.Context) ([]ProviderConfig, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT name, kind, base_url FROM providers WHERE enabled`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderConfig
	for rows.Next() {
		var p ProviderConfig
		if err := rows.Scan(&p.Name, &p.Kind, &p.BaseURL); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// LoadLimits reads per-subject policy limits. max_input_tokens is the
// subject-level input ceiling enforced before any provider invocation.
func (d *DB) LoadLimits(ctx context.Context) (map[string]policy.Limits, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT subject_id, rate_per_minute, max_concurrent,
		       COALESCE(daily_tokens, 0), COALESCE(monthly_tokens, 0),
		       COALESCE(max_input_tokens, 0), COALESCE(max_output_tokens, 0)
		FROM access_policies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]policy.Limits{}
	for rows.Next() {
		var subject string
		var l policy.Limits
		if err := rows.Scan(&subject, &l.RatePerMinute, &l.MaxConcurrent,
			&l.DailyTokens, &l.MonthlyTokens, &l.MaxInputTokens, &l.MaxOutputTokens); err != nil {
			return nil, err
		}
		out[subject] = l
	}
	return out, rows.Err()
}

// ModelGrant is one access_policies grant: the subject may use the public
// model. Grants answer the Permitted question; ceilings live in LoadLimits.
type ModelGrant struct {
	Subject string
	Model   string
}

// LoadGrants reads the explicit subject/model grants from access_policies.
// In database mode these are the only permitted (subject, model) pairs;
// subjects without a row stay denied for that model.
func (d *DB) LoadGrants(ctx context.Context) ([]ModelGrant, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT subject_id, public_model FROM access_policies ORDER BY subject_id, public_model`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ModelGrant
	for rows.Next() {
		var g ModelGrant
		if err := rows.Scan(&g.Subject, &g.Model); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// --- Audit ---------------------------------------------------------------

// WriteAudit persists one audit event. Token counts stay NULL when usage is
// unknown; prompt and completion content is never persisted.
func (d *DB) WriteAudit(ctx context.Context, e audit.Event) error {
	_, err := d.Pool.Exec(ctx, `
		INSERT INTO llm_requests
			(request_id, subject_id, key_id, model, provider, status, error_class,
			 latency_ms, prompt_tokens, completion_tokens, streaming, created_at,
			 trace_id, route_attempts, cost_micros, protocol)
		VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,$15,NULLIF($16,''))
		ON CONFLICT (request_id) DO NOTHING`,
		e.RequestID, e.SubjectID, e.KeyID, e.Model, e.Provider, e.Status, e.ErrorClass,
		e.LatencyMillis, nullInt(e.PromptTokens), nullInt(e.CompletionTokens),
		e.Streaming, e.CreatedAt, e.TraceID, e.RouteAttempts, nullInt64(e.CostMicros),
		e.Protocol)
	return err
}

func nullInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

// --- Authentication ------------------------------------------------------

// Authenticator resolves bearer keys against the database, implementing the
// same surface as auth.Store used by the HTTP layer.
type Authenticator struct {
	DB  *DB
	Now func() time.Time
}

// Authenticate hashes the presented key against each active key's salt and
// returns the resolved Principal, or auth.ErrInvalid/ErrExpired/ErrRevoked.
func (a *Authenticator) Authenticate(key string, now time.Time) (auth.Principal, error) {
	p, err := a.DB.ResolveAuth(context.Background(), key, now)
	if err != nil {
		return p, err
	}
	_ = a.DB.TouchKeyLastUsed(context.Background(), p.KeyID, now)
	return p, nil
}

// MutationStore surface (auth lifecycle) implemented with explicit names.

// Create implements auth.MutationStore; it is an alias for CreateKey.
func (d *DB) Create(ctx context.Context, rec auth.KeyRecord) error { return d.CreateKey(ctx, rec) }

// Get implements auth.MutationStore; it is an alias for GetKey.
func (d *DB) Get(ctx context.Context, keyID string) (auth.KeyRecord, error) {
	return d.GetKey(ctx, keyID)
}

// List implements auth.MutationStore; it is an alias for ListKeys.
func (d *DB) List(ctx context.Context, subject string) ([]auth.KeyRecord, error) {
	return d.ListKeys(ctx, subject)
}

// UpdateStatus implements auth.MutationStore; alias for UpdateKeyStatus.
func (d *DB) UpdateStatus(ctx context.Context, keyID string, status auth.Status, revokedAt time.Time) error {
	return d.UpdateKeyStatus(ctx, keyID, status, revokedAt)
}
