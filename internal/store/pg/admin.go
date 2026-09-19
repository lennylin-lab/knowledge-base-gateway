package pg

// PostgreSQL implementation of the V1.4 admin credential store
// (internal/adminauth.Store). Every credential mutation commits together
// with its management-audit row (the atomic mutation/audit rule); rotation
// inserts the successor and revokes the predecessor in one transaction;
// resolution compares digests in constant time; last-use stamps are
// throttled in SQL and best effort. Only irreversible digests are persisted —
// the schema (migration 0008) rejects scope values outside the closed
// vocabulary and empty grant sets at the database boundary.
//
// Digest layout: migration 0008 defines a single UNIQUE credential_hash
// column and no salt column, so the per-credential salt is stored packed in
// front of its digest:
//
//	credential_hash = salt(16 bytes) || SHA256("kbgw-admin-credential-v1" || salt || secret)
//
// Resolution extracts the salt, recomputes the domain-separated digest, and
// compares in constant time. The digest column stays UNIQUE (no two
// credentials ever share a stored value) and the plaintext never exists in
// the database.

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// AdminCredentialStore implements adminauth.Store on the shared pool.
type AdminCredentialStore struct {
	DB *DB
}

const adminCredentialColumns = `
	id, admin_subject, credential_hash, credential_prefix, scopes, status,
	tenant_id, expires_at, last_used_at, created_at, revoked_at, rotated_from`

// adminDigestSaltLen is the byte length of the salt packed in front of the
// digest inside credential_hash (adminauth.NewSalt generates 16 bytes).
const adminDigestSaltLen = 16

// adminDigestParts splits the stored value into salt and digest.
func adminDigestParts(stored []byte) (salt, digest []byte, ok bool) {
	if len(stored) <= adminDigestSaltLen {
		return nil, nil, false
	}
	return stored[:adminDigestSaltLen], stored[adminDigestSaltLen:], true
}

// encodeAdminDigest renders the stored layout for a new record
// (salt || digest, the adminauth.KeyRecord pair).
func encodeAdminDigest(rec adminauth.CredentialRecord) []byte {
	out := make([]byte, 0, len(rec.Salt)+len(rec.Hash))
	out = append(out, rec.Salt...)
	return append(out, rec.Hash...)
}

func scanAdminCredential(scan func(dest ...any) error) (adminauth.CredentialRecord, error) {
	var rec adminauth.CredentialRecord
	var scopes []string
	var tenant, rotatedFrom *string
	var expires, lastUsed, revoked *time.Time
	var status string
	var storedHash []byte
	if err := scan(&rec.ID, &rec.AdminSubject, &storedHash, &rec.Prefix, &scopes, &status,
		&tenant, &expires, &lastUsed, &rec.CreatedAt, &revoked, &rotatedFrom); err != nil {
		return rec, err
	}
	// Unpack the stored salt||digest so the record matches the in-memory
	// shape; a malformed stored value leaves both empty and can never match.
	rec.Salt, rec.Hash, _ = adminDigestParts(storedHash)
	rec.Scopes = parseAdminScopes(scopes)
	if status == "revoked" {
		rec.Status = adminauth.StatusRevoked
	} else {
		rec.Status = adminauth.StatusActive
	}
	if tenant != nil {
		rec.TenantID = *tenant
	}
	if expires != nil {
		rec.ExpiresAt = *expires
	}
	if lastUsed != nil {
		rec.LastUsedAt = *lastUsed
	}
	if revoked != nil {
		rec.RevokedAt = *revoked
	}
	if rotatedFrom != nil {
		rec.RotatedFrom = *rotatedFrom
	}
	return rec, nil
}

// parseAdminScopes converts the stored scope names; unknown values cannot
// occur (the schema CHECK is the boundary) but are dropped defensively.
func parseAdminScopes(raw []string) []adminauth.Scope {
	out := make([]adminauth.Scope, 0, len(raw))
	for _, s := range raw {
		if sc, err := adminauth.ParseScope(s); err == nil {
			out = append(out, sc)
		}
	}
	return out
}

// adminAuditOp projects the lifecycle audit op onto the shared writeOp
// shape; AuditOp is a mgmt.AdminOp alias, so actor attribution rides along.
func adminAuditOp(op adminauth.AuditOp) mgmt.AdminOp { return op }

// isForeignKeyViolation reports SQLSTATE 23503 (e.g. a create naming an
// unknown tenant).
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

// Create implements adminauth.Store: credential row plus audit row in one
// transaction.
func (s AdminCredentialStore) Create(ctx context.Context, rec adminauth.CredentialRecord, op adminauth.AuditOp) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO admin_credentials
			(id, admin_subject, credential_hash, credential_prefix, scopes, status, tenant_id, expires_at, created_at, rotated_from)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7,''), $8, $9, NULLIF($10,''))`,
		rec.ID, rec.AdminSubject, encodeAdminDigest(rec), rec.Prefix, adminauth.ScopeNames(rec.Scopes),
		string(rec.Status), rec.TenantID, nullTime(rec.ExpiresAt), rec.CreatedAt, rec.RotatedFrom); err != nil {
		if isForeignKeyViolation(err) {
			return adminauth.ErrUnknownTenant
		}
		return err
	}
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Get implements adminauth.Store.
func (s AdminCredentialStore) Get(ctx context.Context, id string) (adminauth.CredentialRecord, error) {
	row := s.DB.Pool.QueryRow(ctx,
		`SELECT `+adminCredentialColumns+` FROM admin_credentials WHERE id = $1`, id)
	rec, err := scanAdminCredential(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return adminauth.CredentialRecord{}, adminauth.ErrNotFound
		}
		return adminauth.CredentialRecord{}, err
	}
	return rec, nil
}

// List implements adminauth.Store. A non-empty tenant is a mandatory SQL
// predicate (tenant-scoped callers can only ever see their tenant's rows).
func (s AdminCredentialStore) List(ctx context.Context, subject, tenant string) ([]adminauth.CredentialRecord, error) {
	rows, err := s.DB.Pool.Query(ctx,
		`SELECT `+adminCredentialColumns+` FROM admin_credentials
		 WHERE ($1 = '' OR admin_subject = $1) AND ($2 = '' OR tenant_id = $2)
		 ORDER BY created_at, id`, subject, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []adminauth.CredentialRecord
	for rows.Next() {
		rec, err := scanAdminCredential(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// Revoke implements adminauth.Store: the status flip and the audit row commit
// together; an already-revoked credential is a no-op (nothing mutated, so no
// audit row).
func (s AdminCredentialStore) Revoke(ctx context.Context, id string, revokedAt time.Time, op adminauth.AuditOp) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ct, err := tx.Exec(ctx,
		`UPDATE admin_credentials SET status = 'revoked', revoked_at = $2
		 WHERE id = $1 AND status = 'active'`, id, revokedAt)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		// Unknown vs already-revoked must be distinguishable for the caller.
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM admin_credentials WHERE id = $1`, id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return adminauth.ErrNotFound
		}
		if err != nil {
			return err
		}
		return nil
	}
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Rotate implements adminauth.Store: predecessor revocation, successor
// insert, and the audit row in one transaction. Losing a concurrent
// revocation or rotation of the predecessor rolls everything back.
func (s AdminCredentialStore) Rotate(ctx context.Context, oldID string, successor adminauth.CredentialRecord, revokedAt time.Time, op adminauth.AuditOp) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ct, err := tx.Exec(ctx,
		`UPDATE admin_credentials SET status = 'revoked', revoked_at = $2
		 WHERE id = $1 AND status = 'active'`, oldID, revokedAt)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return adminauth.ErrInactive
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO admin_credentials
			(id, admin_subject, credential_hash, credential_prefix, scopes, status, tenant_id, expires_at, created_at, rotated_from)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7,''), $8, $9, $10)`,
		successor.ID, successor.AdminSubject, encodeAdminDigest(successor), successor.Prefix,
		adminauth.ScopeNames(successor.Scopes), string(successor.Status), successor.TenantID,
		nullTime(successor.ExpiresAt), successor.CreatedAt, oldID); err != nil {
		return err
	}
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Resolve implements adminauth.Store: the presented secret is hashed with
// each active credential's stored salt and compared in constant time; the
// active/expiry decision follows. The active management set is small, so the
// scan stays bounded.
func (s AdminCredentialStore) Resolve(ctx context.Context, secret string, now time.Time) (adminauth.CredentialRecord, error) {
	rows, err := s.DB.Pool.Query(ctx,
		`SELECT credential_hash FROM admin_credentials WHERE status = 'active'`)
	if err != nil {
		return adminauth.CredentialRecord{}, err
	}
	var stored [][]byte
	for rows.Next() {
		var h []byte
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return adminauth.CredentialRecord{}, err
		}
		stored = append(stored, h)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return adminauth.CredentialRecord{}, err
	}
	rows.Close()

	for _, hash := range stored {
		salt, digest, ok := adminDigestParts(hash)
		if !ok {
			continue
		}
		if subtle.ConstantTimeCompare(adminauth.HashCredential(salt, secret), digest) != 1 {
			continue
		}
		return s.resolveByDigest(ctx, hash, now)
	}
	return adminauth.CredentialRecord{}, adminauth.ErrInvalid
}

// resolveByDigest loads the matched credential row and decides
// revoked/expired. A credential revoked between the digest scan and this
// read answers ErrRevoked through the status check.
func (s AdminCredentialStore) resolveByDigest(ctx context.Context, storedHash []byte, now time.Time) (adminauth.CredentialRecord, error) {
	row := s.DB.Pool.QueryRow(ctx,
		`SELECT `+adminCredentialColumns+` FROM admin_credentials WHERE credential_hash = $1`, storedHash)
	rec, err := scanAdminCredential(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return adminauth.CredentialRecord{}, adminauth.ErrInvalid
		}
		return adminauth.CredentialRecord{}, err
	}
	if err := rec.Active(now); err != nil {
		return adminauth.CredentialRecord{}, err
	}
	return rec, nil
}

// TouchLastUsed implements adminauth.Store: throttled (at most one update
// per credential per minute) so bursts never turn into write load, and best
// effort (errors are dropped; they must never fail the request). The window
// cutoff is computed in Go — interval arithmetic on a parameterized
// timestamptz inside the SQL text is ambiguous under the planner's parameter
// type inference and fails on real PostgreSQL; the Go-side cutoff also keeps
// the threshold identical to the in-memory store's.
func (s AdminCredentialStore) TouchLastUsed(ctx context.Context, id string, now time.Time) {
	_, _ = s.DB.Pool.Exec(ctx,
		`UPDATE admin_credentials SET last_used_at = $2
		 WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < $3)`,
		id, now, now.Add(-time.Minute))
}

// LogAuth implements adminauth.Store: one management-audit row per
// authentication attempt, success or failure. Failures record the reason
// class only — never the presented secret or any digest material. The actor
// block carries credential, tenant, and effective scopes.
func (s AdminCredentialStore) LogAuth(ctx context.Context, p adminauth.Principal, success bool, reason string, at time.Time) {
	action := "admin_auth"
	detail := map[string]any{"success": success, "bootstrap": p.Bootstrap}
	if !success {
		action = "admin_auth_failed"
		detail["reason"] = reason
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return
	}
	_ = writeOp(ctx, s.DB.Pool, mgmt.AdminOp{
		CreatedAt: at, Action: action, Target: p.CredentialID,
		AdminSubject: p.AdminSubject, Detail: raw,
		Actor: mgmt.AdminActor{
			CredentialID: p.CredentialID, TenantID: p.TenantID,
			Scopes: adminauth.ScopeNames(p.Scopes), Bootstrap: p.Bootstrap,
		},
	})
}

// --- API-key tenant boundary ----------------------------------------------

// SubjectTenant reports a subject's authoritative tenant (subjects.tenant_id).
// The api_keys tenant column is a principal field and never the boundary.
func (d *DB) SubjectTenant(ctx context.Context, subject string) (string, bool, error) {
	var tenant string
	err := d.Pool.QueryRow(ctx, `SELECT tenant_id FROM subjects WHERE id = $1`, subject).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return tenant, true, nil
}

// KeySubjectTenant reports the authoritative tenant of a key's subject
// (ok=false for unknown keys).
func (d *DB) KeySubjectTenant(ctx context.Context, keyID string) (string, bool, error) {
	var tenant string
	err := d.Pool.QueryRow(ctx, `
		SELECT s.tenant_id FROM api_keys k JOIN subjects s ON s.id = k.subject_id
		WHERE k.id = $1`, keyID).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return tenant, true, nil
}

// ListKeysInTenant lists a subject's keys with the subject's tenant as a
// mandatory query predicate (tenant-bound admin views).
func (d *DB) ListKeysInTenant(ctx context.Context, subject, tenant string) ([]auth.KeyRecord, error) {
	rows, err := d.Pool.Query(ctx, `
		SELECT k.id, k.tenant_id, k.subject_id, k.key_salt, k.key_hash, k.key_prefix,
		       k.status, k.expires_at, k.last_used_at, k.created_at, k.revoked_at, k.rotated_from
		FROM api_keys k
		JOIN subjects s ON s.id = k.subject_id
		WHERE k.subject_id = $1 AND s.tenant_id = $2
		ORDER BY k.created_at`, subject, tenant)
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

// Compile-time assertion that the store satisfies the adminauth contract.
var _ adminauth.Store = AdminCredentialStore{}
