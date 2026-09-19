package adminauth

// Credential lifecycle: create, list, rotate, revoke, expire. Persistence
// goes through Store; implementations must commit each credential mutation
// and its management-audit record (AuditOp) as one atomic unit, so a
// committed change is always audited and a rolled-back change never leaves an
// audit row. Plaintext credentials exist only in the create/rotate return
// value and are never persisted, logged, or echoed in listings.

import (
	"context"
	"errors"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// Lifecycle errors surfaced by the Manager.
var (
	ErrNotFound = errors.New("admin credential not found")
	ErrInactive = errors.New("admin credential not active")
)

// CredentialRecord is the stored metadata for one admin credential. Only the
// salt and the irreversible digest are secret-shaped material; listings zero
// them before returning.
type CredentialRecord struct {
	ID           string
	AdminSubject string
	Salt         []byte
	Hash         []byte
	Prefix       string // display-safe prefix; never the full credential
	Scopes       []Scope
	Status       Status
	TenantID     string // "" = platform-global credential
	ExpiresAt    time.Time
	LastUsedAt   time.Time
	CreatedAt    time.Time
	RevokedAt    time.Time
	RotatedFrom  string // predecessor credential ID when created by rotation
}

// Redacted returns a copy with salt and digest zeroed, safe for listings and
// rotate replies.
func (r CredentialRecord) Redacted() CredentialRecord {
	r.Salt = nil
	r.Hash = nil
	return r
}

// Active reports whether the credential is usable at time now: active status
// and not past expiry.
func (r CredentialRecord) Active(now time.Time) error {
	if r.Status != StatusActive {
		return ErrRevoked
	}
	if !r.ExpiresAt.IsZero() && now.After(r.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

// Principal projects the record onto the authenticated principal.
func (r CredentialRecord) Principal() Principal {
	return Principal{
		CredentialID: r.ID,
		AdminSubject: r.AdminSubject,
		TenantID:     r.TenantID,
		Scopes:       r.Scopes,
	}
}

// AuditOp is the management-audit record for one credential mutation. It is
// committed in the same transaction as the mutation by every Store
// implementation and carries the acting principal's attribution (subject,
// credential, tenant boundary, effective scopes) through the mgmt.AdminOp
// actor block. Detail carries redacted summaries only — never plaintext
// credentials, digests, or salts.
type AuditOp = mgmt.AdminOp

// Store persists admin credentials. The PostgreSQL store is the production
// implementation; the in-memory store serves development mode and tests.
type Store interface {
	// Create persists the record and its audit row atomically.
	Create(ctx context.Context, rec CredentialRecord, op AuditOp) error
	// Get loads one record by ID.
	Get(ctx context.Context, id string) (CredentialRecord, error)
	// List returns records for one admin subject (metadata only). A non-empty
	// tenant is a mandatory predicate: only credentials bound to that tenant
	// are returned, never a post-query filter over the full set.
	List(ctx context.Context, subject, tenant string) ([]CredentialRecord, error)
	// Revoke revokes the credential and writes its audit row atomically.
	// Revoking an already-revoked credential is a no-op success that writes
	// no audit row (nothing mutated, nothing to audit).
	Revoke(ctx context.Context, id string, revokedAt time.Time, op AuditOp) error
	// Rotate inserts the successor and revokes the predecessor, with the
	// audit rows, in one transaction. A failure leaves the predecessor
	// active and writes nothing.
	Rotate(ctx context.Context, oldID string, successor CredentialRecord, revokedAt time.Time, op AuditOp) error
	// Resolve authenticates a presented credential: constant-time digest
	// comparison against active credentials, then the active/expiry check.
	// ErrInvalid/ErrExpired/ErrRevoked are indistinguishable to callers.
	Resolve(ctx context.Context, secret string, now time.Time) (CredentialRecord, error)
	// TouchLastUsed records last use. Implementations must be bounded (the
	// update is throttled) and must never block or fail the request that
	// triggered it.
	TouchLastUsed(ctx context.Context, id string, now time.Time)
	// LogAuth records one authentication attempt (success or failure) in the
	// management audit trail. Best effort: failures are logged, never fatal.
	LogAuth(ctx context.Context, p Principal, success bool, reason string, at time.Time)
}

// GeneratedCredential is the one-time response to a create/rotate call. The
// plaintext is shown exactly once and never persisted or logged.
type GeneratedCredential struct {
	Record    CredentialRecord // redacted: no salt/digest material
	Plaintext string
}

// Manager implements the credential lifecycle on top of a Store. It contains
// no authorization logic: the HTTP layer checks that the caller holds
// platform-admin (and respects tenant boundaries) before invoking it.
type Manager struct {
	Store Store
	Now   func() time.Time
}

// NewManager builds a Manager using the system clock.
func NewManager(s Store) *Manager {
	return &Manager{Store: s, Now: time.Now}
}

// CreateInput names a new credential. TenantID empty means platform-global;
// the HTTP layer forces tenant-bound creators onto their own tenant.
type CreateInput struct {
	AdminSubject string
	Scopes       []string
	TenantID     string
	ExpiresAt    time.Time // zero = no expiry
}

// Create generates a new active credential and returns the one-time
// plaintext.
func (m *Manager) Create(ctx context.Context, in CreateInput, op AuditOp) (GeneratedCredential, error) {
	scopes, err := ParseScopes(in.Scopes)
	if err != nil {
		return GeneratedCredential{}, err
	}
	if in.AdminSubject == "" {
		return GeneratedCredential{}, errors.New("admin subject is required")
	}
	rec, plain, err := newRecord(in.AdminSubject, scopes, in.TenantID, in.ExpiresAt, "")
	if err != nil {
		return GeneratedCredential{}, err
	}
	op.CreatedAt = m.stamp(op.CreatedAt)
	if err := m.Store.Create(ctx, rec, op); err != nil {
		return GeneratedCredential{}, err
	}
	return GeneratedCredential{Record: rec.Redacted(), Plaintext: plain}, nil
}

// Rotate creates a replacement credential (same subject, scopes, tenant, and
// expiry) and revokes the predecessor as one transaction. The returned old
// record is redacted metadata for the reply.
func (m *Manager) Rotate(ctx context.Context, id string, op AuditOp) (GeneratedCredential, CredentialRecord, error) {
	old, err := m.Store.Get(ctx, id)
	if err != nil {
		return GeneratedCredential{}, CredentialRecord{}, err
	}
	if err := old.Active(m.Now()); err != nil {
		if errors.Is(err, ErrRevoked) {
			return GeneratedCredential{}, old.Redacted(), ErrInactive
		}
		return GeneratedCredential{}, old.Redacted(), err
	}
	rec, plain, err := newRecord(old.AdminSubject, old.Scopes, old.TenantID, old.ExpiresAt, id)
	if err != nil {
		return GeneratedCredential{}, old.Redacted(), err
	}
	now := m.Now()
	op.CreatedAt = m.stamp(op.CreatedAt)
	if err := m.Store.Rotate(ctx, id, rec, now, op); err != nil {
		return GeneratedCredential{}, old.Redacted(), err
	}
	old.Status = StatusRevoked
	old.RevokedAt = now
	return GeneratedCredential{Record: rec.Redacted(), Plaintext: plain}, old.Redacted(), nil
}

// Revoke immediately disables a credential. Revoking an unknown credential
// returns ErrNotFound; an already-revoked credential is a no-op success.
func (m *Manager) Revoke(ctx context.Context, id string, op AuditOp) error {
	rec, err := m.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if rec.Status == StatusRevoked {
		return nil
	}
	op.CreatedAt = m.stamp(op.CreatedAt)
	return m.Store.Revoke(ctx, id, m.Now(), op)
}

// Get returns redacted metadata for one credential (tenant checks, listings).
func (m *Manager) Get(ctx context.Context, id string) (CredentialRecord, error) {
	rec, err := m.Store.Get(ctx, id)
	if err != nil {
		return CredentialRecord{}, err
	}
	return rec.Redacted(), nil
}

// List returns redacted metadata for the admin subject (or every subject
// when empty), bounded to the tenant when one is given.
func (m *Manager) List(ctx context.Context, subject, tenant string) ([]CredentialRecord, error) {
	recs, err := m.Store.List(ctx, subject, tenant)
	if err != nil {
		return nil, err
	}
	out := make([]CredentialRecord, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Redacted())
	}
	return out, nil
}

func (m *Manager) stamp(t time.Time) time.Time {
	if t.IsZero() {
		return m.Now()
	}
	return t
}

func newRecord(subject string, scopes []Scope, tenantID string, expiresAt time.Time, rotatedFrom string) (CredentialRecord, string, error) {
	plain, err := GenerateCredential()
	if err != nil {
		return CredentialRecord{}, "", err
	}
	salt, err := NewSalt()
	if err != nil {
		return CredentialRecord{}, "", err
	}
	id, err := NewID()
	if err != nil {
		return CredentialRecord{}, "", err
	}
	return CredentialRecord{
		ID:           id,
		AdminSubject: subject,
		Salt:         salt,
		Hash:         HashCredential(salt, plain),
		Prefix:       DisplayPrefix(plain),
		Scopes:       scopes,
		Status:       StatusActive,
		TenantID:     tenantID,
		ExpiresAt:    expiresAt,
		CreatedAt:    time.Now(),
		RotatedFrom:  rotatedFrom,
	}, plain, nil
}
