package adminauth

// In-memory credential store for development mode and tests. The lifecycle
// mutations mirror the PostgreSQL transactional semantics under one lock:
// rotation inserts the successor and revokes the predecessor as one visible
// change, and audit rows are appended in the same critical section.

import (
	"context"
	"crypto/subtle"
	"sort"
	"sync"
	"time"
)

// MemoryStore keeps credentials and their audit rows in process.
type MemoryStore struct {
	mu     sync.Mutex
	byID   map[string]*CredentialRecord
	audits []AuditOp
	auths  []AuthEvent
}

// AuthEvent is one recorded authentication attempt (LogAuth), kept so tests
// can pin the authentication-audit contract.
type AuthEvent struct {
	Principal Principal
	Success   bool
	Reason    string
	At        time.Time
}

// NewMemoryStore creates an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: map[string]*CredentialRecord{}}
}

// Create implements Store.
func (s *MemoryStore) Create(_ context.Context, rec CredentialRecord, op AuditOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := rec
	s.byID[rec.ID] = &stored
	s.audits = append(s.audits, op)
	return nil
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, id string) (CredentialRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[id]
	if !ok {
		return CredentialRecord{}, ErrNotFound
	}
	return *rec, nil
}

// List implements Store. Tenant filtering is a mandatory predicate, matching
// the PostgreSQL implementation's SQL constraint.
func (s *MemoryStore) List(_ context.Context, subject, tenant string) ([]CredentialRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []CredentialRecord
	for _, rec := range s.byID {
		if subject != "" && rec.AdminSubject != subject {
			continue
		}
		if tenant != "" && rec.TenantID != tenant {
			continue
		}
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// Revoke implements Store. An already-revoked credential is a no-op that
// writes no audit row.
func (s *MemoryStore) Revoke(_ context.Context, id string, revokedAt time.Time, op AuditOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	if rec.Status == StatusRevoked {
		return nil
	}
	rec.Status = StatusRevoked
	rec.RevokedAt = revokedAt
	s.audits = append(s.audits, op)
	return nil
}

// Rotate implements Store: successor insert plus predecessor revocation and
// both audit rows, all under one lock.
func (s *MemoryStore) Rotate(_ context.Context, oldID string, successor CredentialRecord, revokedAt time.Time, op AuditOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.byID[oldID]
	if !ok {
		return ErrNotFound
	}
	if old.Status != StatusActive {
		return ErrInactive
	}
	stored := successor
	s.byID[successor.ID] = &stored
	old.Status = StatusRevoked
	old.RevokedAt = revokedAt
	s.audits = append(s.audits, op)
	return nil
}

// Resolve implements Store: constant-time digest comparison against active
// credentials, then the active/expiry decision.
func (s *MemoryStore) Resolve(_ context.Context, secret string, now time.Time) (CredentialRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.byID {
		if rec.Status != StatusActive {
			continue
		}
		if subtle.ConstantTimeCompare(HashCredential(rec.Salt, secret), rec.Hash) != 1 {
			continue
		}
		if err := rec.Active(now); err != nil {
			return CredentialRecord{}, err
		}
		return *rec, nil
	}
	return CredentialRecord{}, ErrInvalid
}

// TouchLastUsed implements Store: throttled and best effort.
func (s *MemoryStore) TouchLastUsed(_ context.Context, id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[id]
	if !ok {
		return
	}
	if rec.LastUsedAt.IsZero() || now.Sub(rec.LastUsedAt) >= time.Minute {
		rec.LastUsedAt = now
	}
}

// LogAuth implements Store.
func (s *MemoryStore) LogAuth(_ context.Context, p Principal, success bool, reason string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auths = append(s.auths, AuthEvent{Principal: p, Success: success, Reason: reason, At: at})
}

// AuditSnapshot returns the recorded credential-mutation audit rows (tests).
func (s *MemoryStore) AuditSnapshot() []AuditOp {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditOp, len(s.audits))
	copy(out, s.audits)
	return out
}

// AuthSnapshot returns the recorded authentication events (tests).
func (s *MemoryStore) AuthSnapshot() []AuthEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuthEvent, len(s.auths))
	copy(out, s.auths)
	return out
}
