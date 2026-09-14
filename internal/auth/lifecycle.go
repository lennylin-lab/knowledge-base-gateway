package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Lifecycle errors surfaced by the manager.
var (
	ErrNotFound = fmt.Errorf("api key not found")
	ErrInactive = fmt.Errorf("api key not active")
)

// MutationStore is the write-capable key persistence used by the lifecycle
// manager. The PostgreSQL store implements it; the in-memory store is used in
// development and tests.
type MutationStore interface {
	// Create persists a new key record.
	Create(ctx context.Context, rec KeyRecord) error
	// Get loads one record by ID.
	Get(ctx context.Context, keyID string) (KeyRecord, error)
	// List returns records for a subject (metadata only).
	List(ctx context.Context, subject string) ([]KeyRecord, error)
	// UpdateStatus transitions a key's status and records revoked_at.
	UpdateStatus(ctx context.Context, keyID string, status Status, revokedAt time.Time) error
}

// GeneratedKey is the one-time response to a create/rotate call. Plaintext is
// shown exactly once and never persisted or logged.
type GeneratedKey struct {
	Record    KeyRecord
	Plaintext string
}

// Manager implements create/list/rotate/revoke/expire on top of a
// MutationStore. All returned plaintext keys start with the prefix "kb_".
type Manager struct {
	Store MutationStore
	Now   func() time.Time
}

// NewManager builds a Manager using the system clock.
func NewManager(s MutationStore) *Manager {
	return &Manager{Store: s, Now: time.Now}
}

// Create generates a new active key for the subject with an optional expiry.
// TenantID groups the key under a tenant; subject remains the auth dimension.
func (m *Manager) Create(ctx context.Context, subject, tenantID string, expiresAt time.Time) (GeneratedKey, error) {
	rec, plain, err := m.newRecord(subject, tenantID, "", expiresAt)
	if err != nil {
		return GeneratedKey{}, err
	}
	if err := m.Store.Create(ctx, rec); err != nil {
		return GeneratedKey{}, err
	}
	return GeneratedKey{Record: rec, Plaintext: plain}, nil
}

// Rotate creates a replacement key and revokes the old one atomically from the
// caller's perspective: the new key is created first so a failure leaves the
// old key active. The old record is returned for auditing.
func (m *Manager) Rotate(ctx context.Context, keyID string) (GeneratedKey, KeyRecord, error) {
	old, err := m.Store.Get(ctx, keyID)
	if err != nil {
		return GeneratedKey{}, KeyRecord{}, err
	}
	if old.Status != StatusActive {
		return GeneratedKey{}, old, ErrInactive
	}
	rec, plain, err := m.newRecord(old.Subject, old.TenantID, keyID, old.ExpiresAt)
	if err != nil {
		return GeneratedKey{}, old, err
	}
	if err := m.Store.Create(ctx, rec); err != nil {
		return GeneratedKey{}, old, err
	}
	if err := m.Store.UpdateStatus(ctx, keyID, StatusRevoked, m.Now()); err != nil {
		return GeneratedKey{}, old, err
	}
	return GeneratedKey{Record: rec, Plaintext: plain}, old, nil
}

// Revoke immediately disables a key.
func (m *Manager) Revoke(ctx context.Context, keyID string) error {
	rec, err := m.Store.Get(ctx, keyID)
	if err != nil {
		return err
	}
	if rec.Status == StatusRevoked {
		return nil
	}
	return m.Store.UpdateStatus(ctx, keyID, StatusRevoked, m.Now())
}

// List returns metadata for all keys of a subject. Salt/hash material is
// zeroed so metadata listings can never leak secrets.
func (m *Manager) List(ctx context.Context, subject string) ([]KeyRecord, error) {
	recs, err := m.Store.List(ctx, subject)
	if err != nil {
		return nil, err
	}
	for i := range recs {
		recs[i].Salt = nil
		recs[i].Hash = nil
	}
	return recs, nil
}

func (m *Manager) newRecord(subject, tenantID, rotatedFrom string, expiresAt time.Time) (KeyRecord, string, error) {
	plain, err := GenerateAPIKey()
	if err != nil {
		return KeyRecord{}, "", err
	}
	salt, err := NewSalt()
	if err != nil {
		return KeyRecord{}, "", err
	}
	idBytes := make([]byte, 8)
	if _, err := rand.Read(idBytes); err != nil {
		return KeyRecord{}, "", fmt.Errorf("generate key id: %w", err)
	}
	rec := KeyRecord{
		ID:          "key_" + hex.EncodeToString(idBytes),
		TenantID:    tenantID,
		Subject:     subject,
		Salt:        salt,
		Hash:        HashAPIKey(salt, plain),
		Prefix:      KeyPrefix(plain),
		Status:      StatusActive,
		ExpiresAt:   expiresAt,
		CreatedAt:   m.Now(),
		RotatedFrom: rotatedFrom,
	}
	return rec, plain, nil
}

// GenerateAPIKey returns a new random plaintext key.
func GenerateAPIKey() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate api key: %w", err)
	}
	return "kb_" + hex.EncodeToString(b), nil
}

// KeyPrefix returns the display-safe prefix of a plaintext key.
func KeyPrefix(key string) string {
	if len(key) > 10 {
		return key[:10]
	}
	return key
}

// TrimPrefix returns the key without its "kb_" marker, used by lookups that
// normalize caller input.
func TrimPrefix(key string) string { return strings.TrimPrefix(key, "kb_") }
