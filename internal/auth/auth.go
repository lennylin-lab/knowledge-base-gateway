// Package auth implements API-key hashing, lookup, lifecycle, and Principal
// resolution. Plaintext keys are never logged and never stored.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Status of an API key.
type Status string

const (
	StatusActive  Status = "active"
	StatusRevoked Status = "revoked"
)

// Principal is the resolved identity after successful authentication.
type Principal struct {
	SubjectID string
	TenantID  string
	KeyID     string
}

// KeyRecord is the stored metadata for one key.
type KeyRecord struct {
	ID          string
	TenantID    string
	Subject     string
	Salt        []byte
	Hash        []byte
	Prefix      string // display-safe key prefix; never the full key
	Status      Status
	ExpiresAt   time.Time // zero means no expiry
	LastUsed    time.Time
	CreatedAt   time.Time
	RevokedAt   time.Time
	RotatedFrom string // predecessor key ID when created by rotation
}

// Errors surfaced by the authenticator.
var (
	ErrInvalid = errors.New("invalid api key")
	ErrExpired = errors.New("api key expired")
	ErrRevoked = errors.New("api key revoked")
)

// HashAPIKey returns the salted SHA-256 hash of a plaintext key.
func HashAPIKey(salt []byte, key string) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(key))
	return h.Sum(nil)
}

// NewSalt returns a random 16-byte salt.
func NewSalt() ([]byte, error) {
	s := make([]byte, 16)
	if _, err := rand.Read(s); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}
	return s, nil
}

// Store keeps API key records and authenticates bearer keys. It satisfies the
// lifecycle MutationStore for development mode.
type Store struct {
	mu    sync.RWMutex
	byKey map[string]*KeyRecord // hex(hash) -> record
	byID  map[string]*KeyRecord
}

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{byKey: map[string]*KeyRecord{}, byID: map[string]*KeyRecord{}}
}

// Put stores a record (used at config load and by tests).
func (s *Store) Put(rec KeyRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := rec
	s.byKey[hex.EncodeToString(rec.Hash)] = &stored
	s.byID[rec.ID] = &stored
}

// Authenticate resolves a bearer key to a Principal, or returns one of
// ErrInvalid/ErrExpired/ErrRevoked. Comparison is constant time.
func (s *Store) Authenticate(key string, now time.Time) (Principal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rec := range s.byKey {
		if subtle.ConstantTimeCompare(HashAPIKey(rec.Salt, key), rec.Hash) == 1 {
			switch {
			case rec.Status != StatusActive:
				return Principal{}, ErrRevoked
			case !rec.ExpiresAt.IsZero() && now.After(rec.ExpiresAt):
				return Principal{}, ErrExpired
			}
			return Principal{SubjectID: rec.Subject, TenantID: rec.TenantID, KeyID: rec.ID}, nil
		}
	}
	return Principal{}, ErrInvalid
}

// Create persists a new key record (lifecycle MutationStore).
func (s *Store) Create(_ context.Context, rec KeyRecord) error {
	s.Put(rec)
	return nil
}

// Get loads one record by ID (lifecycle MutationStore).
func (s *Store) Get(_ context.Context, keyID string) (KeyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.byID[keyID]
	if !ok {
		return KeyRecord{}, ErrNotFound
	}
	return *rec, nil
}

// List returns records for a subject, oldest first (lifecycle MutationStore).
func (s *Store) List(_ context.Context, subject string) ([]KeyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []KeyRecord
	for _, rec := range s.byID {
		if rec.Subject == subject {
			out = append(out, *rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// UpdateStatus transitions a key's status and records revoked_at (lifecycle
// MutationStore).
func (s *Store) UpdateStatus(_ context.Context, keyID string, status Status, revokedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[keyID]
	if !ok {
		return ErrNotFound
	}
	rec.Status = status
	if status == StatusRevoked {
		rec.RevokedAt = revokedAt
	}
	return nil
}
