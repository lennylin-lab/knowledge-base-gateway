// Package auth implements API-key hashing, lookup, and Principal resolution.
// Plaintext keys are never logged and never stored.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
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
	KeyID     string
}

// KeyRecord is the stored metadata for one key.
type KeyRecord struct {
	ID        string
	Subject   string
	Salt      []byte
	Hash      []byte
	Status    Status
	ExpiresAt time.Time // zero means no expiry
	LastUsed  time.Time
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

// Store keeps API key records and authenticates bearer keys.
type Store struct {
	mu    sync.RWMutex
	byKey map[string]*KeyRecord // hex(hash) -> record
}

// NewStore creates an empty store.
func NewStore() *Store { return &Store{byKey: map[string]*KeyRecord{}} }

// Put stores a record (used at config load and later by the DB-backed store).
func (s *Store) Put(rec KeyRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[hex.EncodeToString(rec.Hash)] = &rec
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
			return Principal{SubjectID: rec.Subject, KeyID: rec.ID}, nil
		}
	}
	return Principal{}, ErrInvalid
}
