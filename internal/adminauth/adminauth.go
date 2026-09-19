// Package adminauth implements V1.4 scoped admin identities: the closed
// scope vocabulary, the authenticated admin Principal, salted irreversible
// credential hashing in a domain distinct from API keys, and the credential
// lifecycle (create/rotate/revoke/expire) on top of a Store. Plaintext
// credentials start with the "kba_" marker, are shown exactly once in the
// create/rotate reply, and are never persisted or logged.
//
// The legacy GATEWAY_ADMIN_TOKEN remains a separate authentication path: it
// resolves to a bootstrap Principal with platform-admin behavior so both
// paths work during the migration until scoped credentials are
// production-verified (see docs/admin-rbac.md).
package adminauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Scope is one entry of the closed admin permission vocabulary. Scopes are
// the only authorization currency: handlers must never compare role names
// directly, and the database CHECK on admin_credentials.scopes rejects any
// value outside this set.
type Scope string

const (
	// ScopeViewer reads models, providers, policies, audit, and usage metadata.
	ScopeViewer Scope = "viewer"
	// ScopeOperator flips model/provider switches, routes, and subject
	// default-model slots.
	ScopeOperator Scope = "operator"
	// ScopeBilling manages prices, budgets, and usage export.
	ScopeBilling Scope = "billing"
	// ScopePlatformAdmin manages credentials, tenants, policies, and every
	// other administrative operation.
	ScopePlatformAdmin Scope = "platform-admin"
)

// AllScopes is the closed scope vocabulary in canonical order.
var AllScopes = []Scope{ScopeViewer, ScopeOperator, ScopeBilling, ScopePlatformAdmin}

// implications is the single owner of the implication rules: a grant of one
// scope satisfies every scope it implies (platform-admin is the superset;
// operator and billing both carry read access).
var implications = map[Scope][]Scope{
	ScopeOperator:      {ScopeViewer},
	ScopeBilling:       {ScopeViewer},
	ScopePlatformAdmin: {ScopeViewer, ScopeOperator, ScopeBilling},
}

// Satisfies reports whether the granted scope fulfills the required scope,
// following the implication rules.
func (s Scope) Satisfies(required Scope) bool {
	if s == required {
		return true
	}
	for _, implied := range implications[s] {
		if implied == required {
			return true
		}
	}
	return false
}

// ParseScope validates one scope name against the closed vocabulary.
func ParseScope(raw string) (Scope, error) {
	s := Scope(strings.TrimSpace(raw))
	for _, known := range AllScopes {
		if s == known {
			return s, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownScope, raw)
}

// ParseScopes validates and deduplicates a scope list. At least one scope is
// required (mirroring the database cardinality CHECK); duplicates collapse so
// a redundant grant cannot widen or double-record anything.
func ParseScopes(raw []string) ([]Scope, error) {
	seen := map[Scope]struct{}{}
	out := make([]Scope, 0, len(raw))
	for _, r := range raw {
		s, err := ParseScope(r)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, ErrNoScopes
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ScopeNames renders scopes as canonical strings (audit views, responses).
func ScopeNames(scopes []Scope) []string {
	out := make([]string, 0, len(scopes))
	for _, s := range scopes {
		out = append(out, string(s))
	}
	sort.Strings(out)
	return out
}

// Principal is the resolved admin identity after successful authentication.
// TenantID is the tenant boundary: empty means platform-global (only such
// principals may see global data), non-empty binds every read and mutation to
// that tenant with mandatory query predicates.
type Principal struct {
	CredentialID string
	AdminSubject string
	TenantID     string
	Scopes       []Scope
	Bootstrap    bool // authenticated via the legacy GATEWAY_ADMIN_TOKEN
}

// Has reports whether the principal's grants satisfy the required scope.
func (p Principal) Has(required Scope) bool {
	for _, s := range p.Scopes {
		if s.Satisfies(required) {
			return true
		}
	}
	return false
}

// Global reports whether the principal crosses tenant boundaries (platform
// identities). Bootstrap tokens are always global.
func (p Principal) Global() bool { return p.TenantID == "" }

// Credential authentication failures. Handlers must answer every one of them
// with the same 401 envelope so credential states are never enumerable.
var (
	ErrInvalid = errors.New("invalid admin credential")
	ErrExpired = errors.New("admin credential expired")
	ErrRevoked = errors.New("admin credential revoked")
	// Grant validation errors (create/rotate payloads). ErrUnknownTenant is
	// translated by store implementations from the tenant foreign key so
	// callers can answer a clean 400 without importing driver types.
	ErrNoScopes      = errors.New("at least one scope is required")
	ErrUnknownScope  = errors.New("unknown scope")
	ErrUnknownTenant = errors.New("unknown tenant")
)

// hashDomain separates admin-credential digests from API-key digests: a
// captured digest of one kind can never be replayed as the other.
const hashDomain = "kbgw-admin-credential-v1"

// HashCredential returns the salted, domain-separated SHA-256 digest of a
// plaintext admin credential. Only this digest is ever persisted.
func HashCredential(salt []byte, secret string) []byte {
	h := sha256.New()
	h.Write([]byte(hashDomain))
	h.Write(salt)
	h.Write([]byte(secret))
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

// CredentialPrefix is the plaintext marker that routes a bearer token to the
// admin-credential authentication path. The underscore cannot appear in an
// API key (kb_ + hex), so the two namespaces never collide.
const CredentialPrefix = "kba_"

// HasCredentialPrefix reports whether a presented token is shaped like a
// stored admin credential.
func HasCredentialPrefix(token string) bool {
	return strings.HasPrefix(token, CredentialPrefix)
}

// GenerateCredential returns a new random plaintext credential.
func GenerateCredential() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate admin credential: %w", err)
	}
	return CredentialPrefix + hex.EncodeToString(b), nil
}

// DisplayPrefix returns the display-safe prefix of a plaintext credential
// (marker plus four hex characters); never enough to reconstruct anything.
func DisplayPrefix(secret string) string {
	if len(secret) > len(CredentialPrefix)+4 {
		return secret[:len(CredentialPrefix)+4]
	}
	return secret
}

// NewID returns a fresh credential identifier.
func NewID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate credential id: %w", err)
	}
	return "adm_" + hex.EncodeToString(b), nil
}

// Status of an admin credential. The closed set mirrors the database CHECK.
type Status string

const (
	StatusActive  Status = "active"
	StatusRevoked Status = "revoked"
)
