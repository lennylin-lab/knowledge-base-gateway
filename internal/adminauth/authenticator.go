package adminauth

// Authentication of admin API callers. Two paths coexist during the migration
// (both must keep working until scoped credentials are production-verified):
//
//  1. stored credentials ("kba_..." plaintext, salted irreversible digests);
//  2. the legacy GATEWAY_ADMIN_TOKEN, resolved as a platform-admin bootstrap
//     Principal so its behavior is explicit and observable as the bootstrap
//     actor in the audit trail.
//
// Failure answers are uniform (no credential-state enumeration), failed
// attempts are rate limited independently of the model-traffic limiter, and
// post-auth bookkeeping (last-use, authentication audit) is bounded and never
// blocks the request.

import (
	"context"
	"crypto/subtle"
	"sync"
	"time"
)

// Authenticator resolves admin callers. Store may be nil when only the legacy
// bootstrap token is configured (development mode without persistence).
type Authenticator struct {
	Store       Store
	LegacyToken string
	Limiter     *AuthLimiter
	Now         func() time.Time
}

// BootstrapSubject is the observable actor name for legacy-token calls.
const BootstrapSubject = "bootstrap-token"

// BootstrapScopes is the explicit platform-admin behavior of the legacy token.
var BootstrapScopes = []Scope{ScopeViewer, ScopeOperator, ScopeBilling, ScopePlatformAdmin}

func (a *Authenticator) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// PostAuth finalizes authentication bookkeeping (last-use stamp, audit row).
// The caller runs it off the request path (bounded, best effort); tests may
// invoke it synchronously for determinism.
type PostAuth func()

// Authenticate resolves the presented token to a Principal.
//
// Errors:
//   - ErrRateLimited: too many recent failures for this client (429 upstream);
//   - ErrInvalid (and the credential-state errors): uniform 401 upstream.
//
// On success the returned PostAuth performs the bounded last-use update and
// the authentication-audit write; it never fails the request.
func (a *Authenticator) Authenticate(ctx context.Context, token, clientKey string) (Principal, PostAuth, error) {
	now := a.now()
	if a.Limiter != nil {
		if ok, retryAfter := a.Limiter.Allow(clientKey, now); !ok {
			return Principal{}, nil, &RateLimitError{RetryAfter: retryAfter}
		}
	}

	if HasCredentialPrefix(token) {
		return a.authenticateCredential(ctx, token, clientKey, now)
	}
	return a.authenticateLegacy(ctx, token, clientKey, now)
}

func (a *Authenticator) authenticateCredential(ctx context.Context, token, clientKey string, now time.Time) (Principal, PostAuth, error) {
	if a.Store == nil {
		a.fail(clientKey, now)
		return Principal{}, nil, ErrInvalid
	}
	rec, err := a.Store.Resolve(ctx, token, now)
	if err != nil {
		a.fail(clientKey, now)
		reason := "invalid"
		switch {
		case err == ErrExpired:
			reason = "expired"
		case err == ErrRevoked:
			reason = "revoked"
		}
		p := Principal{AdminSubject: "unknown"}
		return Principal{}, a.postAuth(p, false, reason, now), ErrInvalid
	}
	p := rec.Principal()
	return p, a.postAuth(p, true, "", now), nil
}

func (a *Authenticator) authenticateLegacy(ctx context.Context, token, clientKey string, now time.Time) (Principal, PostAuth, error) {
	if a.LegacyToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(a.LegacyToken)) != 1 {
		a.fail(clientKey, now)
		return Principal{}, a.postAuth(Principal{AdminSubject: "unknown"}, false, "invalid", now), ErrInvalid
	}
	p := Principal{
		AdminSubject: BootstrapSubject,
		Scopes:       BootstrapScopes,
		Bootstrap:    true,
	}
	return p, a.postAuth(p, true, "", now), nil
}

func (a *Authenticator) fail(clientKey string, now time.Time) {
	if a.Limiter != nil {
		a.Limiter.RecordFailure(clientKey, now)
	}
}

// postAuth builds the bounded bookkeeping closure: authentication audit row
// (always) and the throttled last-use stamp (stored credentials only).
func (a *Authenticator) postAuth(p Principal, success bool, reason string, now time.Time) PostAuth {
	return func() {
		if a.Store == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.Store.LogAuth(ctx, p, success, reason, now)
		if success && !p.Bootstrap && p.CredentialID != "" {
			a.Store.TouchLastUsed(ctx, p.CredentialID, now)
		}
	}
}

// RateLimitError reports an independently rate-limited admin authentication.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string { return "admin authentication rate limited" }

// AuthLimiter is the independent fixed-window failure limiter for admin
// authentication. Successes never count; only failures do, so a legitimate
// operator is never locked out by normal use.
type AuthLimiter struct {
	mu      sync.Mutex
	max     int
	windows map[string]*failureWindow
}

type failureWindow struct {
	start    time.Time
	failures int
}

// DefaultFailureLimit is the per-client failure budget per minute.
const DefaultFailureLimit = 20

// NewAuthLimiter builds the limiter with the default failure budget.
func NewAuthLimiter() *AuthLimiter {
	return &AuthLimiter{max: DefaultFailureLimit, windows: map[string]*failureWindow{}}
}

// Allow reports whether another authentication attempt may proceed, and for
// how long the client should wait when it may not.
func (l *AuthLimiter) Allow(clientKey string, now time.Time) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.windows[clientKey]
	if w == nil || now.Sub(w.start) >= time.Minute {
		// A new (or expired) window always allows; drop stale entries lazily.
		if len(l.windows) > 4096 {
			l.windows = map[string]*failureWindow{}
		}
		return true, 0
	}
	if w.failures >= l.max {
		return false, time.Minute - now.Sub(w.start)
	}
	return true, 0
}

// RecordFailure counts one failed authentication in the client's window.
func (l *AuthLimiter) RecordFailure(clientKey string, now time.Time) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.windows[clientKey]
	if w == nil || now.Sub(w.start) >= time.Minute {
		w = &failureWindow{start: now}
		l.windows[clientKey] = w
	}
	w.failures++
}
