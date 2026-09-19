package httpapi

// V1.4 scoped admin authentication and authorization. One middleware
// authenticates the caller (stored admin credential or the legacy
// GATEWAY_ADMIN_TOKEN bootstrap identity), one central route policy maps
// every admin route to its required scope and tenant-boundary rule, and the
// resolved principal travels on the request context for actor attribution.
// Handlers never compare roles themselves.
//
// Failure envelopes are uniform on purpose: every authentication failure is
// the same 401 (credential states are not enumerable), every scope or tenant
// boundary violation is the same 403 raised before any store call (target
// existence is not revealed), and rate-limited callers get the same 429 with
// Retry-After.

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// adminDisabledResponse writes the 404 posture used when the admin API is
// not enabled at all.
func adminDisabledResponse(w http.ResponseWriter) {
	writeError(w, newRequestID(), http.StatusNotFound, "not_found", "admin_disabled", "admin API is not enabled")
}

// adminPrincipalContextKey carries the authenticated admin principal.
type adminPrincipalContextKey struct{}

// principalFromContext returns the authenticated admin principal for the
// request; ok is false when the guard did not run (tests call handlers
// directly).
func principalFromContext(ctx context.Context) (adminauth.Principal, bool) {
	p, ok := ctx.Value(adminPrincipalContextKey{}).(adminauth.Principal)
	return p, ok
}

// adminRoutePolicy is the single owner of the route → (scope, tenant
// boundary) matrix. globalOnly marks platform-global resources: tenant-bound
// identities are denied even when they hold the scope. Routes not listed
// default to the strictest policy (platform-admin, global-only).
//
//	Matrix (see docs/admin-rbac.md for the annotated version):
//	  /admin/keys                 POST/GET      platform-admin  tenant-checkable
//	  /admin/keys/{id}/rotate     POST          platform-admin  tenant-checkable
//	  /admin/keys/{id}/revoke     POST          platform-admin  tenant-checkable
//	  /admin/admins               POST/GET      platform-admin  tenant-checkable
//	  /admin/admins/{id}/rotate   POST          platform-admin  tenant-checkable
//	  /admin/admins/{id}/revoke   POST          platform-admin  tenant-checkable
//	  /admin/models               GET           viewer          global
//	  /admin/models/{name}/...    POST          operator        global
//	  /admin/providers            GET           viewer          global
//	  /admin/policies             GET           viewer          tenant-predicated
//	  /admin/policies/{s}/...     POST          operator        tenant-checkable
//	  /admin/audit                GET           viewer          tenant-predicated
//	  /admin/usage                GET           viewer          tenant-predicated
//	  /admin/management-log       GET           viewer          global
//	  /admin/prices               GET/POST      billing         global
//	  /admin/budgets              GET/POST      billing         tenant-checkable
func adminRoutePolicy(method, path string) (scope adminauth.Scope, globalOnly bool) {
	switch {
	case path == "/admin/keys", strings.HasPrefix(path, "/admin/keys/"):
		return adminauth.ScopePlatformAdmin, false
	case path == "/admin/admins", strings.HasPrefix(path, "/admin/admins/"):
		return adminauth.ScopePlatformAdmin, false
	case path == "/admin/models" && method == http.MethodGet:
		return adminauth.ScopeViewer, true
	case strings.HasPrefix(path, "/admin/models/"):
		return adminauth.ScopeOperator, true
	case path == "/admin/providers":
		return adminauth.ScopeViewer, true
	case path == "/admin/policies":
		return adminauth.ScopeViewer, false
	case strings.HasPrefix(path, "/admin/policies/"):
		return adminauth.ScopeOperator, false
	case path == "/admin/audit":
		return adminauth.ScopeViewer, false
	case path == "/admin/usage":
		return adminauth.ScopeViewer, false
	case path == "/admin/management-log":
		return adminauth.ScopeViewer, true
	case path == "/admin/prices":
		return adminauth.ScopeBilling, true
	case path == "/admin/budgets":
		return adminauth.ScopeBilling, false
	default:
		return adminauth.ScopePlatformAdmin, true
	}
}

// adminInsufficientScope writes the uniform 403 envelope used for both scope
// and tenant-boundary denials (raised before any store access).
func adminInsufficientScope(w http.ResponseWriter, requestID string) {
	writeError(w, requestID, http.StatusForbidden, "permission_error", "insufficient_scope",
		"this admin credential is not authorized for the requested management operation")
}

// adminAuthRateLimited writes the 429 envelope for the independent admin
// authentication limiter.
func adminAuthRateLimited(w http.ResponseWriter, requestID string, retryAfter time.Duration) {
	if retryAfter > 0 {
		w.Header().Set("Retry-After", retryAfterText(retryAfter))
	}
	writeError(w, requestID, http.StatusTooManyRequests, "rate_limit_error", "admin_rate_limited",
		"too many failed admin authentications; retry later")
}

func retryAfterText(d time.Duration) string {
	secs := int(d.Seconds()) + 1
	if secs < 1 {
		secs = 1
	}
	return strconv.Itoa(secs)
}

// clientKey identifies the rate-limiting bucket: the remote host, without
// port. Credential material never enters the key.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// authenticateAdmin resolves the caller through the full dual-path
// authenticator when wired (stored credentials plus the legacy bootstrap
// token); otherwise it falls back to the legacy-token-only comparison so
// token-only deployments behave exactly as before. Returns the principal and
// whether the request may proceed (errors already written).
func (d AdminDeps) authenticateAdmin(w http.ResponseWriter, r *http.Request) (adminauth.Principal, bool) {
	requestID := newRequestID()
	writeUnauthorized := func() {
		// Uniform envelope: invalid, expired, revoked, and unknown are
		// indistinguishable so credential states never leak.
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "invalid_admin_token", "invalid admin token")
	}

	token, ok := bearer(r)
	if !ok {
		writeUnauthorized()
		return adminauth.Principal{}, false
	}

	if d.AdminAuth != nil {
		p, post, err := d.AdminAuth.Authenticate(r.Context(), token, clientKey(r))
		if post != nil {
			// Bounded bookkeeping (auth audit row for success AND failure,
			// throttled last-use stamp) runs off the request path; the
			// closure carries its own timeout.
			go post()
		}
		var rlErr *adminauth.RateLimitError
		switch {
		case errors.As(err, &rlErr):
			adminAuthRateLimited(w, requestID, rlErr.RetryAfter)
			return adminauth.Principal{}, false
		case err != nil:
			writeUnauthorized()
			return adminauth.Principal{}, false
		}
		return p, true
	}

	// Legacy-token-only mode (development/bootstrap): the historical
	// constant-time comparison, unchanged.
	if d.Token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(d.Token)) != 1 {
		writeUnauthorized()
		return adminauth.Principal{}, false
	}
	return d.bootstrapPrincipal(), true
}

// bootstrapPrincipal is the explicit legacy-token identity: platform-admin
// behavior, observable as the bootstrap actor.
func (d AdminDeps) bootstrapPrincipal() adminauth.Principal {
	return adminauth.Principal{
		AdminSubject: adminauth.BootstrapSubject,
		Scopes:       adminauth.BootstrapScopes,
		Bootstrap:    true,
	}
}

// guard wraps a handler with authentication, the central scope policy, and
// the tenant-boundary rule, then passes the principal via context.
func (d AdminDeps) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !d.Enabled() {
			adminDisabledResponse(w)
			return
		}
		principal, ok := d.authenticateAdmin(w, r)
		if !ok {
			return
		}
		scope, globalOnly := adminRoutePolicy(r.Method, r.URL.Path)
		if !principal.Has(scope) || (globalOnly && !principal.Global()) {
			adminInsufficientScope(w, newRequestID())
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), adminPrincipalContextKey{}, principal)))
	}
}

// adminOpFrom builds a management-audit op attributed to the authenticated
// caller: actor subject, credential, tenant boundary, and effective scopes.
func adminOpFrom(r *http.Request, action, target string, detail []byte) mgmt.AdminOp {
	op := mgmt.AdminOp{CreatedAt: time.Now(), Action: action, Target: target, Detail: detail}
	if p, ok := principalFromContext(r.Context()); ok {
		op.AdminSubject = p.AdminSubject
		op.Actor = mgmt.AdminActor{
			CredentialID: p.CredentialID,
			TenantID:     p.TenantID,
			Scopes:       adminauth.ScopeNames(p.Scopes),
			Bootstrap:    p.Bootstrap,
		}
	}
	return op
}

// mapMgmtErr translates management-store errors on tenant-bounded requests:
// a boundary violation is a uniform 403 (the service cannot predicate → the
// request is denied, never widened).
func mapMgmtErr(w http.ResponseWriter, requestID string, err error) bool {
	if errors.Is(err, mgmt.ErrTenantBoundary) {
		adminInsufficientScope(w, requestID)
		return true
	}
	return false
}
