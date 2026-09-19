package httpapi

// V1.4 admin-credential lifecycle endpoints (/admin/admins): create, list,
// rotate, and revoke scoped management identities. Scope: platform-admin.
// The plaintext credential is returned exactly once in the create/rotate
// reply — it is never persisted, logged, or echoed in listings. Every
// mutation commits together with its management-audit record (actor
// attribution included), and tenant-bound platform-admins stay inside their
// tenant: they can only mint tenant-bound credentials, list their tenant's
// credentials, and touch credentials of their own tenant.

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
)

// registerAdminCredentialRoutes mounts /admin/admins. Called from
// NewAdminMux only when deps.AdminManager is non-nil.
func registerAdminCredentialRoutes(mux *http.ServeMux, guard func(http.HandlerFunc) http.HandlerFunc, deps AdminDeps) {
	mux.HandleFunc("/admin/admins", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreateAdminCredential(w, r, deps)
		case http.MethodGet:
			handleListAdminCredentials(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
	mux.HandleFunc("/admin/admins/", guard(func(w http.ResponseWriter, r *http.Request) {
		credID := strings.TrimPrefix(r.URL.Path, "/admin/admins/")
		parts := strings.Split(credID, "/")
		if len(parts) != 2 {
			writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_path", "use /admin/admins/{id}/rotate or /revoke")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use POST")
			return
		}
		switch parts[1] {
		case "rotate":
			handleRotateAdminCredential(w, r, deps, parts[0])
		case "revoke":
			handleRevokeAdminCredential(w, r, deps, parts[0])
		default:
			writeError(w, newRequestID(), http.StatusNotFound, "not_found", "unknown_action", "unknown admin credential action")
		}
	}))
}

// createAdminBody is the credential-creation payload. An empty tenant_id
// mints a platform-global credential (global platform-admins only);
// tenant-bound creators always mint credentials bound to their own tenant.
type createAdminBody struct {
	AdminSubject string   `json:"admin_subject"`
	Scopes       []string `json:"scopes"`
	TenantID     string   `json:"tenant_id"`
	ExpiresInHs  int      `json:"expires_in_hours"` // 0 = no expiry
}

func handleCreateAdminCredential(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	var body createAdminBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil || body.AdminSubject == "" {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "admin_subject is required")
		return
	}
	// Tenant boundary: a tenant-bound platform-admin cannot mint global or
	// foreign-tenant credentials (the rollback rule: never downgrade a
	// tenant-scoped identity into a global token).
	tenantID := body.TenantID
	if !principal.Global() {
		if tenantID == "" {
			tenantID = principal.TenantID
		}
		if tenantID != principal.TenantID {
			adminInsufficientScope(w, requestID)
			return
		}
	}
	var expires time.Time
	if body.ExpiresInHs > 0 {
		expires = deps.AdminManager.Now().Add(time.Duration(body.ExpiresInHs) * time.Hour)
	}
	detail, _ := json.Marshal(map[string]any{
		"admin_subject": body.AdminSubject, "scopes": body.Scopes,
		"tenant_id": tenantID, "expires_at": orNull(expires),
	})
	// The create audit row is targeted at the new credential's subject; the
	// detail above already carries the redacted metadata (scopes, tenant
	// binding, expiry — never plaintext material), and the store commits it
	// in the same transaction as the credential itself.
	gen, err := deps.AdminManager.Create(r.Context(), adminauth.CreateInput{
		AdminSubject: body.AdminSubject,
		Scopes:       body.Scopes,
		TenantID:     tenantID,
		ExpiresAt:    expires,
	}, adminOpFrom(r, "admin_credential_create", body.AdminSubject, detail))
	if err != nil {
		writeAdminCredentialErr(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"credential_id": gen.Record.ID, "credential": gen.Plaintext, // one-time plaintext
		"prefix": gen.Record.Prefix, "admin_subject": gen.Record.AdminSubject,
		"scopes": adminauth.ScopeNames(gen.Record.Scopes), "tenant_id": gen.Record.TenantID,
		"expires_at": orNull(gen.Record.ExpiresAt), "created_at": gen.Record.CreatedAt,
	})
}

func handleListAdminCredentials(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	recs, err := deps.AdminManager.List(r.Context(), r.URL.Query().Get("admin_subject"), principal.TenantID)
	if err != nil {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not list admin credentials")
		return
	}
	type meta struct {
		CredentialID string     `json:"credential_id"`
		AdminSubject string     `json:"admin_subject"`
		Scopes       []string   `json:"scopes"`
		TenantID     string     `json:"tenant_id,omitempty"`
		Prefix       string     `json:"prefix"`
		Status       string     `json:"status"`
		ExpiresAt    *time.Time `json:"expires_at"`
		LastUsedAt   *time.Time `json:"last_used_at"`
		CreatedAt    time.Time  `json:"created_at"`
		RevokedAt    *time.Time `json:"revoked_at"`
		RotatedFrom  string     `json:"rotated_from,omitempty"`
	}
	out := make([]meta, 0, len(recs))
	for _, rec := range recs {
		m := meta{
			CredentialID: rec.ID, AdminSubject: rec.AdminSubject,
			Scopes: adminauth.ScopeNames(rec.Scopes), TenantID: rec.TenantID,
			Prefix: rec.Prefix, Status: string(rec.Status),
			CreatedAt: rec.CreatedAt, RotatedFrom: rec.RotatedFrom,
		}
		if !rec.ExpiresAt.IsZero() {
			m.ExpiresAt = &rec.ExpiresAt
		}
		if !rec.LastUsedAt.IsZero() {
			m.LastUsedAt = &rec.LastUsedAt
		}
		if !rec.RevokedAt.IsZero() {
			m.RevokedAt = &rec.RevokedAt
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"credentials": out})
}

func handleRotateAdminCredential(w http.ResponseWriter, r *http.Request, deps AdminDeps, credID string) {
	requestID := newRequestID()
	if !requireAdminCredentialTenant(w, r, deps, credID) {
		return
	}
	detail, _ := json.Marshal(map[string]any{"rotated_from": credID})
	gen, old, err := deps.AdminManager.Rotate(r.Context(), credID, adminOpFrom(r, "admin_credential_rotate", credID, detail))
	if err != nil {
		writeAdminCredentialErr(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"credential_id": gen.Record.ID, "credential": gen.Plaintext, // one-time plaintext
		"rotated_from": old.ID, "prefix": gen.Record.Prefix,
		"admin_subject": gen.Record.AdminSubject, "scopes": adminauth.ScopeNames(gen.Record.Scopes),
		"tenant_id": gen.Record.TenantID, "expires_at": orNull(gen.Record.ExpiresAt),
	})
}

func handleRevokeAdminCredential(w http.ResponseWriter, r *http.Request, deps AdminDeps, credID string) {
	requestID := newRequestID()
	if !requireAdminCredentialTenant(w, r, deps, credID) {
		return
	}
	detail, _ := json.Marshal(map[string]any{"status": "revoked"})
	err := deps.AdminManager.Revoke(r.Context(), credID, adminOpFrom(r, "admin_credential_revoke", credID, detail))
	if err != nil {
		writeAdminCredentialErr(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"credential_id": credID, "status": "revoked"})
}

// requireAdminCredentialTenant applies the tenant boundary to rotate/revoke:
// tenant-bound platform-admins may only touch credentials of their own
// tenant. Violations answer the non-leaky credential_not_found.
func requireAdminCredentialTenant(w http.ResponseWriter, r *http.Request, deps AdminDeps, credID string) bool {
	principal, _ := principalFromContext(r.Context())
	if principal.Global() {
		return true
	}
	rec, err := deps.AdminManager.Get(r.Context(), credID)
	if err != nil {
		if errors.Is(err, adminauth.ErrNotFound) {
			writeError(w, newRequestID(), http.StatusNotFound, "not_found", "credential_not_found", "admin credential not found")
			return false
		}
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not load the admin credential")
		return false
	}
	if rec.TenantID != principal.TenantID {
		// Foreign and missing are indistinguishable.
		writeError(w, newRequestID(), http.StatusNotFound, "not_found", "credential_not_found", "admin credential not found")
		return false
	}
	return true
}

// writeAdminCredentialErr maps lifecycle errors. Grant validation errors
// (unknown scope, empty scope set) and unknown tenants are client faults;
// everything else is infrastructure.
func writeAdminCredentialErr(w http.ResponseWriter, requestID string, err error) {
	switch {
	case errors.Is(err, adminauth.ErrNotFound):
		writeError(w, requestID, http.StatusNotFound, "not_found", "credential_not_found", "admin credential not found")
	case errors.Is(err, adminauth.ErrInactive):
		writeError(w, requestID, http.StatusConflict, "invalid_request_error", "credential_not_active", "only active credentials can be rotated")
	case errors.Is(err, adminauth.ErrNoScopes), errors.Is(err, adminauth.ErrUnknownScope):
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "scopes must be a non-empty subset of viewer, operator, billing, platform-admin")
	case errors.Is(err, adminauth.ErrUnknownTenant):
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "unknown tenant_id")
	default:
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "credential_create_failed", "could not persist the admin credential")
	}
}
