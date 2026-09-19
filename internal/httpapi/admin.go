package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// AdminDeps wires the key lifecycle manager and the management query service
// behind the scoped admin endpoints. Authentication runs through AdminAuth
// when wired (stored credentials with the legacy GATEWAY_ADMIN_TOKEN as the
// bootstrap identity); the legacy Token field alone keeps the historical
// token-only behavior for development and bootstrap deployments.
type AdminDeps struct {
	Manager *auth.Manager
	Logger  *slog.Logger
	Token   string // GATEWAY_ADMIN_TOKEN; empty disables the legacy path
	Mgmt    mgmt.Service
	// AdminAuth authenticates scoped admin credentials (and the legacy token
	// as the bootstrap identity) with the independent rate limiter. Nil keeps
	// the token-only posture.
	AdminAuth *adminauth.Authenticator
	// AdminManager drives the admin-credential lifecycle behind
	// /admin/admins. Nil disables those endpoints.
	AdminManager *adminauth.Manager
	// ApplyModelChange is the runtime refresh boundary for the model
	// enable/disable switch. It is invoked after the persisted mutation and
	// its audit record committed atomically, and must make the change visible
	// to the running process (catalog and route tables) without a restart.
	// A returned error is reported as refresh_failed: the change stays
	// persisted and audited, and the response makes clear the refresh — not
	// the mutation — failed.
	ApplyModelChange func(ctx context.Context, publicModel string, enabled bool) error
	// ApplyPolicyChange is the runtime refresh boundary for the subject
	// default-model mutation. It runs after the persisted mutation and its
	// audit record committed atomically, and reloads the subject's policy
	// (limits and default-model slots) into the running process. Failure
	// semantics mirror ApplyModelChange: refresh_failed means the change is
	// committed and audited but the live process may be divergent.
	ApplyPolicyChange func(ctx context.Context, subject string) error
	// ProviderRuntime reports live per-provider breaker state so
	// /admin/providers reflects the running process instead of the registry
	// alone. A false ok leaves the store-reported fields untouched.
	ProviderRuntime func(name string) (mgmt.ProviderRuntime, bool)
	// Accounting is the pricing/budget management surface (V1.4, database
	// mode). Nil disables /admin/prices and /admin/budgets and omits the
	// budget-utilization section of /admin/usage.
	Accounting AccountingAdmin
}

// Enabled reports whether the admin API accepts any authentication path.
func (d AdminDeps) Enabled() bool {
	return d.Token != "" || d.AdminAuth != nil
}

// NewAdminMux builds the management API. Endpoints are disabled (404) unless
// an authentication path is configured (legacy token or stored credentials);
// the listener must stay on an internal network. Every request is
// authenticated, checked against the central route→scope policy
// (adminRoutePolicy), and attributed in each mutation's management-audit
// record.
func NewAdminMux(deps AdminDeps) *http.ServeMux {
	// Management failure paths log; tests build deps without a logger, so
	// normalize once instead of guarding every log site.
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	guard := deps.guard

	mux.HandleFunc("/admin/keys", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreateKey(w, r, deps)
		case http.MethodGet:
			handleListKeys(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
	mux.HandleFunc("/admin/keys/", guard(func(w http.ResponseWriter, r *http.Request) {
		keyID := strings.TrimPrefix(r.URL.Path, "/admin/keys/")
		parts := strings.Split(keyID, "/")
		if len(parts) != 2 {
			writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_path", "use /admin/keys/{id}/rotate or /revoke")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use POST")
			return
		}
		switch parts[1] {
		case "rotate":
			handleRotateKey(w, r, deps, parts[0])
		case "revoke":
			handleRevokeKey(w, r, deps, parts[0])
		default:
			writeError(w, newRequestID(), http.StatusNotFound, "not_found", "unknown_action", "unknown key action")
		}
	}))

	// V1.4 admin-credential lifecycle: create/list/rotate/revoke scoped
	// identities (platform-admin scope; tenant-bound creators stay within
	// their tenant).
	if deps.AdminManager != nil {
		registerAdminCredentialRoutes(mux, guard, deps)
	}

	// V1.2 management queries: read-only operations plus the model
	// enable/disable switch, all management-audited with actor attribution.
	if deps.Mgmt != nil {
		mgmtGuard := guard(func(w http.ResponseWriter, r *http.Request) {
			handleManagement(w, r, deps)
		})
		mux.HandleFunc("/admin/models", mgmtGuard)
		mux.HandleFunc("/admin/models/", mgmtGuard)
		mux.HandleFunc("/admin/providers", mgmtGuard)
		mux.HandleFunc("/admin/policies", mgmtGuard)
		mux.HandleFunc("/admin/policies/", mgmtGuard)
		mux.HandleFunc("/admin/audit", mgmtGuard)
		mux.HandleFunc("/admin/usage", mgmtGuard)
		mux.HandleFunc("/admin/management-log", mgmtGuard)
	}
	if deps.Accounting != nil {
		registerAccountingAdmin(mux, guard, deps)
	}
	return mux
}

// TenantKeyBoundary lets tenant-bound platform-admins operate on API keys
// strictly inside their tenant. The tenant is resolved from subjects.tenant_id
// — the authoritative subject→tenant binding — never from the optional
// api_keys principal column. Without this boundary (development mode),
// tenant-bound callers are denied (fail closed).
type TenantKeyBoundary interface {
	// SubjectTenant reports the authoritative tenant of a subject (ok=false
	// when the subject does not exist).
	SubjectTenant(ctx context.Context, subject string) (tenant string, ok bool, err error)
	// ListKeysInTenant lists a subject's keys with the tenant as a mandatory
	// predicate.
	ListKeysInTenant(ctx context.Context, subject, tenant string) ([]auth.KeyRecord, error)
	// KeySubjectTenant reports the authoritative tenant of a key's subject
	// (ok=false for unknown keys).
	KeySubjectTenant(ctx context.Context, keyID string) (tenant string, ok bool, err error)
}

// keyBoundary extracts the tenant boundary from the wired key store.
func keyBoundary(deps AdminDeps) TenantKeyBoundary {
	if deps.Manager == nil {
		return nil
	}
	kb, _ := deps.Manager.Store.(TenantKeyBoundary)
	return kb
}

// requireTenantKeys answers the tenant-bound preconditions for key
// operations: a boundary must exist and the target subject's authoritative
// tenant must match the caller's. Absent boundary → 403; foreign or unknown
// subject → non-leaky 404.
func requireTenantKeys(w http.ResponseWriter, r *http.Request, deps AdminDeps, subject string) bool {
	p, _ := principalFromContext(r.Context())
	if p.Global() {
		return true
	}
	requestID := newRequestID()
	kb := keyBoundary(deps)
	if kb == nil {
		adminInsufficientScope(w, requestID)
		return false
	}
	tenant, ok, err := kb.SubjectTenant(r.Context(), subject)
	if err != nil {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not resolve the subject tenant")
		return false
	}
	if !ok || tenant != p.TenantID {
		// Unknown and foreign subjects are indistinguishable.
		writeError(w, requestID, http.StatusNotFound, "not_found", "subject_not_found", "subject not found")
		return false
	}
	return true
}

// handleManagement dispatches the management query surface. The principal
// from the guard supplies the tenant boundary (empty for platform identities)
// and the actor attribution of every audit record.
func handleManagement(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	ctx := r.Context()
	principal, _ := principalFromContext(ctx)
	tenant := principal.TenantID
	requestID := newRequestID()
	switch {
	case r.URL.Path == "/admin/models" && r.Method == http.MethodGet:
		models, err := deps.Mgmt.Models(ctx)
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query models")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"models": models})

	case strings.HasPrefix(r.URL.Path, "/admin/models/") && r.Method == http.MethodPost:
		// /admin/models/{name}/enable | /disable
		rest := strings.TrimPrefix(r.URL.Path, "/admin/models/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 || (parts[1] != "enable" && parts[1] != "disable") {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_path", "use /admin/models/{name}/enable or /disable")
			return
		}
		name, enable := parts[0], parts[1] == "enable"
		// The persisted mutation and its management-audit record commit as one
		// atomic operation: an error here means nothing changed, so a failed
		// operation can never leave a committed mutation reported as failed.
		// The store reads the previous state inside the transaction for the
		// redacted old/new summary.
		detail, _ := json.Marshal(map[string]bool{"enabled": enable})
		err := deps.Mgmt.SetModelEnabledWithAudit(ctx, name, enable, adminOpFrom(r,
			"model_"+map[bool]string{true: "enable", false: "disable"}[enable], name, detail))
		if err != nil {
			if errors.Is(err, mgmt.ErrNotFound) {
				writeError(w, requestID, http.StatusNotFound, "not_found", "model_not_found", "model not found")
				return
			}
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "update_failed", "could not update model")
			return
		}
		// Runtime refresh after persistence. The audit evidence is already
		// committed; a refresh failure is reported as exactly that, so
		// operators know the stored state and the running process diverge.
		if deps.ApplyModelChange != nil {
			if err := deps.ApplyModelChange(ctx, name, enable); err != nil {
				deps.Logger.Error("management runtime refresh failed", "target", name, "error", err)
				writeError(w, requestID, http.StatusInternalServerError, "internal_error", "refresh_failed", "model state saved but the runtime refresh failed")
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"model": name, "status": map[bool]string{true: "enabled", false: "disabled"}[enable]})

	case strings.HasPrefix(r.URL.Path, "/admin/policies/") && strings.HasSuffix(r.URL.Path, "/default-model") && r.Method == http.MethodPost:
		// /admin/policies/{subject}/default-model: body {model, kind}.
		subject := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/policies/"), "/default-model")
		if subject == "" || strings.Contains(subject, "/") {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_path", "use /admin/policies/{subject}/default-model")
			return
		}
		handleSetDefaultModel(w, r, deps, subject)

	case r.URL.Path == "/admin/providers" && r.Method == http.MethodGet:
		providers, err := deps.Mgmt.Providers(ctx)
		if err != nil {
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not query providers")
			return
		}
		// Overlay live breaker state so the view reflects the running process.
		if deps.ProviderRuntime != nil {
			for i := range providers {
				if rt, ok := deps.ProviderRuntime(providers[i].Name); ok {
					providers[i].ApplyRuntime(rt)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"providers": providers})

	case r.URL.Path == "/admin/policies" && r.Method == http.MethodGet:
		policies, err := deps.Mgmt.Policies(ctx, r.URL.Query().Get("subject"), tenant)
		if mapMgmtErr(w, requestID, err) {
			return
		}
		if err != nil {
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not query policies")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policies": policies})

	case r.URL.Path == "/admin/audit" && r.Method == http.MethodGet:
		q := r.URL.Query()
		events, err := deps.Mgmt.QueryAudit(ctx, mgmt.AuditFilter{
			RequestID: q.Get("request_id"), Subject: q.Get("subject"), Model: q.Get("model"),
			From: queryTime(q.Get("from")), To: queryTime(q.Get("to")),
			Limit: queryInt(q.Get("limit")), Tenant: tenant,
		})
		if mapMgmtErr(w, requestID, err) {
			return
		}
		if err != nil {
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not query audit records")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": events})

	case r.URL.Path == "/admin/usage" && r.Method == http.MethodGet:
		q := r.URL.Query()
		rows, err := deps.Mgmt.Usage(ctx, mgmt.AuditFilter{
			Subject: q.Get("subject"), Model: q.Get("model"),
			From: queryTime(q.Get("from")), To: queryTime(q.Get("to")),
			Tenant: tenant,
		})
		if mapMgmtErr(w, requestID, err) {
			return
		}
		if err != nil {
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not query usage")
			return
		}
		body := map[string]any{"usage": rows}
		if budgets, have := budgetUsageSection(deps, r, tenant); have {
			// Wired accounting: current-period budget utilization. Nil stays
			// nil when unwired so the section is detectably absent.
			body["budgets"] = budgets
		}
		writeJSON(w, http.StatusOK, body)

	case r.URL.Path == "/admin/management-log" && r.Method == http.MethodGet:
		ops, err := deps.Mgmt.Ops(ctx, queryInt(r.URL.Query().Get("limit")))
		if err != nil {
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not query management log")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"operations": ops})

	default:
		writeError(w, requestID, http.StatusNotFound, "not_found", "not_found", "unknown management endpoint")
	}
}

// defaultModelBody is the default-model mutation payload: kind selects the
// slot ("chat" → default_model, "embedding" → default_embedding_model).
type defaultModelBody struct {
	Model string `json:"model"`
	Kind  string `json:"kind"`
}

// handleSetDefaultModel applies the subject default-model mutation through
// the atomic mutation + management-audit path (the pattern of the model
// enable/disable switch), refreshing the running policy only after the
// transaction committed. Detail records the slot and model names only; the
// store adds the previous value inside its transaction. A tenant-bound
// operator can only target subjects of its own tenant (non-leaky 404
// otherwise).
func handleSetDefaultModel(w http.ResponseWriter, r *http.Request, deps AdminDeps, subject string) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	var body defaultModelBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil || body.Model == "" || body.Kind == "" {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "model and kind are required")
		return
	}
	if body.Kind != "chat" && body.Kind != "embedding" {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "kind must be chat or embedding")
		return
	}
	detail, _ := json.Marshal(map[string]string{"model": body.Model, "kind": body.Kind})
	err := deps.Mgmt.SetDefaultModelWithAudit(r.Context(), subject, body.Model, body.Kind,
		adminOpFrom(r, "default_model_"+body.Kind, subject, detail), principal.TenantID)
	if err != nil {
		if mapMgmtErr(w, requestID, err) {
			return
		}
		if errors.Is(err, mgmt.ErrNotFound) {
			writeError(w, requestID, http.StatusNotFound, "not_found", "not_found", "subject or model not found")
			return
		}
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "update_failed", "could not update the subject default model")
		return
	}
	// Runtime refresh after persistence; failure semantics mirror the model
	// toggle (refresh_failed with the audit evidence committed).
	if deps.ApplyPolicyChange != nil {
		if err := deps.ApplyPolicyChange(r.Context(), subject); err != nil {
			deps.Logger.Error("management runtime refresh failed", "target", subject, "error", err)
			writeError(w, requestID, http.StatusInternalServerError, "internal_error", "refresh_failed", "default model saved but the runtime refresh failed")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"subject": subject, "kind": body.Kind, "default_model": body.Model})
}

func queryTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return time.Time{}
}

func queryInt(raw string) int {
	n, _ := strconv.Atoi(raw)
	return n
}

type createKeyBody struct {
	Subject     string `json:"subject"`
	TenantID    string `json:"tenant_id"`
	ExpiresInHs int    `json:"expires_in_hours"` // 0 = no expiry
}

// handleCreateKey mints an API key (platform-admin scope). Tenant-bound
// platform-admins may only mint keys for subjects of their own tenant,
// resolved through the authoritative subjects.tenant_id binding, and cannot
// label the key with a foreign tenant (the label books budgets, jobs, and
// settlement — mirroring the admin-credential minting rule). The mutation is
// audit-logged with actor attribution when the management service is wired.
func handleCreateKey(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	var body createKeyBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil || body.Subject == "" {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "subject is required")
		return
	}
	// Declared principal tenant: a tenant-bound creator is confined to its own
	// tenant. The tenant is declared in the body, so the denial is a plain
	// 403 before any store access — nothing about other tenants is probed.
	if !principal.Global() {
		if body.TenantID == "" {
			body.TenantID = principal.TenantID
		}
		if body.TenantID != principal.TenantID {
			adminInsufficientScope(w, requestID)
			return
		}
	}
	if !requireTenantKeys(w, r, deps, body.Subject) {
		return
	}
	var expires time.Time
	if body.ExpiresInHs > 0 {
		expires = deps.Manager.Now().Add(time.Duration(body.ExpiresInHs) * time.Hour)
	}
	gen, err := deps.Manager.Create(r.Context(), body.Subject, body.TenantID, expires)
	if err != nil {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "key_create_failed", "could not create key")
		return
	}
	auditKeyMutation(r, deps, "key_create", gen.Record.ID, map[string]any{
		"subject": gen.Record.Subject, "tenant_id": body.TenantID,
		"prefix": gen.Record.Prefix, "expires_at": orNull(gen.Record.ExpiresAt),
	})
	writeJSON(w, http.StatusCreated, map[string]any{
		"key_id": gen.Record.ID, "key": gen.Plaintext, // one-time plaintext
		"prefix": gen.Record.Prefix, "subject": gen.Record.Subject,
		"expires_at": orNull(gen.Record.ExpiresAt), "created_at": gen.Record.CreatedAt,
	})
}

// auditKeyMutation best-effort records an API-key lifecycle mutation in the
// management audit trail with the caller's actor attribution. Unlike the
// model/default-model/credential mutations this record is written after the
// key change, not in one transaction (the key store and the audit sink are
// separate); key rows are append-only and revocation idempotent, so the
// evidence gap is bounded.
func auditKeyMutation(r *http.Request, deps AdminDeps, action, target string, detail map[string]any) {
	if deps.Mgmt == nil {
		return
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return
	}
	if err := deps.Mgmt.WriteOp(r.Context(), adminOpFrom(r, action, target, raw)); err != nil {
		deps.Logger.Error("management audit write failed", "action", action, "target", target, "error", err)
	}
}

func handleListKeys(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	subject := r.URL.Query().Get("subject")
	if subject == "" {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "subject query parameter is required")
		return
	}
	var (
		recs []auth.KeyRecord
		err  error
	)
	if principal.Global() {
		recs, err = deps.Manager.List(r.Context(), subject)
	} else {
		// Mandatory predicate: the store resolves the key's subject tenant.
		kb := keyBoundary(deps)
		if kb == nil {
			adminInsufficientScope(w, requestID)
			return
		}
		recs, err = kb.ListKeysInTenant(r.Context(), subject, principal.TenantID)
	}
	if err != nil {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "key_list_failed", "could not list keys")
		return
	}
	type meta struct {
		KeyID     string     `json:"key_id"`
		Prefix    string     `json:"prefix"`
		Status    string     `json:"status"`
		ExpiresAt *time.Time `json:"expires_at"`
		CreatedAt time.Time  `json:"created_at"`
		RevokedAt *time.Time `json:"revoked_at"`
	}
	out := make([]meta, 0, len(recs))
	for _, rec := range recs {
		m := meta{KeyID: rec.ID, Prefix: rec.Prefix, Status: string(rec.Status), CreatedAt: rec.CreatedAt}
		if !rec.ExpiresAt.IsZero() {
			m.ExpiresAt = &rec.ExpiresAt
		}
		if !rec.RevokedAt.IsZero() {
			m.RevokedAt = &rec.RevokedAt
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

// requireTenantKeyTarget applies the tenant boundary to key rotate/revoke:
// the target key's subject must live in the caller's tenant. Absent boundary
// → 403; unknown or foreign key → non-leaky 404 (key_not_found matches the
// unknown-key behavior of the global path).
func requireTenantKeyTarget(w http.ResponseWriter, r *http.Request, deps AdminDeps, keyID string) bool {
	p, _ := principalFromContext(r.Context())
	if p.Global() {
		return true
	}
	requestID := newRequestID()
	kb := keyBoundary(deps)
	if kb == nil {
		adminInsufficientScope(w, requestID)
		return false
	}
	tenant, ok, err := kb.KeySubjectTenant(r.Context(), keyID)
	if err != nil {
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "query_failed", "could not resolve the key tenant")
		return false
	}
	if !ok || tenant != p.TenantID {
		writeError(w, requestID, http.StatusNotFound, "not_found", "key_not_found", "key not found")
		return false
	}
	return true
}

func handleRotateKey(w http.ResponseWriter, r *http.Request, deps AdminDeps, keyID string) {
	if !requireTenantKeyTarget(w, r, deps, keyID) {
		return
	}
	requestID := newRequestID()
	gen, old, err := deps.Manager.Rotate(r.Context(), keyID)
	if err != nil {
		if err == auth.ErrNotFound {
			writeError(w, requestID, http.StatusNotFound, "not_found", "key_not_found", "key not found")
			return
		}
		if err == auth.ErrInactive {
			writeError(w, requestID, http.StatusConflict, "invalid_request_error", "key_not_active", "only active keys can be rotated")
			return
		}
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "key_rotate_failed", "could not rotate key")
		return
	}
	auditKeyMutation(r, deps, "key_rotate", gen.Record.ID, map[string]any{
		"rotated_from": old.ID, "subject": old.Subject, "prefix": gen.Record.Prefix,
	})
	writeJSON(w, http.StatusCreated, map[string]any{
		"key_id": gen.Record.ID, "key": gen.Plaintext, // one-time plaintext
		"rotated_from": old.ID, "prefix": gen.Record.Prefix,
	})
}

func handleRevokeKey(w http.ResponseWriter, r *http.Request, deps AdminDeps, keyID string) {
	if !requireTenantKeyTarget(w, r, deps, keyID) {
		return
	}
	requestID := newRequestID()
	err := deps.Manager.Revoke(r.Context(), keyID)
	if err != nil {
		if err == auth.ErrNotFound {
			writeError(w, requestID, http.StatusNotFound, "not_found", "key_not_found", "key not found")
			return
		}
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "key_revoke_failed", "could not revoke key")
		return
	}
	auditKeyMutation(r, deps, "key_revoke", keyID, map[string]any{"status": "revoked"})
	writeJSON(w, http.StatusOK, map[string]any{"key_id": keyID, "status": "revoked"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func orNull(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
