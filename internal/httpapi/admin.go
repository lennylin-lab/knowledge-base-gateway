package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// AdminDeps wires the key lifecycle manager and the management query service
// behind token-gated endpoints.
type AdminDeps struct {
	Manager *auth.Manager
	Logger  *slog.Logger
	Token   string // GATEWAY_ADMIN_TOKEN; empty disables the endpoints
	Mgmt    mgmt.Service
	// ApplyModelChange is the runtime refresh boundary for the model
	// enable/disable switch. It is invoked after the persisted mutation and
	// its audit record committed atomically, and must make the change visible
	// to the running process (catalog and route tables) without a restart.
	// A returned error is reported as refresh_failed: the change stays
	// persisted and audited, and the response makes clear the refresh — not
	// the mutation — failed.
	ApplyModelChange func(ctx context.Context, publicModel string, enabled bool) error
	// ProviderRuntime reports live per-provider breaker state so
	// /admin/providers reflects the running process instead of the registry
	// alone. A false ok leaves the store-reported fields untouched.
	ProviderRuntime func(name string) (mgmt.ProviderRuntime, bool)
}

// NewAdminMux builds the management API. Endpoints are disabled (404) unless
// a token is configured; the listener must stay on an internal network.
// Management queries (when Mgmt is wired) are separately authenticated with
// the same token and every mutating operation appends a management-audit
// record.
func NewAdminMux(deps AdminDeps) *http.ServeMux {
	// Management failure paths log; tests build deps without a logger, so
	// normalize once instead of guarding every log site.
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	guard := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if deps.Token == "" {
				writeError(w, newRequestID(), http.StatusNotFound, "not_found", "admin_disabled", "admin API is not enabled")
				return
			}
			const prefix = "Bearer "
			got := r.Header.Get("Authorization")
			if !strings.HasPrefix(got, prefix) || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got[len(prefix):])), []byte(deps.Token)) != 1 {
				writeError(w, newRequestID(), http.StatusUnauthorized, "authentication_error", "invalid_admin_token", "invalid admin token")
				return
			}
			next(w, r)
		}
	}

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

	// V1.2 management queries: read-only operations plus the model
	// enable/disable switch, all management-audited.
	if deps.Mgmt != nil {
		mgmtGuard := guard(func(w http.ResponseWriter, r *http.Request) {
			// Every request is already token-authenticated; keep a hook here
			// for future per-admin authorization policies.
			handleManagement(w, r, deps)
		})
		mux.HandleFunc("/admin/models", mgmtGuard)
		mux.HandleFunc("/admin/models/", mgmtGuard)
		mux.HandleFunc("/admin/providers", mgmtGuard)
		mux.HandleFunc("/admin/policies", mgmtGuard)
		mux.HandleFunc("/admin/audit", mgmtGuard)
		mux.HandleFunc("/admin/usage", mgmtGuard)
		mux.HandleFunc("/admin/management-log", mgmtGuard)
	}
	return mux
}

// handleManagement dispatches the management query surface.
func handleManagement(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	ctx := r.Context()
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
			writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_path", "use /admin/models/{name}/enable or /disable")
			return
		}
		name, enable := parts[0], parts[1] == "enable"
		// The persisted mutation and its management-audit record commit as one
		// atomic operation: an error here means nothing changed, so a failed
		// operation can never leave a committed mutation reported as failed.
		detail, _ := json.Marshal(map[string]bool{"enabled": enable})
		err := deps.Mgmt.SetModelEnabledWithAudit(ctx, name, enable, mgmt.AdminOp{
			CreatedAt: time.Now(), Action: "model_" + map[bool]string{true: "enable", false: "disable"}[enable],
			Target: name, Detail: detail,
		})
		if err != nil {
			if errors.Is(err, mgmt.ErrNotFound) {
				writeError(w, newRequestID(), http.StatusNotFound, "not_found", "model_not_found", "model not found")
				return
			}
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "update_failed", "could not update model")
			return
		}
		// Runtime refresh after persistence. The audit evidence is already
		// committed; a refresh failure is reported as exactly that, so
		// operators know the stored state and the running process diverge.
		if deps.ApplyModelChange != nil {
			if err := deps.ApplyModelChange(ctx, name, enable); err != nil {
				deps.Logger.Error("management runtime refresh failed", "target", name, "error", err)
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "refresh_failed", "model state saved but the runtime refresh failed")
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"model": name, "status": map[bool]string{true: "enabled", false: "disabled"}[enable]})

	case r.URL.Path == "/admin/providers" && r.Method == http.MethodGet:
		providers, err := deps.Mgmt.Providers(ctx)
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query providers")
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
		policies, err := deps.Mgmt.Policies(ctx, r.URL.Query().Get("subject"))
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query policies")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policies": policies})

	case r.URL.Path == "/admin/audit" && r.Method == http.MethodGet:
		q := r.URL.Query()
		events, err := deps.Mgmt.QueryAudit(ctx, mgmt.AuditFilter{
			RequestID: q.Get("request_id"), Subject: q.Get("subject"), Model: q.Get("model"),
			From: queryTime(q.Get("from")), To: queryTime(q.Get("to")),
			Limit: queryInt(q.Get("limit")),
		})
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query audit records")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": events})

	case r.URL.Path == "/admin/usage" && r.Method == http.MethodGet:
		q := r.URL.Query()
		rows, err := deps.Mgmt.Usage(ctx, mgmt.AuditFilter{
			Subject: q.Get("subject"), Model: q.Get("model"),
			From: queryTime(q.Get("from")), To: queryTime(q.Get("to")),
		})
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query usage")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"usage": rows})

	case r.URL.Path == "/admin/management-log" && r.Method == http.MethodGet:
		ops, err := deps.Mgmt.Ops(ctx, queryInt(r.URL.Query().Get("limit")))
		if err != nil {
			writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query management log")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"operations": ops})

	default:
		writeError(w, newRequestID(), http.StatusNotFound, "not_found", "not_found", "unknown management endpoint")
	}
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

func handleCreateKey(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	var body createKeyBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil || body.Subject == "" {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "subject is required")
		return
	}
	var expires time.Time
	if body.ExpiresInHs > 0 {
		expires = deps.Manager.Now().Add(time.Duration(body.ExpiresInHs) * time.Hour)
	}
	gen, err := deps.Manager.Create(r.Context(), body.Subject, body.TenantID, expires)
	if err != nil {
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "key_create_failed", "could not create key")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"key_id": gen.Record.ID, "key": gen.Plaintext, // one-time plaintext
		"prefix": gen.Record.Prefix, "subject": gen.Record.Subject,
		"expires_at": orNull(gen.Record.ExpiresAt), "created_at": gen.Record.CreatedAt,
	})
}

func handleListKeys(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	subject := r.URL.Query().Get("subject")
	if subject == "" {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "subject query parameter is required")
		return
	}
	recs, err := deps.Manager.List(r.Context(), subject)
	if err != nil {
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "key_list_failed", "could not list keys")
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

func handleRotateKey(w http.ResponseWriter, r *http.Request, deps AdminDeps, keyID string) {
	gen, old, err := deps.Manager.Rotate(r.Context(), keyID)
	if err != nil {
		if err == auth.ErrNotFound {
			writeError(w, newRequestID(), http.StatusNotFound, "not_found", "key_not_found", "key not found")
			return
		}
		if err == auth.ErrInactive {
			writeError(w, newRequestID(), http.StatusConflict, "invalid_request_error", "key_not_active", "only active keys can be rotated")
			return
		}
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "key_rotate_failed", "could not rotate key")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"key_id": gen.Record.ID, "key": gen.Plaintext, // one-time plaintext
		"rotated_from": old.ID, "prefix": gen.Record.Prefix,
	})
}

func handleRevokeKey(w http.ResponseWriter, r *http.Request, deps AdminDeps, keyID string) {
	err := deps.Manager.Revoke(r.Context(), keyID)
	if err != nil {
		if err == auth.ErrNotFound {
			writeError(w, newRequestID(), http.StatusNotFound, "not_found", "key_not_found", "key not found")
			return
		}
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "key_revoke_failed", "could not revoke key")
		return
	}
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
