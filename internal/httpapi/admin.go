package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
)

// AdminDeps wires the key lifecycle manager behind token-gated endpoints.
type AdminDeps struct {
	Manager *auth.Manager
	Logger  *slog.Logger
	Token   string // GATEWAY_ADMIN_TOKEN; empty disables the endpoints
}

// NewAdminMux builds the management API. Endpoints are disabled (404) unless
// a token is configured; the listener must stay on an internal network.
func NewAdminMux(deps AdminDeps) *http.ServeMux {
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
	return mux
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
