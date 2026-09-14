package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
)

func adminServer(t *testing.T) (*httptest.Server, *auth.Store) {
	t.Helper()
	store := auth.NewStore()
	deps := AdminDeps{Manager: auth.NewManager(store), Token: "admin-secret"}
	return httptest.NewServer(NewAdminMux(deps)), store
}

func adminReq(t *testing.T, srv *httptest.Server, method, path string, body any, token string) (*http.Response, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, srv.URL+path, rd)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestAdminDisabledWithoutToken(t *testing.T) {
	srv := httptest.NewServer(NewAdminMux(AdminDeps{Manager: auth.NewManager(auth.NewStore())}))
	defer srv.Close()
	resp, _ := adminReq(t, srv, http.MethodPost, "/admin/keys", createKeyBody{Subject: "s"}, "anything")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("admin must be disabled without token, got %d", resp.StatusCode)
	}
}

func TestAdminKeyLifecycle(t *testing.T) {
	srv, _ := adminServer(t)
	defer srv.Close()

	// Wrong token rejected.
	resp, _ := adminReq(t, srv, http.MethodPost, "/admin/keys", createKeyBody{Subject: "svc"}, "nope")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 for bad admin token, got %d", resp.StatusCode)
	}

	// Create: plaintext shown exactly once.
	resp, body := adminReq(t, srv, http.MethodPost, "/admin/keys",
		createKeyBody{Subject: "svc", TenantID: "tenant_default", ExpiresInHs: 24}, "admin-secret")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create failed: %d %v", resp.StatusCode, body)
	}
	plain, _ := body["key"].(string)
	keyID, _ := body["key_id"].(string)
	if !strings.HasPrefix(plain, "kb_") || keyID == "" {
		t.Fatalf("bad create response: %v", body)
	}

	// List: metadata only, no plaintext or hash material.
	resp, body = adminReq(t, srv, http.MethodGet, "/admin/keys?subject=svc", nil, "admin-secret")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list failed: %d", resp.StatusCode)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), plain) {
		t.Fatal("listing must not include the plaintext key")
	}

	// Rotate: new plaintext, old revoked.
	resp, body = adminReq(t, srv, http.MethodPost, "/admin/keys/"+keyID+"/rotate", nil, "admin-secret")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rotate failed: %d %v", resp.StatusCode, body)
	}
	newPlain, _ := body["key"].(string)
	if newPlain == plain {
		t.Fatal("rotation must produce a new plaintext key")
	}

	// Revoke the replacement.
	resp, _ = adminReq(t, srv, http.MethodPost, "/admin/keys/"+keyID+"/revoke", nil, "admin-secret")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("revoke failed: %d", resp.StatusCode)
	}

	// Lifecycle errors: rotating a revoked key conflicts; unknown key 404s.
	resp, _ = adminReq(t, srv, http.MethodPost, "/admin/keys/"+keyID+"/rotate", nil, "admin-secret")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("rotating revoked key should conflict, got %d", resp.StatusCode)
	}
	resp, _ = adminReq(t, srv, http.MethodPost, "/admin/keys/key_missing/revoke", nil, "admin-secret")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown key should 404, got %d", resp.StatusCode)
	}
	_ = time.Now
}
