package httpapi

// Tests for GET /v1/models and GET /v1/models/{model}: caller-filtered
// public discovery that never leaks providers, upstream names, or other
// tenants' policies.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

func newModelsFixture(t *testing.T) (*ModelsHandler, string, string) {
	t.Helper()
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{ID: "k1", Subject: "subject-m", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	salt2, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{ID: "k2", Subject: "subject-limited", Salt: salt2, Hash: auth.HashAPIKey(salt2, testKey+"2"), Status: auth.StatusActive})

	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "alpha", Provider: "fake", UpstreamModel: "up-alpha", Enabled: true,
			Capabilities: fullCaps, ConfigVersion: 2},
		{PublicName: "beta", Provider: "fake", UpstreamModel: "up-beta", Enabled: true,
			Capabilities: noResponsesCaps, ConfigVersion: 1},
		{PublicName: "disabled", Provider: "fake", UpstreamModel: "up-x", Enabled: false},
	})
	pol := policy.New()
	pol.AllowAll("subject-m")             // sees alpha + beta
	pol.Allow("subject-limited", "alpha") // sees only alpha
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, time.Second, 0)
	h := &ModelsHandler{Auth: store, Service: svc, Policy: pol}
	return h, testKey, testKey + "2"
}

func doModels(t *testing.T, h *ModelsHandler, path, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestModelsListFilteredByAuthorization(t *testing.T) {
	h, full, limited := newModelsFixture(t)
	rec := doModels(t, h, "/v1/models", full)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var out struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Object != "list" || len(out.Data) != 2 {
		t.Fatalf("data = %+v (disabled models must be hidden)", out.Data)
	}
	if out.Data[0].ID != "alpha" || out.Data[1].ID != "beta" {
		t.Fatalf("ids = %+v", out.Data)
	}
	// The list shape must not carry provider or upstream names.
	if strings.Contains(rec.Body.String(), "fake") || strings.Contains(rec.Body.String(), "up-alpha") {
		t.Fatalf("list leaks provider details: %s", rec.Body.String())
	}

	// The limited subject sees only alpha.
	rec = doModels(t, h, "/v1/models", limited)
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Data) != 1 || out.Data[0].ID != "alpha" {
		t.Fatalf("limited subject sees %+v", out.Data)
	}
}

func TestModelsDetailPublicFieldsOnly(t *testing.T) {
	h, full, _ := newModelsFixture(t)
	rec := doModels(t, h, "/v1/models/alpha", full)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var d modelDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.ID != "alpha" || d.Object != "model" || d.Status != "enabled" {
		t.Errorf("detail header wrong: %+v", d)
	}
	if d.ConfigVersion != 2 {
		t.Errorf("config version = %d", d.ConfigVersion)
	}
	if len(d.Protocols) != 2 {
		t.Errorf("protocols = %v", d.Protocols)
	}
	if !d.Capabilities.Tools || !d.Capabilities.Responses || d.Capabilities.MaxOutputTokens != 2048 {
		t.Errorf("capabilities = %+v", d.Capabilities)
	}
	for _, leaked := range []string{"fake", "up-alpha", "upstream"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Errorf("detail leaks %q: %s", leaked, rec.Body.String())
		}
	}
	// beta declares responses:false so protocols must exclude it.
	rec = doModels(t, h, "/v1/models/beta", full)
	var d2 modelDetail
	_ = json.Unmarshal(rec.Body.Bytes(), &d2)
	if len(d2.Protocols) != 1 || d2.Protocols[0] != "chat" {
		t.Errorf("beta protocols = %v", d2.Protocols)
	}
}

func TestModelsUnknownAndUnauthorizedNonLeaky(t *testing.T) {
	h, _, limited := newModelsFixture(t)
	// Unknown model.
	rec := doModels(t, h, "/v1/models/nope", testKey)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("unknown model: %d %s", rec.Code, rec.Body.String())
	}
	// Disabled catalog entry: identical envelope.
	rec = doModels(t, h, "/v1/models/disabled", testKey)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("disabled model: %d %s", rec.Code, rec.Body.String())
	}
	// Known but unauthorized: identical envelope, and the limited subject
	// cannot distinguish existence.
	rec = doModels(t, h, "/v1/models/beta", limited)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("unauthorized model: %d", rec.Code)
	}
	if strings.Contains(strings.ToLower(rec.Body.String()), "policy") || strings.Contains(rec.Body.String(), "exists") {
		t.Fatalf("error leaks authorization details: %s", rec.Body.String())
	}
}

func TestModelsAuthRequired(t *testing.T) {
	h, _, _ := newModelsFixture(t)
	rec := doModels(t, h, "/v1/models", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing key: %d", rec.Code)
	}
	rec = doModels(t, h, "/v1/models", "sk-wrong")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad key: %d", rec.Code)
	}
}

func TestModelsMethodGuard(t *testing.T) {
	h, _, _ := newModelsFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/models", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
}
