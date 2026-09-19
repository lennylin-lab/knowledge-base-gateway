package httpapi

// V1.4 admin RBAC matrix tests: the route→scope matrix denies every
// unauthorized read/mutation with the uniform 403, tenant-bound identities
// are denied global views and get tenant-predicated answers, credential
// plaintext appears exactly once, and rotation/revocation take effect
// immediately. All offline (in-memory stores).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

// tenantAwareKeyStore wraps the in-memory API-key store with the tenant
// boundary resolved from a subject→tenant map (the development stand-in for
// the authoritative subjects.tenant_id join).
type tenantAwareKeyStore struct {
	*auth.Store
	tenantOf map[string]string
}

func (s *tenantAwareKeyStore) SubjectTenant(_ context.Context, subject string) (string, bool, error) {
	tenant, ok := s.tenantOf[subject]
	return tenant, ok, nil
}

func (s *tenantAwareKeyStore) KeySubjectTenant(_ context.Context, keyID string) (string, bool, error) {
	rec, err := s.Store.Get(context.Background(), keyID)
	if err != nil {
		return "", false, nil
	}
	tenant, ok := s.tenantOf[rec.Subject]
	return tenant, ok, nil
}

func (s *tenantAwareKeyStore) ListKeysInTenant(_ context.Context, subject, tenant string) ([]auth.KeyRecord, error) {
	if t, ok := s.tenantOf[subject]; !ok || t != tenant {
		return nil, nil
	}
	return s.Store.List(context.Background(), subject)
}

type rbacFixture struct {
	mux    *http.ServeMux
	admins *adminauth.MemoryStore
	keys   *tenantAwareKeyStore
	// keyID is a real active API key belonging to subject_home (tenant_home),
	// so rotate/revoke cells exercise real targets.
	keyID string
}

const (
	legacyToken = "root-bootstrap-token"
	tenantEU    = "tenant_eu"
	tenantHome  = "tenant_home"
)

func newRBACFixture(t *testing.T) *rbacFixture {
	t.Helper()
	credStore := adminauth.NewMemoryStore()
	adminAuth := &adminauth.Authenticator{
		Store:       credStore,
		LegacyToken: legacyToken,
		Limiter:     adminauth.NewAuthLimiter(),
		Now:         time.Now,
	}
	keyStore := &tenantAwareKeyStore{
		Store: auth.NewStore(),
		tenantOf: map[string]string{
			"subject_eu":    tenantEU,
			"subject_home":  tenantHome,
			"subject_ghost": tenantEU,
		},
	}
	mgmtSvc := mgmt.NewMemoryService(
		policy.NewCatalog([]policy.ModelInfo{
			{PublicName: "gateway-echo", Provider: "fake", UpstreamModel: "echo", Enabled: true},
		}),
		func() *policy.Policy {
			p := policy.New()
			p.Allow("subject_eu", "gateway-echo")
			p.Allow("subject_home", "gateway-echo")
			return p
		}(),
		nil, audit.NewMemorySink(nil), nil)
	mgmtSvc.TenantOfSubject = func(subject string) (string, bool) {
		tenant, ok := keyStore.tenantOf[subject]
		return tenant, ok
	}
	deps := AdminDeps{
		Token:        legacyToken,
		Manager:      auth.NewManager(keyStore),
		AdminAuth:    adminAuth,
		AdminManager: adminauth.NewManager(credStore),
		Mgmt:         mgmtSvc,
		Accounting:   &accountingAdminStub{},
	}
	// A real active API key for a foreign-tenant subject.
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	plain, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	keyStore.Put(auth.KeyRecord{
		ID: "key_real", Subject: "subject_home", Salt: salt,
		Hash: auth.HashAPIKey(salt, plain), Status: auth.StatusActive,
	})
	fx := &rbacFixture{mux: NewAdminMux(deps), admins: credStore, keys: keyStore, keyID: "key_real"}
	return fx
}

// newKey mints one active API key for the subject and returns its ID, so
// matrix cells never interfere through shared lifecycle targets.
func (f *rbacFixture) newKey(t *testing.T, subject string) string {
	t.Helper()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	plain, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	id := "key_" + subject + "_" + fmt.Sprint(time.Now().UnixNano())
	f.keys.Put(auth.KeyRecord{
		ID: id, Subject: subject, Salt: salt,
		Hash: auth.HashAPIKey(salt, plain), Status: auth.StatusActive,
	})
	return id
}

// issue creates one scoped credential directly in the store and returns its
// plaintext.
func (f *rbacFixture) issue(t *testing.T, subject string, scopes []string, tenant string) string {
	t.Helper()
	mgr := adminauth.NewManager(f.admins)
	mgr.Now = time.Now
	gen, err := mgr.Create(context.Background(), adminauth.CreateInput{
		AdminSubject: subject, Scopes: scopes, TenantID: tenant,
	}, mgmt.AdminOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("issue %s: %v", subject, err)
	}
	return gen.Plaintext
}

func (f *rbacFixture) do(t *testing.T, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

// TestAdminScopeMatrix pins the route→scope matrix: every cell is explicit,
// so adding a route or changing a scope must update this table deliberately.
func TestAdminScopeMatrix(t *testing.T) {
	f := newRBACFixture(t)

	viewer := f.issue(t, "v", []string{"viewer"}, "")
	operator := f.issue(t, "o", []string{"operator"}, "")
	billing := f.issue(t, "b", []string{"billing"}, "")
	platform := f.issue(t, "p", []string{"platform-admin"}, "")
	euViewer := f.issue(t, "ev", []string{"viewer"}, tenantEU)
	euOperator := f.issue(t, "eo", []string{"operator"}, tenantEU)
	euBilling := f.issue(t, "eb", []string{"billing"}, tenantEU)
	euPlatform := f.issue(t, "ep", []string{"platform-admin"}, tenantEU)

	type cell struct {
		token string
		route route
		want  int
	}

	// routesFor builds the route set for one actor; the rotate/revoke cells
	// target a dedicated key owned by subject_home (tenant_home) so global
	// identities exercise real success paths and tenant-bound ones the
	// non-leaky foreign-key answer.
	actorKeys := map[string]string{}
	for _, actor := range []string{legacyToken, viewer, operator, billing, platform, euViewer, euOperator, euBilling, euPlatform} {
		actorKeys[actor] = f.newKey(t, "subject_home")
	}
	routesFor := func(keyID string) []route {
		return []route{
			{http.MethodPost, "/admin/keys", `{"subject":"subject_eu"}`},
			{http.MethodGet, "/admin/keys?subject=subject_eu", ""},
			{http.MethodPost, "/admin/keys/" + keyID + "/rotate", ""},
			{http.MethodPost, "/admin/keys/" + keyID + "/revoke", ""},
			{http.MethodPost, "/admin/admins", `{"admin_subject":"newop","scopes":["viewer"]}`},
			{http.MethodGet, "/admin/admins", ""},
			{http.MethodGet, "/admin/models", ""},
			{http.MethodPost, "/admin/models/gateway-echo/disable", ""},
			{http.MethodGet, "/admin/providers", ""},
			{http.MethodGet, "/admin/policies", ""},
			{http.MethodPost, "/admin/policies/subject_eu/default-model", `{"model":"gateway-echo","kind":"chat"}`},
			{http.MethodGet, "/admin/audit", ""},
			{http.MethodGet, "/admin/usage", ""},
			{http.MethodGet, "/admin/management-log", ""},
			{http.MethodGet, "/admin/prices", ""},
			{http.MethodGet, "/admin/budgets", ""},
		}
	}
	cases := make([]cell, 0, 9*16+9)
	addActor := func(token string, routes []route, want func(r route) int) {
		for _, r := range routes {
			cases = append(cases, cell{token, r, want(r)})
		}
	}

	// Bootstrap: everything allowed.
	addActor(legacyToken, routesFor(actorKeys[legacyToken]), func(r route) int {
		return statusOf(r.method, r.path)
	})
	// Global viewer: reads only (key administration and all mutations denied).
	addActor(viewer, routesFor(actorKeys[viewer]), func(r route) int {
		if isViewerRead(r) {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Global operator: viewer reads plus model toggle and default-model.
	addActor(operator, routesFor(actorKeys[operator]), func(r route) int {
		if isViewerRead(r) || r.path == "/admin/models/gateway-echo/disable" ||
			r.path == "/admin/policies/subject_eu/default-model" {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Global billing: prices and budgets (plus the viewer reads via implication).
	addActor(billing, routesFor(actorKeys[billing]), func(r route) int {
		if isViewerRead(r) || r.path == "/admin/prices" || r.path == "/admin/budgets" {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Global platform-admin: everything.
	addActor(platform, routesFor(actorKeys[platform]), func(r route) int {
		return statusOf(r.method, r.path)
	})
	// Tenant-bound viewer: tenant-predicated reads only; global views denied.
	addActor(euViewer, routesFor(actorKeys[euViewer]), func(r route) int {
		if r.path == "/admin/policies" || r.path == "/admin/audit" || r.path == "/admin/usage" {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Tenant-bound operator: predicated reads plus its tenant's default-model.
	addActor(euOperator, routesFor(actorKeys[euOperator]), func(r route) int {
		if r.path == "/admin/policies" || r.path == "/admin/audit" || r.path == "/admin/usage" ||
			r.path == "/admin/policies/subject_eu/default-model" {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Tenant-bound billing: budgets of its tenant only; global prices denied.
	addActor(euBilling, routesFor(actorKeys[euBilling]), func(r route) int {
		if r.path == "/admin/policies" || r.path == "/admin/audit" || r.path == "/admin/usage" ||
			r.path == "/admin/budgets" {
			return statusOf(r.method, r.path)
		}
		return http.StatusForbidden
	})
	// Tenant-bound platform-admin: tenant-scoped everything. Global resource
	// views stay denied; foreign keys answer the non-leaky 404.
	addActor(euPlatform, routesFor(actorKeys[euPlatform]), func(r route) int {
		switch {
		case r.path == "/admin/models", r.path == "/admin/providers",
			r.path == "/admin/management-log", r.path == "/admin/prices",
			r.path == "/admin/models/gateway-echo/disable":
			return http.StatusForbidden
		case strings.HasPrefix(r.path, "/admin/keys/") && r.method == http.MethodPost:
			return http.StatusNotFound // foreign key: key_not_found
		}
		return statusOf(r.method, r.path)
	})

	for _, c := range cases {
		rec := f.do(t, c.token, c.route.method, c.route.path, c.route.body)
		if rec.Code != c.want {
			t.Errorf("%-16s %-7s %-52s = %d, want %d (%s)",
				actorName(c.token), c.route.method, c.route.path, rec.Code, c.want, rec.Body.String())
		}
	}
}

type route struct {
	method, path, body string
}

// envelopeClass strips the per-request request_id so two responses can be
// compared for equality of their error class.
func envelopeClass(rec *httptest.ResponseRecorder) string {
	var e struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
			Msg  string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	return e.Error.Type + "|" + e.Error.Code + "|" + e.Error.Msg
}

// statusOf is the expected success code per route (global identity, valid
// target, empty body unless the route declares one).
func statusOf(method, path string) int {
	if method != http.MethodPost {
		return http.StatusOK
	}
	switch {
	case path == "/admin/keys", strings.Contains(path, "/rotate"),
		path == "/admin/admins", strings.Contains(path, "/admin/admins/") &&
			strings.Contains(path, "/rotate"):
		return http.StatusCreated
	default:
		return http.StatusOK
	}
}

func isViewerRead(r route) bool {
	switch r.path {
	case "/admin/models", "/admin/providers", "/admin/policies", "/admin/audit",
		"/admin/usage", "/admin/management-log":
		return r.method == http.MethodGet
	}
	return false
}

// actorName maps test tokens back to labels for readable failures.
func actorName(token string) string {
	switch {
	case token == legacyToken:
		return "bootstrap"
	case strings.HasPrefix(token, "v"):
		return "viewer"
	case strings.HasPrefix(token, "o"):
		return "operator"
	case strings.HasPrefix(token, "b"):
		return "billing"
	case strings.HasPrefix(token, "p"):
		return "platform"
	case strings.HasPrefix(token, "e"):
		return "eu-" + map[string]string{"v": "viewer", "o": "operator", "b": "billing", "p": "platform"}[token[1:2]]
	}
	return "unknown"
}

// TestTenantBoundaryNonLeaky pins the enumeration contract: a tenant-bound
// identity probing a foreign subject or key gets the same answer as an
// unknown one, and a foreign default-model target is a plain 404.
func TestTenantBoundaryNonLeaky(t *testing.T) {
	f := newRBACFixture(t)
	euPlatform := f.issue(t, "ep", []string{"platform-admin"}, tenantEU)

	// Unknown subject and foreign subject: identical 404.
	unknown := f.do(t, euPlatform, http.MethodPost, "/admin/keys", `{"subject":"no-such"}`)
	foreign := f.do(t, euPlatform, http.MethodPost, "/admin/keys", `{"subject":"subject_home"}`)
	if unknown.Code != http.StatusNotFound || foreign.Code != http.StatusNotFound {
		t.Fatalf("unknown=%d foreign=%d, both must be 404", unknown.Code, foreign.Code)
	}
	if envelopeClass(unknown) != envelopeClass(foreign) {
		t.Fatalf("unknown and foreign subjects must be indistinguishable: %s vs %s",
			envelopeClass(unknown), envelopeClass(foreign))
	}

	// Default-model on a foreign subject: 404 (operator path).
	euOperator := f.issue(t, "eo", []string{"operator"}, tenantEU)
	foreignModel := f.do(t, euOperator, http.MethodPost, "/admin/policies/subject_home/default-model",
		`{"model":"gateway-echo","kind":"chat"}`)
	if foreignModel.Code != http.StatusNotFound {
		t.Fatalf("foreign default-model target must 404, got %d", foreignModel.Code)
	}

	// A tenant-bound platform-admin cannot label a minted key with a foreign
	// tenant — the principal-tenant label books budgets, jobs, and settlement
	// (declared in the body, so the denial is a plain 403). An empty label is
	// forced to the caller's own tenant.
	if rec := f.do(t, euPlatform, http.MethodPost, "/admin/keys",
		fmt.Sprintf(`{"subject":"subject_eu","tenant_id":%q}`, tenantHome)); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign-tenant key label must 403, got %d", rec.Code)
	}
	rec := f.do(t, euPlatform, http.MethodPost, "/admin/keys", `{"subject":"subject_eu"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("own-tenant key mint: %d %s", rec.Code, rec.Body.String())
	}
	var keyOut struct {
		KeyID string `json:"key_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &keyOut); err != nil {
		t.Fatal(err)
	}
	if labeled, err := f.keys.Get(context.Background(), keyOut.KeyID); err != nil || labeled.TenantID != tenantEU {
		t.Fatalf("empty key label must be forced to the caller's tenant: %+v err=%v", labeled, err)
	}

	// Global views are denied before any store access (no existence leaks).
	euViewer := f.issue(t, "ev", []string{"viewer"}, tenantEU)
	for _, path := range []string{"/admin/models", "/admin/providers", "/admin/management-log", "/admin/prices"} {
		if rec := f.do(t, euViewer, http.MethodGet, path, ""); rec.Code != http.StatusForbidden {
			t.Fatalf("tenant-bound %s must be denied, got %d", path, rec.Code)
		}
	}
}

// TestAdminCredentialEndpoints pins the lifecycle wire behavior: plaintext
// once, redacted listings, rotation revoking the old credential immediately,
// and tenant-bound creators staying inside their tenant.
func TestAdminCredentialEndpoints(t *testing.T) {
	f := newRBACFixture(t)

	// Create: plaintext shown exactly once, never in a listing.
	rec := f.do(t, legacyToken, http.MethodPost, "/admin/admins",
		`{"admin_subject":"ops","scopes":["operator","billing"],"expires_in_hours":24}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created struct {
		CredentialID string   `json:"credential_id"`
		Credential   string   `json:"credential"`
		Scopes       []string `json:"scopes"`
		Prefix       string   `json:"prefix"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.Credential, adminauth.CredentialPrefix) ||
		created.CredentialID == "" || len(created.Scopes) != 2 {
		t.Fatalf("bad create reply: %s", rec.Body.String())
	}
	list := f.do(t, legacyToken, http.MethodGet, "/admin/admins", "")
	if strings.Contains(list.Body.String(), created.Credential) {
		t.Fatal("listing must never contain the plaintext credential")
	}
	if !strings.Contains(list.Body.String(), created.Prefix) {
		t.Fatal("listing carries the display prefix")
	}

	// The credential authenticates (operator implies the viewer read), then
	// rotation kills it immediately.
	rec = f.do(t, created.Credential, http.MethodGet, "/admin/models", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("new credential must authenticate: %d", rec.Code)
	}
	rot := f.do(t, legacyToken, http.MethodPost, "/admin/admins/"+created.CredentialID+"/rotate", "")
	if rot.Code != http.StatusCreated {
		t.Fatalf("rotate: %d %s", rot.Code, rot.Body.String())
	}
	if rec = f.do(t, created.Credential, http.MethodGet, "/admin/models", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("rotated-away credential must fail immediately, got %d", rec.Code)
	}

	// Revoke the successor.
	var rotated struct {
		CredentialID string `json:"credential_id"`
		Credential   string `json:"credential"`
	}
	if err := json.Unmarshal(rot.Body.Bytes(), &rotated); err != nil {
		t.Fatal(err)
	}
	rec = f.do(t, legacyToken, http.MethodPost, "/admin/admins/"+rotated.CredentialID+"/revoke", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("revoke: %d", rec.Code)
	}
	if rec = f.do(t, rotated.Credential, http.MethodGet, "/admin/models", ""); rec.Code != http.StatusUnauthorized {
		t.Fatal("revoked credential must fail immediately")
	}

	// Scope validation is closed-vocabulary.
	rec = f.do(t, legacyToken, http.MethodPost, "/admin/admins", `{"admin_subject":"x","scopes":["root"]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invented scope must 400, got %d", rec.Code)
	}
	rec = f.do(t, legacyToken, http.MethodPost, "/admin/admins", `{"admin_subject":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing scopes must 400, got %d", rec.Code)
	}
}

// TestTenantBoundCredentialMinting pins the downgrade rule: a tenant-bound
// platform-admin cannot mint global or foreign-tenant credentials.
func TestTenantBoundCredentialMinting(t *testing.T) {
	f := newRBACFixture(t)
	euPlatform := f.issue(t, "ep", []string{"platform-admin"}, tenantEU)

	// Explicit foreign tenant: denied.
	if rec := f.do(t, euPlatform, http.MethodPost, "/admin/admins",
		fmt.Sprintf(`{"admin_subject":"x","scopes":["viewer"],"tenant_id":%q}`, tenantHome)); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign-tenant mint must 403, got %d", rec.Code)
	}
	// Empty tenant: forced to the caller's own tenant.
	rec := f.do(t, euPlatform, http.MethodPost, "/admin/admins", `{"admin_subject":"euops","scopes":["viewer"]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("own-tenant mint: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		TenantID string `json:"tenant_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.TenantID != tenantEU {
		t.Fatalf("tenant-bound mint must be scoped to the caller's tenant, got %q", out.TenantID)
	}
	// Listing is predicated: no foreign credentials visible.
	legacyList := f.do(t, legacyToken, http.MethodGet, "/admin/admins", "")
	euList := f.do(t, euPlatform, http.MethodGet, "/admin/admins", "")
	var all, eu struct {
		Credentials []struct {
			TenantID string `json:"tenant_id"`
		} `json:"credentials"`
	}
	_ = json.Unmarshal(euList.Body.Bytes(), &eu)
	for _, c := range eu.Credentials {
		if c.TenantID != tenantEU {
			t.Fatalf("tenant-bound listing leaked a foreign credential: %+v", c)
		}
	}
	_ = json.Unmarshal(legacyList.Body.Bytes(), &all)
	if len(all.Credentials) < len(eu.Credentials) {
		t.Fatal("global listing must be a superset of the tenant listing")
	}
}

// TestAdminAuthenticationAudited pins that successful and failed admin
// authentications land in the management audit trail (bounded bookkeeping —
// polled, never blocking the request path).
func TestAdminAuthenticationAudited(t *testing.T) {
	f := newRBACFixture(t)
	f.do(t, legacyToken, http.MethodGet, "/admin/models", "")     // success
	f.do(t, "totally-wrong", http.MethodGet, "/admin/models", "") // failure
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := f.admins.AuthSnapshot()
		if len(snap) >= 2 {
			success, failure := false, false
			for _, e := range snap {
				if e.Success && e.Principal.Bootstrap {
					success = true
				}
				if !e.Success && e.Reason == "invalid" {
					failure = true
				}
			}
			if success && failure {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("admin authentication audit rows did not land")
}

// TestAdminAuthenticationRateLimited pins the independent 429 on repeated
// failures, without blocking a different client.
func TestAdminAuthenticationRateLimited(t *testing.T) {
	f := newRBACFixture(t)
	for i := 0; i < adminauth.DefaultFailureLimit; i++ {
		if rec := f.do(t, "wrong"+fmt.Sprint(i), http.MethodGet, "/admin/models", ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d must be 401, got %d", i, rec.Code)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/models", nil)
	req.Header.Set("Authorization", "Bearer "+legacyToken)
	// Same client key as the failures above (httptest defaults both to
	// 192.0.2.1): the limiter must trip even for the correct token.
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("after the failure budget even the valid token gets 429, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), legacyToken) {
		t.Fatal("the legacy token must never appear in any response")
	}
}
