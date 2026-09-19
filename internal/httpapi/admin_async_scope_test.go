package httpapi

// V1.4 scoped-admin visibility on background responses: the contained
// scope-check seam in AsyncJobsHandler.loadOwnedJob. Owners keep the
// owner-only view; admin credentials with the viewer scope gain GET
// visibility within their tenant reach; cancellation stays owner-only; and
// admin states stay non-enumerable on the /v1 surface.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

type asyncAdminFixture struct {
	mux      *http.ServeMux
	jobs     *async.MemoryStore
	creds    *adminauth.MemoryStore
	apiStore *auth.Store
}

func newAsyncAdminFixture(t *testing.T) *asyncAdminFixture {
	t.Helper()
	apiStore := auth.NewStore()
	creds := adminauth.NewMemoryStore()
	adminAuth := &adminauth.Authenticator{Store: creds, Limiter: adminauth.NewAuthLimiter(), Now: time.Now}
	jobs := async.NewMemoryStore(nil)
	handler := &AsyncJobsHandler{
		Auth: apiStore, Jobs: jobs, Audit: audit.NewMemorySink(nil),
		Metrics: metrics.New(), AdminAuth: adminAuth, PollHint: time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/responses/{id}", handler.ServeHTTP)
	mux.HandleFunc("POST /v1/responses/{id}/cancel", handler.ServeHTTP)
	return &asyncAdminFixture{mux: mux, jobs: jobs, creds: creds, apiStore: apiStore}
}

func (f *asyncAdminFixture) seedJob(t *testing.T, id, subject, tenant string) {
	t.Helper()
	_, err := f.jobs.Create(context.Background(), async.CreateInput{
		JobID: id, SubjectID: subject, TenantID: tenant,
		Protocol: "responses", PublicModel: "gateway-echo",
		RequestDigest: "digest-" + id, Request: []byte(`{"model":"gateway-echo"}`),
		Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

func (f *asyncAdminFixture) apiKey(t *testing.T, id, subject string) string {
	t.Helper()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	plain, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	f.apiStore.Put(auth.KeyRecord{ID: id, Subject: subject, Salt: salt,
		Hash: auth.HashAPIKey(salt, plain), Status: auth.StatusActive})
	return plain
}

func (f *asyncAdminFixture) credential(t *testing.T, subject string, scopes []string, tenant string) string {
	t.Helper()
	mgr := adminauth.NewManager(f.creds)
	gen, err := mgr.Create(context.Background(), adminauth.CreateInput{
		AdminSubject: subject, Scopes: scopes, TenantID: tenant,
	}, mgmt.AdminOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("issue credential: %v", err)
	}
	return gen.Plaintext
}

func (f *asyncAdminFixture) do(t *testing.T, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

func TestAsyncScopedAdminGet(t *testing.T) {
	f := newAsyncAdminFixture(t)
	f.seedJob(t, "resp_eu", "subject-r", "tenant_eu")
	f.seedJob(t, "resp_home", "subject-other", "tenant_home")

	owner := f.apiKey(t, "key_r", "subject-r")
	stranger := f.apiKey(t, "key_s", "subject-nobody")
	globalViewer := f.credential(t, "adm-view", []string{"viewer"}, "")
	tenantViewer := f.credential(t, "adm-eu-view", []string{"viewer"}, "tenant_eu")
	billing := f.credential(t, "adm-bill", []string{"billing"}, "")

	// Owner sees the job; a stranger gets the non-leaky 404.
	if rec := f.do(t, http.MethodGet, "/v1/responses/resp_eu", owner); rec.Code != http.StatusOK {
		t.Fatalf("owner GET: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.do(t, http.MethodGet, "/v1/responses/resp_eu", stranger); rec.Code != http.StatusNotFound {
		t.Fatalf("stranger GET must be 404, got %d", rec.Code)
	}

	// Global viewer admin sees any job.
	for _, id := range []string{"resp_eu", "resp_home"} {
		if rec := f.do(t, http.MethodGet, "/v1/responses/"+id, globalViewer); rec.Code != http.StatusOK {
			t.Fatalf("global viewer admin GET %s: %d", id, rec.Code)
		}
	}

	// Tenant-bound viewer admin sees only its tenant's jobs, with the same
	// non-leaky envelope for the foreign tenant.
	if rec := f.do(t, http.MethodGet, "/v1/responses/resp_eu", tenantViewer); rec.Code != http.StatusOK {
		t.Fatalf("tenant viewer admin GET own tenant: %d", rec.Code)
	}
	rec := f.do(t, http.MethodGet, "/v1/responses/resp_home", tenantViewer)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant viewer admin GET foreign tenant must be 404, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "response_not_found") {
		t.Fatalf("tenant denial must reuse the non-leaky envelope: %s", rec.Body.String())
	}

	// Every scope in the vocabulary implies viewer (one owner of the
	// implication rules), so a billing admin legitimately holds read
	// visibility — pinned here so a future scope addition cannot silently
	// widen or narrow job visibility without revisiting this contract.
	if rec := f.do(t, http.MethodGet, "/v1/responses/resp_eu", billing); rec.Code != http.StatusOK {
		t.Fatalf("billing (viewer-implied) GET: %d", rec.Code)
	}

	// Admin credentials cannot cancel: visibility is not control.
	cancelRec := f.do(t, http.MethodPost, "/v1/responses/resp_eu/cancel", globalViewer)
	if cancelRec.Code != http.StatusForbidden {
		t.Fatalf("admin cancel must be 403, got %d", cancelRec.Code)
	}
	// Owner cancel still works.
	if rec := f.do(t, http.MethodPost, "/v1/responses/resp_eu/cancel", owner); rec.Code != http.StatusOK {
		t.Fatalf("owner cancel: %d", rec.Code)
	}

	// Invalid admin credential: uniform 401, indistinguishable from a bad
	// API key.
	if rec := f.do(t, http.MethodGet, "/v1/responses/resp_eu", adminauth.CredentialPrefix+"ffffffff"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid admin credential must be 401, got %d", rec.Code)
	}

	// Without the admin authenticator wired the admin path stays closed.
	bare := &AsyncJobsHandler{Auth: f.apiStore, Jobs: f.jobs, Metrics: metrics.New()}
	req := httptest.NewRequest(http.MethodGet, "/v1/responses/resp_eu", nil)
	req.Header.Set("Authorization", "Bearer "+adminauth.CredentialPrefix+"ffffffff")
	bareRec := httptest.NewRecorder()
	bareMux := http.NewServeMux()
	bareMux.HandleFunc("GET /v1/responses/{id}", bare.ServeHTTP)
	bareMux.ServeHTTP(bareRec, req)
	if bareRec.Code != http.StatusUnauthorized {
		t.Fatalf("admin credential without wiring must be 401, got %d", bareRec.Code)
	}
}
