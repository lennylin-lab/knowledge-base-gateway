package httpapi

// V1.4 lifecycle admin surface tests: the scope-matrix cells for
// /admin/lifecycle/* and /admin/exports, the tenant boundary on exports
// (mandatory predicate + the never-tenant-exportable management log), and
// the unwired 404 rollback posture. All offline with a stub LifecycleAdmin.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// lifecycleStub is a scriptable LifecycleAdmin recording the calls handlers
// make.
type lifecycleStub struct {
	policies    []lifecycle.Policy
	runs        []lifecycle.RunView
	results     []lifecycle.TableResult
	upserts     []lifecycle.PolicyInput
	runOpts     []lifecycle.Options
	exports     []lifecycle.ExportFilter
	seenTenant  string
	lastSummary lifecycle.ExportSummary
}

func (s *lifecycleStub) Policies(context.Context) ([]lifecycle.Policy, error) {
	return s.policies, nil
}

func (s *lifecycleStub) UpsertPolicy(_ context.Context, in lifecycle.PolicyInput, _ mgmt.AdminOp) error {
	s.upserts = append(s.upserts, in)
	return nil
}

func (s *lifecycleStub) Runs(context.Context, int) ([]lifecycle.RunView, error) {
	return s.runs, nil
}

func (s *lifecycleStub) StartRun(_ context.Context, opts lifecycle.Options, _ mgmt.AdminOp) ([]lifecycle.TableResult, error) {
	s.runOpts = append(s.runOpts, opts)
	return s.results, nil
}

func (s *lifecycleStub) BeginExport(_ context.Context, in lifecycle.ExportRequest) (string, error) {
	s.exports = append(s.exports, in.Filter)
	if in.Filter.Tenant == "tenant_ghost" {
		return "", lifecycle.ErrUnknownTenant
	}
	return "exp_stub", nil
}

func (s *lifecycleStub) StreamExport(_ context.Context, _ io.Writer, _ string, _ lifecycle.ExportRequest) (lifecycle.ExportSummary, error) {
	s.lastSummary = lifecycle.ExportSummary{Rows: 1, SHA256: "aa", Complete: true}
	return s.lastSummary, nil
}

func (s *lifecycleStub) Exports(_ context.Context, tenant string, _ int) ([]lifecycle.ExportView, error) {
	s.seenTenant = tenant
	return []lifecycle.ExportView{}, nil
}

type lifecycleFixture struct {
	mux   *http.ServeMux
	stub  *lifecycleStub
	creds *adminauth.MemoryStore
}

func newLifecycleFixture(t *testing.T, withLifecycle bool) *lifecycleFixture {
	t.Helper()
	creds := adminauth.NewMemoryStore()
	adminAuth := &adminauth.Authenticator{
		Store: creds, LegacyToken: legacyToken,
		Limiter: adminauth.NewAuthLimiter(), Now: time.Now,
	}
	deps := AdminDeps{
		Token: legacyToken, AdminAuth: adminAuth,
		AdminManager: adminauth.NewManager(creds),
	}
	f := &lifecycleFixture{creds: creds}
	if withLifecycle {
		f.stub = &lifecycleStub{
			policies: []lifecycle.Policy{{Table: lifecycle.TableRequests, TTL: 3600, Enabled: true}},
			runs:     []lifecycle.RunView{{ID: 1, Table: lifecycle.TableRequests, Status: "completed"}},
			results:  []lifecycle.TableResult{{Table: lifecycle.TableRequests, Status: "completed", Deleted: 2}},
		}
		deps.Lifecycle = f.stub
	}
	f.mux = NewAdminMux(deps)
	return f
}

func (f *lifecycleFixture) credential(t *testing.T, subject string, scopes []string, tenant string) string {
	t.Helper()
	gen, err := adminauth.NewManager(f.creds).Create(context.Background(), adminauth.CreateInput{
		AdminSubject: subject, Scopes: scopes, TenantID: tenant,
	}, mgmt.AdminOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("issue credential: %v", err)
	}
	return gen.Plaintext
}

func (f *lifecycleFixture) do(t *testing.T, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

func TestLifecycleScopeMatrix(t *testing.T) {
	f := newLifecycleFixture(t, true)
	globalViewer := f.credential(t, "adm-view", []string{"viewer"}, "")
	tenantViewer := f.credential(t, "adm-eu-view", []string{"viewer"}, "tenant_eu")
	platform := legacyToken
	tenantPlatform := f.credential(t, "adm-eu-root", []string{"platform-admin"}, "tenant_eu")

	// Policy views: viewer + global-only.
	if rec := f.do(t, http.MethodGet, "/admin/lifecycle/policies", globalViewer, ""); rec.Code != http.StatusOK {
		t.Fatalf("global viewer policies: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.do(t, http.MethodGet, "/admin/lifecycle/policies", tenantViewer, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("tenant viewer policies must be 403 (global-only view), got %d", rec.Code)
	}

	// Policy mutation: platform-admin + global-only.
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/policies", globalViewer,
		`{"table":"usage_ledger","ttl_seconds":86400}`); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer policy mutation must be 403, got %d", rec.Code)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/policies", tenantPlatform,
		`{"table":"usage_ledger","ttl_seconds":86400}`); rec.Code != http.StatusForbidden {
		t.Fatalf("tenant platform-admin policy mutation must be 403 (global-only), got %d", rec.Code)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/policies", platform,
		`{"table":"usage_ledger","ttl_seconds":86400}`); rec.Code != http.StatusOK {
		t.Fatalf("platform policy mutation: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/policies", platform,
		`{"table":"tenants","ttl_seconds":86400}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("ungoverned table must be 400, got %d", rec.Code)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/policies", platform,
		`{"table":"usage_ledger","ttl_seconds":0}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("zero TTL must be 400, got %d", rec.Code)
	}

	// Run history: viewer + global-only; triggering: platform-admin only.
	if rec := f.do(t, http.MethodGet, "/admin/lifecycle/runs", globalViewer, ""); rec.Code != http.StatusOK {
		t.Fatalf("global viewer runs: %d", rec.Code)
	}
	if rec := f.do(t, http.MethodGet, "/admin/lifecycle/runs", tenantViewer, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("tenant viewer runs must be 403, got %d", rec.Code)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/runs", globalViewer, `{"dry_run":true}`); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer run trigger must be 403, got %d", rec.Code)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/runs", platform, `{"dry_run":true}`); rec.Code != http.StatusOK {
		t.Fatalf("platform run trigger: %d %s", rec.Code, rec.Body.String())
	}
	if len(f.stub.runOpts) != 1 || !f.stub.runOpts[0].DryRun {
		t.Fatalf("run options = %+v", f.stub.runOpts)
	}
	if rec := f.do(t, http.MethodPost, "/admin/lifecycle/runs", platform, `{"table":"nope"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown run table must be 400, got %d", rec.Code)
	}
}

func TestExportScopeAndTenantBoundary(t *testing.T) {
	f := newLifecycleFixture(t, true)
	globalViewer := f.credential(t, "adm-view", []string{"viewer"}, "")
	billing := f.credential(t, "adm-bill", []string{"billing"}, "")
	tenantViewer := f.credential(t, "adm-eu-view", []string{"viewer"}, "tenant_eu")

	// Exports are viewer-scope (billing implies viewer: usage exports).
	body := `{"subject":"s1","max_rows":10}`
	if rec := f.do(t, http.MethodPost, "/admin/exports", billing, body); rec.Code != http.StatusOK {
		t.Fatalf("billing export: %d %s", rec.Code, rec.Body.String())
	}

	// The tenant boundary is a mandatory predicate: a tenant-bound caller's
	// declared tenant is overridden with its own.
	rec := f.do(t, http.MethodPost, "/admin/exports", tenantViewer, `{"tenant":"tenant_other"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("tenant export: %d %s", rec.Code, rec.Body.String())
	}
	if len(f.stub.exports) == 0 || f.stub.exports[len(f.stub.exports)-1].Tenant != "tenant_eu" {
		t.Fatalf("tenant filter must be forced to the caller's tenant: %+v", f.stub.exports)
	}

	// The management log is platform scope and never tenant-exportable.
	rec = f.do(t, http.MethodPost, "/admin/exports", tenantViewer, `{"include_management_log":true}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("tenant management-log export must be 403, got %d", rec.Code)
	}
	// A platform-global caller may include it.
	rec = f.do(t, http.MethodPost, "/admin/exports", globalViewer, `{"include_management_log":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("global management-log export: %d %s", rec.Code, rec.Body.String())
	}
	if len(f.stub.exports) == 0 || !f.stub.exports[len(f.stub.exports)-1].IncludeManagementLog {
		t.Fatalf("management-log flag must reach the service: %+v", f.stub.exports)
	}

	// Invalid time bound: 400.
	if rec := f.do(t, http.MethodPost, "/admin/exports", globalViewer, `{"from":"yesterday"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad from must be 400, got %d", rec.Code)
	}

	// Unknown tenant: 400 before any artifact bytes are committed.
	rec = f.do(t, http.MethodPost, "/admin/exports", globalViewer, `{"tenant":"tenant_ghost"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown tenant must be 400, got %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unknown_tenant") {
		t.Fatalf("unknown tenant error code missing: %s", rec.Body.String())
	}

	// Export records listing is tenant-predicated.
	rec = f.do(t, http.MethodGet, "/admin/exports", tenantViewer, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("tenant export list: %d", rec.Code)
	}
	if f.stub.seenTenant != "tenant_eu" {
		t.Fatalf("list tenant predicate = %q", f.stub.seenTenant)
	}
}

func TestExportArtifactIsVerifiableNDJSON(t *testing.T) {
	// The stub writes a real artifact through the same framing contract the
	// domain service uses; the checksum covers exactly the record lines.
	f := newLifecycleFixture(t, true)
	f.stub.lastSummary = func() lifecycle.ExportSummary {
		rec, _ := lifecycle.EncodeRecord(lifecycle.RecordRequest, map[string]any{"id": "r1"})
		sum := sha256.Sum256(rec)
		return lifecycle.ExportSummary{Rows: 1, SHA256: hex.EncodeToString(sum[:]), Complete: true}
	}()
	// Wrap the stub so Export writes real bytes.
	deps := AdminDeps{Token: legacyToken, Lifecycle: &writingStub{stub: f.stub}}
	f.mux = NewAdminMux(deps)

	rec := f.do(t, http.MethodPost, "/admin/exports", legacyToken, `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("export: %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("content type = %q", ct)
	}
	lines := strings.Split(strings.TrimSuffix(rec.Body.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("artifact lines = %d, want header+record+summary", len(lines))
	}
	var header map[string]map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatal(err)
	}
	if _, ok := header["export"]["id"]; !ok {
		t.Fatal("header must carry the export id")
	}
	var summary struct {
		Summary lifecycle.ExportSummary `json:"summary"`
	}
	if err := json.Unmarshal([]byte(lines[2]), &summary); err != nil {
		t.Fatal(err)
	}
	if !summary.Summary.Complete || summary.Summary.Rows != 1 {
		t.Fatalf("summary = %+v", summary.Summary)
	}
}

// writingStub delegates to a lifecycleStub but writes a real NDJSON artifact.
type writingStub struct{ stub *lifecycleStub }

func (w *writingStub) Policies(ctx context.Context) ([]lifecycle.Policy, error) {
	return w.stub.Policies(ctx)
}
func (w *writingStub) UpsertPolicy(ctx context.Context, in lifecycle.PolicyInput, op mgmt.AdminOp) error {
	return w.stub.UpsertPolicy(ctx, in, op)
}
func (w *writingStub) Runs(ctx context.Context, limit int) ([]lifecycle.RunView, error) {
	return w.stub.Runs(ctx, limit)
}
func (w *writingStub) StartRun(ctx context.Context, opts lifecycle.Options, op mgmt.AdminOp) ([]lifecycle.TableResult, error) {
	return w.stub.StartRun(ctx, opts, op)
}
func (w *writingStub) Exports(ctx context.Context, tenant string, limit int) ([]lifecycle.ExportView, error) {
	return w.stub.Exports(ctx, tenant, limit)
}

func (w *writingStub) BeginExport(_ context.Context, in lifecycle.ExportRequest) (string, error) {
	w.stub.exports = append(w.stub.exports, in.Filter)
	return "exp_test", nil
}

func (w *writingStub) StreamExport(_ context.Context, out io.Writer, _ string, _ lifecycle.ExportRequest) (lifecycle.ExportSummary, error) {
	rec, err := lifecycle.EncodeRecord(lifecycle.RecordRequest, map[string]any{"id": "r1"})
	if err != nil {
		return lifecycle.ExportSummary{}, err
	}
	sum := sha256.Sum256(rec)
	header, _ := json.Marshal(map[string]any{"export": map[string]any{"id": "exp_test", "requested_by": "platform"}})
	if _, err := out.Write(append(header, '\n')); err != nil {
		return lifecycle.ExportSummary{}, err
	}
	if _, err := out.Write(rec); err != nil {
		return lifecycle.ExportSummary{}, err
	}
	s := lifecycle.ExportSummary{Rows: 1, SHA256: hex.EncodeToString(sum[:]), Complete: true}
	raw, _ := json.Marshal(map[string]any{"summary": s})
	if _, err := out.Write(append(raw, '\n')); err != nil {
		return lifecycle.ExportSummary{}, err
	}
	return s, nil
}

func TestLifecycleUnwiredIs404(t *testing.T) {
	f := newLifecycleFixture(t, false)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/admin/lifecycle/policies"},
		{http.MethodPost, "/admin/lifecycle/policies"},
		{http.MethodGet, "/admin/lifecycle/runs"},
		{http.MethodPost, "/admin/lifecycle/runs"},
		{http.MethodGet, "/admin/exports"},
		{http.MethodPost, "/admin/exports"},
	} {
		if rec := f.do(t, tc.method, tc.path, legacyToken, `{}`); rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s unwired must be 404 (rollback posture), got %d", tc.method, tc.path, rec.Code)
		}
	}
}

// bytesSink satisfies io.Writer without extra imports in assertions.
var _ io.Writer = (*bytes.Buffer)(nil)
