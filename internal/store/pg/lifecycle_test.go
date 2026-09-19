package pg

// Env-gated integration tests for the data-lifecycle store on real
// PostgreSQL: retention policy mutation atomicity, sweep eligibility
// (terminal jobs only, settled/released ledger only), the archive→verify→
// delete cycle against a real filesystem sink, KeyTTL enforcement and
// reclamation, and tenant-scoped redacted exports. Run with
// TEST_DATABASE_URL set; the tests skip otherwise and are serialized with
// the same advisory lock as the other schema-driving suites.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// lifecycleTestEnv is the seeded fixture shared by the suite.
type lifecycleTestEnv struct {
	store    *LifecycleStore
	jobs     *AsyncStore
	tenantA  string
	tenantB  string
	subjectA string
	subjectB string
}

func newLifecycleTestEnv(t *testing.T) *lifecycleTestEnv {
	t.Helper()
	dsn := testDatabaseURL(t)
	ensureAsyncSchema(t, dsn)
	pool, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(pool.Close)

	env := &lifecycleTestEnv{
		store:    &LifecycleStore{DB: pool},
		jobs:     &AsyncStore{DB: pool},
		tenantA:  "tenant_lc_a",
		tenantB:  "tenant_lc_b",
		subjectA: "subject_lc_a",
		subjectB: "subject_lc_b",
	}

	clean := []string{
		`DELETE FROM data_exports WHERE id LIKE 'exp_lc%' OR requested_by LIKE 'lc-%'`,
		`DELETE FROM archive_runs`,
		`DELETE FROM retention_policies`,
		`DELETE FROM admin_audit WHERE action IN ('retention_policy_upsert', 'retention_run', 'data_export', 'lc_test_op')`,
		`DELETE FROM llm_requests WHERE request_id LIKE 'req_lc%'`,
		`DELETE FROM usage_ledger WHERE subject_id LIKE 'subject_lc%'`,
		`DELETE FROM idempotency_keys WHERE subject_id LIKE 'subject_lc%'`,
		`DELETE FROM async_job_requests WHERE job_id LIKE 'job_lc%'`,
		`DELETE FROM async_job_results WHERE job_id LIKE 'job_lc%'`,
		`DELETE FROM async_jobs WHERE job_id LIKE 'job_lc%'`,
		`DELETE FROM subjects WHERE id LIKE 'subject_lc%'`,
		`DELETE FROM tenants WHERE id LIKE 'tenant_lc%'`,
	}
	for _, stmt := range clean {
		if _, err := pool.Pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("clean: %v: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		for _, stmt := range clean {
			_, _ = pool.Pool.Exec(context.Background(), stmt)
		}
	})

	for _, stmt := range []string{
		`INSERT INTO tenants (id, name) VALUES ('tenant_lc_a', 'LC A'), ('tenant_lc_b', 'LC B') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_lc_a', 'tenant_lc_a'), ('subject_lc_b', 'tenant_lc_b') ON CONFLICT (id) DO NOTHING`,
	} {
		if _, err := pool.Pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v: %v", stmt, err)
		}
	}
	return env
}

// seedRequest inserts one audit row at the given age.
func (e *lifecycleTestEnv) seedRequest(t *testing.T, requestID, subject, tenant string, age time.Duration) {
	t.Helper()
	_, err := e.store.DB.Pool.Exec(context.Background(), `
		INSERT INTO llm_requests (request_id, subject_id, key_id, model, provider, status,
		       latency_ms, streaming, created_at, trace_id, route_attempts, protocol)
		VALUES ($1, $2, 'key_lc', 'gateway-echo', 'fake-primary', 200, 5, false, $3, $1, 1, 'chat')`,
		requestID, subject, time.Now().Add(-age))
	if err != nil {
		t.Fatalf("seed request %s: %v", requestID, err)
	}
}

// seedJob inserts one async job (optionally with a stored result).
func (e *lifecycleTestEnv) seedJob(t *testing.T, jobID, subject, tenant, status string, age time.Duration, withResult bool) {
	t.Helper()
	_, err := e.store.DB.Pool.Exec(context.Background(), `
		INSERT INTO async_jobs (job_id, subject_id, tenant_id, protocol, public_model,
		       request_digest, status, created_at, updated_at, visible_at)
		VALUES ($1, $2, $3, 'responses', 'gateway-echo', 'digest', $4, $5, $5, $5)`,
		jobID, subject, tenant, status, time.Now().Add(-age))
	if err != nil {
		t.Fatalf("seed job %s: %v", jobID, err)
	}
	if withResult {
		if _, err := e.store.DB.Pool.Exec(context.Background(), `
			INSERT INTO async_job_results (job_id, response, error_class, result_bytes, retention_expires_at, created_at)
			VALUES ($1, '{"secret":"completion-content"}', NULL, 28, $2, $2)`,
			jobID, time.Now().Add(-age)); err != nil {
			t.Fatalf("seed result %s: %v", jobID, err)
		}
	}
}

// seedLedger inserts one ledger row in the given settlement state.
func (e *lifecycleTestEnv) seedLedger(t *testing.T, requestID, subject, tenant, status string, age time.Duration) {
	t.Helper()
	var settledAt any
	if status == "settled" {
		settledAt = time.Now().Add(-age)
	}
	_, err := e.store.DB.Pool.Exec(context.Background(), `
		INSERT INTO usage_ledger (request_id, subject_id, tenant_id, protocol, public_model,
		       settle_status, settled_at, created_at)
		VALUES ($1, $2, $3, 'chat', 'gateway-echo', $4, $5, $6)`,
		requestID, subject, tenant, status, settledAt, time.Now().Add(-age))
	if err != nil {
		t.Fatalf("seed ledger %s: %v", requestID, err)
	}
}

func TestLifecyclePolicyMutationCommitsWithAudit(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()

	op := mgmt.AdminOp{Action: "retention_policy_upsert", AdminSubject: "lc-test"}
	in := lifecycle.PolicyInput{Table: lifecycle.TableRequests, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true}
	if err := env.store.UpsertRetentionPolicy(ctx, in, op); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	policies, err := env.store.RetentionPolicies(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(policies) != 1 || policies[0].Table != lifecycle.TableRequests || policies[0].TTL != 3600 || !policies[0].Enabled {
		t.Fatalf("policies = %+v", policies)
	}

	// The audit record committed with the mutation (atomic rule), including
	// the previous-state summary on the second write.
	var auditCount int
	if err := env.store.DB.Pool.QueryRow(ctx,
		`SELECT count(*) FROM admin_audit WHERE action = 'retention_policy_upsert'`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("audit rows = %d, want exactly one committed with the mutation", auditCount)
	}
	in.TTL = 7200
	if err := env.store.UpsertRetentionPolicy(ctx, in, op); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	policies, _ = env.store.RetentionPolicies(ctx)
	if len(policies) != 1 || policies[0].TTL != 7200 {
		t.Fatalf("policies after update = %+v", policies)
	}

	// Ungoverned tables are rejected before any write.
	if err := env.store.UpsertRetentionPolicy(ctx,
		lifecycle.PolicyInput{Table: "tenants", TTL: 60}, op); err == nil {
		t.Fatal("ungoverned table must be rejected")
	}
}

func TestLifecycleSweepArchiveThenDelete(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()

	// Old (48h) and fresh rows; policy TTL 1h makes only the old one eligible.
	env.seedRequest(t, "req_lc_old", env.subjectA, env.tenantA, 48*time.Hour)
	env.seedRequest(t, "req_lc_fresh", env.subjectA, env.tenantA, time.Minute)

	if err := env.store.UpsertRetentionPolicy(ctx, lifecycle.PolicyInput{
		Table: lifecycle.TableRequests, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true,
	}, mgmt.AdminOp{Action: "lc_test_op"}); err != nil {
		t.Fatalf("policy: %v", err)
	}

	archiveDir := t.TempDir()
	sw, err := lifecycle.NewSweeper(lifecycle.SweeperDeps{
		Store: env.store, Sink: lifecycle.NewFSArchiveSink(archiveDir), Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := sw.Run(ctx, lifecycle.Options{Only: lifecycle.TableRequests})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	r := res[0]
	if r.Status != "completed" || r.Archived != 1 || r.Deleted != 1 {
		t.Fatalf("result = %+v", r)
	}

	// The old row is gone; the fresh row survived.
	var count int
	if err := env.store.DB.Pool.QueryRow(ctx,
		`SELECT count(*) FROM llm_requests WHERE request_id = 'req_lc_old'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("expired row must be deleted")
	}
	if err := env.store.DB.Pool.QueryRow(ctx,
		`SELECT count(*) FROM llm_requests WHERE request_id = 'req_lc_fresh'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("fresh row must survive")
	}

	// The archive artifact is complete, verified, and content-matching.
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	var manifest lifecycle.Manifest
	var dataFile string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".manifest.json") {
			raw, err := os.ReadFile(filepath.Join(archiveDir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			dataFile = filepath.Join(archiveDir, manifest.DataFile)
		}
	}
	if !manifest.Complete || manifest.RowCount != 1 || manifest.Table != lifecycle.TableRequests {
		t.Fatalf("manifest = %+v", manifest)
	}
	if manifest.SchemaVersion != 11 {
		t.Fatalf("manifest schema version = %d, want 11", manifest.SchemaVersion)
	}
	data, err := os.ReadFile(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if manifest.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("manifest checksum does not match the stored bytes")
	}
	if !strings.Contains(string(data), `"request_id":"req_lc_old"`) {
		t.Fatalf("archive must contain the projected row: %s", data)
	}

	// Idempotent rerun: nothing left to archive or delete.
	res, err = sw.Run(ctx, lifecycle.Options{Only: lifecycle.TableRequests})
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if res[0].Archived != 0 || res[0].Deleted != 0 {
		t.Fatalf("rerun must be a no-op: %+v", res[0])
	}

	// The run bookkeeping landed in archive_runs with terminal state.
	runs, err := env.store.RecentRuns(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Status != "completed" || runs[0].RowsDeleted != 0 {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestLifecycleEligibilityKeepsLiveState(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()

	// Jobs: old terminal job eligible; old queued and running jobs are live
	// work and must never be swept. The old completed job carries a result —
	// its content-free record must ride the job's archive batch before the
	// cascade deletes it.
	env.seedJob(t, "job_lc_done", env.subjectA, env.tenantA, "completed", 48*time.Hour, true)
	env.seedJob(t, "job_lc_run", env.subjectA, env.tenantA, "running", 48*time.Hour, false)
	env.seedJob(t, "job_lc_queue", env.subjectA, env.tenantA, "queued", 48*time.Hour, false)

	// Ledger: settled and released rows age out; the reserved row is the
	// exactly-once settlement input and must never be eligible.
	env.seedLedger(t, "req_lc_led_settled", env.subjectA, env.tenantA, "settled", 48*time.Hour)
	env.seedLedger(t, "req_lc_led_released", env.subjectA, env.tenantA, "released", 48*time.Hour)
	env.seedLedger(t, "req_lc_led_reserved", env.subjectA, env.tenantA, "reserved", 48*time.Hour)

	for _, p := range []lifecycle.PolicyInput{
		{Table: lifecycle.TableJobs, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true},
		{Table: lifecycle.TableResults, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true},
		{Table: lifecycle.TableLedger, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true},
	} {
		if err := env.store.UpsertRetentionPolicy(ctx, p, mgmt.AdminOp{Action: "lc_test_op"}); err != nil {
			t.Fatalf("policy %s: %v", p.Table, err)
		}
	}

	archiveDir := t.TempDir()
	sw, err := lifecycle.NewSweeper(lifecycle.SweeperDeps{
		Store: env.store, Sink: lifecycle.NewFSArchiveSink(archiveDir), Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := sw.Run(ctx, lifecycle.Options{})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	byTable := map[lifecycle.TableName]lifecycle.TableResult{}
	for _, r := range res {
		byTable[r.Table] = r
	}
	if byTable[lifecycle.TableJobs].Deleted != 1 || byTable[lifecycle.TableLedger].Deleted != 2 {
		t.Fatalf("sweep results = %+v", res)
	}

	mustCount := func(query string, want int) {
		t.Helper()
		var n int
		if err := env.store.DB.Pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("query %q = %d, want %d", query, n, want)
		}
	}
	mustCount(`SELECT count(*) FROM async_jobs WHERE job_id = 'job_lc_run'`, 1)
	mustCount(`SELECT count(*) FROM async_jobs WHERE job_id = 'job_lc_queue'`, 1)
	mustCount(`SELECT count(*) FROM async_jobs WHERE job_id = 'job_lc_done'`, 0)
	mustCount(`SELECT count(*) FROM usage_ledger WHERE request_id = 'req_lc_led_reserved'`, 1)
	mustCount(`SELECT count(*) FROM usage_ledger WHERE settle_status IN ('settled','released')`, 0)

	// The completed job's result was archived (content-free) before the
	// cascade removed it; the response body never entered the archive.
	archiveBytes := readDirBytes(t, archiveDir)
	if !strings.Contains(archiveBytes, `"type":"async_job_result"`) {
		t.Fatalf("job batch must archive its result record: %s", archiveBytes)
	}
	if strings.Contains(archiveBytes, "completion-content") {
		t.Fatal("archives must never contain stored response content")
	}

	// Dry run reports eligible counts without touching anything.
	res, err = sw.Run(ctx, lifecycle.Options{DryRun: true, Only: lifecycle.TableJobs})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Status != "dry-run" || res[0].Eligible != 0 {
		t.Fatalf("dry run = %+v", res[0])
	}
}

func readDirBytes(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".ndjson") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
	}
	return b.String()
}

func TestLifecycleStaleRunRecovery(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()

	runID, err := env.store.StartRun(ctx, lifecycle.TableRequests, false, time.Now().Add(-2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	n, err := env.store.MarkStaleRunsFailed(ctx, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	runs, err := env.store.RecentRuns(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].ID != runID || runs[0].Status != "failed" || runs[0].FinishedAt == nil {
		t.Fatalf("stale run = %+v", runs[0])
	}
}

func TestLifecycleKeyTTLEnforcementAndSweep(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()
	base := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	snap := []byte(`{"model":"gateway-echo"}`)

	in := async.CreateInput{
		JobID: "job_lc_idem_1", SubjectID: env.subjectA, TenantID: env.tenantA,
		Protocol: "responses", PublicModel: "gateway-echo",
		RequestDigest: "digest-lc", Request: snap,
		KeyHash: "hash-lc", KeyTTL: time.Hour, Now: base,
	}
	if _, err := env.jobs.Create(ctx, in); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Inside the window the mapping replays the original job.
	out, err := env.jobs.Create(ctx, func() async.CreateInput {
		c := in
		c.JobID = "job_lc_idem_2"
		c.Now = base.Add(30 * time.Minute)
		return c
	}())
	if err != nil || !out.Replay || out.Job.ID != "job_lc_idem_1" {
		t.Fatalf("in-window replay = %+v err=%v", out, err)
	}

	// After expires_at the mapping no longer replays: the KeyTTL contract is
	// enforced at the lookup boundary even before the sweep runs. The new
	// creation also reclaims the expired row in the same transaction (the
	// unique index must admit the fresh mapping).
	out, err = env.jobs.Create(ctx, func() async.CreateInput {
		c := in
		c.JobID = "job_lc_idem_3"
		c.Now = base.Add(2 * time.Hour)
		return c
	}())
	if err != nil || out.Replay || out.Job.ID != "job_lc_idem_3" {
		t.Fatalf("expired key must not replay: %+v err=%v", out, err)
	}

	// A mapping that expired without a later create still sits in the table:
	// the reclamation sweep is what removes it.
	stale := in
	stale.JobID = "job_lc_idem_s"
	stale.KeyHash = "hash-lc-stale"
	if _, err := env.jobs.Create(ctx, stale); err != nil {
		t.Fatalf("create stale: %v", err)
	}

	// The reclamation sweep removes exactly the expired mappings.
	n, err := env.jobs.SweepExpiredIdempotencyKeys(ctx, base.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("swept = %d, want 1", n)
	}
	var remaining int
	if err := env.store.DB.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE subject_id = $1`, env.subjectA).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining mappings = %d, want the live one", remaining)
	}
}

func TestLifecycleScopedRedactedExport(t *testing.T) {
	env := newLifecycleTestEnv(t)
	ctx := context.Background()

	// Two tenants' audit rows plus a stored result with completion content.
	env.seedRequest(t, "req_lc_ex_a1", env.subjectA, env.tenantA, time.Hour)
	env.seedRequest(t, "req_lc_ex_a2", env.subjectA, env.tenantA, 2*time.Hour)
	env.seedRequest(t, "req_lc_ex_b1", env.subjectB, env.tenantB, 90*time.Minute)
	env.seedJob(t, "job_lc_ex", env.subjectA, env.tenantA, "completed", time.Hour, true)
	if _, err := env.store.DB.Pool.Exec(ctx,
		`INSERT INTO admin_audit (action, target, admin_subject, detail)
		 VALUES ('lc_test_op', 'target', 'lc-admin', '{"x":1}')`); err != nil {
		t.Fatalf("seed management log: %v", err)
	}

	admin, err := lifecycle.NewAdmin(lifecycle.AdminDeps{
		Store: env.store, Export: env.store,
		Sink: lifecycle.NewFSArchiveSink(t.TempDir()), Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Tenant-scoped export: only tenant A rows, and never the management log.
	var buf bytes.Buffer
	summary, err := admin.Export(ctx, &buf, lifecycle.ExportRequest{
		RequestedBy: "lc-admin", Tenant: env.tenantA,
		Filter: lifecycle.ExportFilter{Tenant: env.tenantA},
		Op:     mgmt.AdminOp{Action: "data_export", AdminSubject: "lc-admin"},
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !summary.Complete {
		t.Fatalf("summary = %+v", summary)
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	var records []map[string]any
	for i, l := range lines {
		if i == 0 {
			continue // header
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if _, ok := m["summary"]; ok {
			continue
		}
		records = append(records, m)
	}
	if summary.Rows != int64(len(records)) || len(records) != 4 { // 2 requests + job + result
		t.Fatalf("records = %d summary.Rows = %d, want 4", len(records), summary.Rows)
	}
	for _, rec := range records {
		if rec["type"] == string(lifecycle.RecordManagementLog) {
			t.Fatal("tenant export must never include the management log")
		}
		body := rec["record"].(map[string]any)
		if sub, ok := body["subject_id"].(string); ok && sub == env.subjectB {
			t.Fatal("cross-tenant record leaked into the export")
		}
		if rec["type"] == string(lifecycle.RecordResult) {
			if _, has := body["response"]; has {
				t.Fatal("export projections must omit the stored response body")
			}
		}
	}
	// The checksum verifies exactly the emitted record bytes.
	hash := sha256.New()
	for i, l := range lines {
		if i == 0 || strings.Contains(l, `"summary"`) {
			continue
		}
		hash.Write([]byte(l))
		hash.Write([]byte("\n"))
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != summary.SHA256 {
		t.Fatal("summary checksum does not verify the emitted artifact")
	}

	// The export record completed with its verification metadata, and the
	// tenant predicate governs who can list it.
	exports, err := env.store.ListExports(ctx, env.tenantA, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(exports) != 1 || exports[0].Status != "completed" {
		t.Fatalf("exports = %+v", exports)
	}
	var filters map[string]any
	if err := json.Unmarshal(exports[0].Filters, &filters); err != nil {
		t.Fatal(err)
	}
	if filters["rows"] != float64(4) || filters["sha256"] != summary.SHA256 {
		t.Fatalf("verification metadata = %v", filters)
	}
	if other, err := env.store.ListExports(ctx, env.tenantB, 10); err != nil || len(other) != 0 {
		t.Fatalf("tenant B must see none of tenant A's exports: %+v err=%v", other, err)
	}

	// Platform-global export may include the management log.
	buf.Reset()
	if _, err := admin.Export(ctx, &buf, lifecycle.ExportRequest{
		RequestedBy: "platform",
		Filter:      lifecycle.ExportFilter{IncludeManagementLog: true},
		Op:          mgmt.AdminOp{Action: "data_export", AdminSubject: "platform"},
	}); err != nil {
		t.Fatalf("global export: %v", err)
	}
	if !strings.Contains(buf.String(), `"type":"management_log"`) {
		t.Fatalf("global export with the flag must include management-log rows: %s", buf.String())
	}

	// Unknown tenants are rejected before any artifact is produced.
	if _, err := admin.Export(ctx, io.Discard, lifecycle.ExportRequest{
		Filter: lifecycle.ExportFilter{Tenant: "tenant_lc_missing"},
	}); err != lifecycle.ErrUnknownTenant {
		t.Fatalf("unknown tenant err = %v", err)
	}
}
