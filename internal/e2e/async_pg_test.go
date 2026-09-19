package e2e

// V1.4 end-to-end rollout validation over the full serving stack: real HTTP
// server, real PostgreSQL (job queue, results, idempotency, usage ledger,
// budget policies), and a real worker pool — the coverage gap the async child
// flagged (its own suite ran the worker over the in-memory store only).
//
// The file is organized as the staged rollout rehearsal:
//
//	flags off          TestAsyncE2EFlagsOffRollbackPosture    (V1.3-identical wire, readiness)
//	async on           TestAsyncE2ECreatePollResult           (create → poll → result)
//	                   TestAsyncE2ECreateCancel               (create → cancel, owner isolation)
//	                   TestAsyncE2ELegacyAdminTokenVisibility (rollback-path admin identity)
//	budgets on         TestAsyncE2EBudgetEnforcementDenies    (429 before any job)
//	budgets rollback   TestAsyncE2EBudgetsOffStillCapturesLedger
//	async rollback     TestAsyncE2EShutdownDrainCommitsInFlight
//	race re-verified   TestAsyncE2ECancelCommitRaceSettlesExactlyOnce
//
// The tests share the test database with the internal/store/pg suites and
// therefore run under the same session-level advisory lock; schema is brought
// to the migration head and left there. Run with TEST_DATABASE_URL set; the
// tests skip otherwise.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
)

// e2eDatabaseLockKey is identical to internal/store/pg's testDatabaseLockKey
// (documented there): concurrent `go test ./...` package runs cannot race on
// the shared TEST_DATABASE_URL schema.
const e2eDatabaseLockKey int64 = 721534891

const (
	e2eSubjectOwner = "subject_e2e_owner"
	e2eSubjectOther = "subject_e2e_other"
	e2eTenant       = "tenant_default"
	e2eOwnerKey     = "sk-e2e-owner-plaintext"
	e2eOtherKey     = "sk-e2e-other-plaintext"
	e2eLegacyToken  = "legacy-e2e-admin-token"
	e2eModel        = "gateway-echo"
	e2eProvider     = "fake"
	e2ePriceVersion = 9014
)

// e2eStage selects the feature flags under rehearsal (the rollout order:
// dark ledger, async, budgets).
type e2eStage struct {
	asyncEnabled     bool              // GATEWAY_ASYNC_ENABLED
	budgetEnforce    bool              // GATEWAY_BUDGETS_ENABLED
	budgetMicros     int64             // >0 seeds a subject daily budget policy
	providerOverride provider.Provider // nil = plain Fake
}

// asyncE2EEnv is one staged gateway: HTTP server + worker pool over the real
// database.
type asyncE2EEnv struct {
	srv    *httptest.Server
	client *http.Client
	jobs   *pgstore.AsyncStore
	ledger *pgstore.LedgerStore
	pool   *async.Pool
	sink   *audit.MemorySink
	db     *sql.DB
}

// newAsyncE2EEnv brings the schema to head, seeds the E2E subject/price rows,
// and assembles the full gateway exactly as cmd/gateway does for the stage:
// admission pipeline (limiter/quota/accounting over the PG ledger), worker
// pool, job query/cancel routes, and the readiness aggregate with feature
// checks registered only while their feature is on.
func newAsyncE2EEnv(t *testing.T, stage e2eStage) *asyncE2EEnv {
	t.Helper()
	dsn := e2eTestDatabaseURL(t)
	lockE2EDatabase(t, dsn)
	ensureE2ESchema(t, dsn)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cleanE2ERows(t, db)
	t.Cleanup(func() { cleanE2ERows(t, db) })

	// Subject rows back the async_jobs/usage_ledger foreign keys; the API-key
	// store stays in-memory (key lifecycle is covered by its own suites).
	seed := []string{
		`INSERT INTO tenants (id, name) VALUES ('` + e2eTenant + `', 'E2E Default') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('` + e2eSubjectOwner + `', '` + e2eTenant + `'), ('` + e2eSubjectOther + `', '` + e2eTenant + `') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO pricing_catalog (provider, public_model, price_version, currency,
			input_micros_per_token, output_micros_per_token, effective_from)
			VALUES ('` + e2eProvider + `', '` + e2eModel + `', ` + fmt.Sprint(e2ePriceVersion) + `, 'USD', 1000, 2000, now() - interval '1 hour')
			ON CONFLICT (provider, public_model, price_version) DO NOTHING`,
	}
	if stage.budgetMicros > 0 {
		seed = append(seed, `INSERT INTO budget_policies (scope, subject_id, tenant_id, period, currency, amount_micros, enabled)
			VALUES ('subject', '`+e2eSubjectOwner+`', '`+e2eTenant+`', 'daily', 'USD', `+fmt.Sprint(stage.budgetMicros)+`, true)
			ON CONFLICT (scope, COALESCE(subject_id, ''), tenant_id, period, currency) DO UPDATE SET amount_micros = EXCLUDED.amount_micros, enabled = true`)
	}
	for _, stmt := range seed {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed: %v: %v", stmt, err)
		}
	}

	ctx := context.Background()
	dbw, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(dbw.Close)

	prov := provider.Provider(provider.Fake{})
	if stage.providerOverride != nil {
		prov = stage.providerOverride
	}

	catalog := policy.NewCatalog([]policy.ModelInfo{{
		PublicName: e2eModel, Provider: e2eProvider, UpstreamModel: "echo-model", Enabled: true,
		Capabilities: model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true},
	}})
	svc := gateway.New(catalog, map[string]provider.Provider{e2eProvider: prov}, 5*time.Second, 0)
	svc.RetryWait = time.Millisecond
	// The async service shares the live route table; only the total deadline
	// differs (the job timeout), mirroring cmd/gateway.
	asyncSvc := gateway.New(catalog, map[string]provider.Provider{e2eProvider: prov}, 30*time.Second, 0)
	asyncSvc.Routes = svc.Routes
	svc.Routes.SetRoutes(e2eModel, []router.Route{{
		ProviderName: e2eProvider, Provider: prov, UpstreamModel: "echo-model",
		Priority: 10, Enabled: true, Breaker: router.NewBreaker(10, time.Minute),
	}})

	store := auth.NewStore()
	for _, rec := range []struct{ id, subject, plaintext string }{
		{"key-e2e-owner", e2eSubjectOwner, e2eOwnerKey},
		{"key-e2e-other", e2eSubjectOther, e2eOtherKey},
	} {
		salt, err := auth.NewSalt()
		if err != nil {
			t.Fatal(err)
		}
		// TenantID rides the principal exactly as the production
		// authenticators resolve it; the async rows and the ledger derive
		// their tenant from it (jobs carry the composite subject/tenant FK).
		store.Put(auth.KeyRecord{ID: rec.id, Subject: rec.subject, TenantID: e2eTenant,
			Salt: salt, Hash: auth.HashAPIKey(salt, rec.plaintext), Status: auth.StatusActive})
	}

	pol := policy.New()
	pol.AllowAll(e2eSubjectOwner)
	pol.AllowAll(e2eSubjectOther)

	rateLimiter := limiter.New(1000, 100)
	quotaGate := quota.NewMemory()
	ledger := &pgstore.LedgerStore{DB: dbw}
	env := &asyncE2EEnv{
		jobs: &pgstore.AsyncStore{DB: dbw}, ledger: ledger,
		sink: audit.NewMemorySink(nil), db: db,
	}
	accountingGate := &accounting.Gate{
		Store: ledger, Budgets: accounting.NewMemoryBudget(),
		Enforcement: stage.budgetEnforce, Now: time.Now, Metrics: metrics.New(),
	}

	chat := &httpapi.ChatHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Accounting: accountingGate, Audit: env.sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}

	// Feature flags: readiness registers the queue/worker checks only while
	// async is enabled, and the job routes only exist then (the rollback
	// posture the flags-off rehearsal asserts).
	var (
		asyncBundle *httpapi.Async
		jobsHandler http.Handler
	)
	responses := &httpapi.ResponsesHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Accounting: accountingGate, Audit: env.sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
	}
	if stage.asyncEnabled {
		cancels := async.NewCancelRegistry()
		env.pool = async.NewPool(async.PoolDeps{
			Store: env.jobs, Service: asyncSvc, Policy: pol, Limiter: rateLimiter,
			Quota: quotaGate, Accounting: accountingGate,
			Audit: env.sink, Metrics: metrics.New(), Cancels: cancels,
			Encoder: httpapi.NewAsyncEncoder(), Now: time.Now,
		}, async.PoolConfig{
			WorkerID: "worker-e2e", Count: 2, PollInterval: 5 * time.Millisecond,
			Lease: 10 * time.Second, JobTimeout: 5 * time.Second,
			MaxAttempts: 3, ResultTTL: 24 * time.Hour,
			MaxResultBytes: 1 << 20, Drain: 3 * time.Second,
		})
		env.pool.Start(ctx)
		t.Cleanup(func() { env.pool.Stop(3 * time.Second) })
		asyncBundle = &httpapi.Async{
			Jobs: env.jobs, Cancels: cancels, Wake: env.pool.Wake,
			ResultTTL: 24 * time.Hour, KeyTTL: time.Hour,
			MaxResultBytes: 1 << 20, MaxKeyBytes: 256,
		}
		responses.Async = asyncBundle
		jobsHandler = &httpapi.AsyncJobsHandler{
			Auth: store, Jobs: env.jobs, Cancels: cancels, Audit: env.sink,
			Metrics: metrics.New(),
			AdminAuth: &adminauth.Authenticator{
				Store: adminauth.NewMemoryStore(), LegacyToken: e2eLegacyToken,
				Limiter: adminauth.NewAuthLimiter(), Now: time.Now,
			},
			PollHint: time.Second, Now: time.Now,
		}
	}

	readiness := httpapi.NewReadiness(nil)
	readiness.Register("database", true, dbw.Ready)
	readiness.Register("catalog", true, func(context.Context) error {
		if catalog == nil || len(catalog.All()) == 0 {
			return errors.New("model catalog is empty")
		}
		return nil
	})
	if asyncBundle != nil {
		readiness.Register("queue", true, func(ctx context.Context) error {
			_, err := env.jobs.QueueDepth(ctx)
			return err
		})
		readiness.Register("worker", true, func(context.Context) error {
			if env.pool != nil && env.pool.Healthy() {
				return nil
			}
			return errors.New("worker pool not accepting jobs")
		})
	}
	// The settlement check rides the ledger (database mode), independent of
	// the async flag — exactly the cmd/gateway wiring.
	readiness.Register("settlement", true, func(ctx context.Context) error {
		n, err := ledger.ReservedBacklog(ctx, 10*time.Minute)
		if err != nil {
			return err
		}
		if n > 100 {
			return fmt.Errorf("reserved ledger backlog %d exceeds threshold", n)
		}
		return nil
	})

	mreg := metrics.New()
	env.srv = httptest.NewServer(httpapi.NewMux(chat, httpapi.Deps{
		Ready:           readiness,
		Metrics:         mreg.Handler(),
		Responses:       responses,
		ResponsesGet:    jobsHandler,
		ResponsesCancel: jobsHandler,
	}))
	t.Cleanup(env.srv.Close)
	env.client = env.srv.Client()
	return env
}

func e2eTestDatabaseURL(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping the PostgreSQL async E2E rehearsal")
	}
	return dsn
}

// lockE2EDatabase serializes against the schema-driving suites in
// internal/store/pg and cmd/gateway (same advisory lock key).
func lockE2EDatabase(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("lock connect: %v", err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("lock connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", e2eDatabaseLockKey); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", e2eDatabaseLockKey)
		_ = conn.Close()
		_ = db.Close()
	})
}

// ensureE2ESchema migrates the shared database to the head (no-op when it is
// already there) and never rolls it back down.
func ensureE2ESchema(t *testing.T, dsn string) {
	t.Helper()
	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	dir, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations dir: %v", err)
	}
	driver, err := pgx5.WithInstance(mdb, &pgx5.Config{})
	if err != nil {
		t.Fatalf("migrate driver: %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+dir, "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate instance: %v", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

// cleanE2ERows removes exactly the rows this suite creates (subject- and
// version-scoped), never the other suites' fixtures. Ledger rows go first:
// usage_ledger.job_id references async_jobs without a cascade.
func cleanE2ERows(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM usage_ledger WHERE subject_id IN ('` + e2eSubjectOwner + `','` + e2eSubjectOther + `')`,
		`DELETE FROM idempotency_keys WHERE subject_id IN ('` + e2eSubjectOwner + `','` + e2eSubjectOther + `')`,
		`DELETE FROM async_job_results WHERE job_id IN (SELECT job_id FROM async_jobs WHERE subject_id IN ('` + e2eSubjectOwner + `','` + e2eSubjectOther + `'))`,
		`DELETE FROM async_job_requests WHERE job_id IN (SELECT job_id FROM async_jobs WHERE subject_id IN ('` + e2eSubjectOwner + `','` + e2eSubjectOther + `'))`,
		`DELETE FROM async_jobs WHERE subject_id IN ('` + e2eSubjectOwner + `','` + e2eSubjectOther + `')`,
		`DELETE FROM budget_policies WHERE subject_id = '` + e2eSubjectOwner + `'`,
		`DELETE FROM pricing_catalog WHERE provider = '` + e2eProvider + `' AND public_model = '` + e2eModel + `' AND price_version = ` + fmt.Sprint(e2ePriceVersion),
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("clean e2e rows: %v: %v", stmt, err)
		}
	}
}

// --- HTTP helpers ---------------------------------------------------------

func (e *asyncE2EEnv) do(t *testing.T, method, path, key, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	resp, raw, err := e.tryDo(method, path, key, body, hdr)
	if err != nil {
		t.Fatal(err)
	}
	return resp, raw
}

// tryDo is the non-fatal variant, safe to call from auxiliary goroutines.
func (e *asyncE2EEnv) tryDo(method, path, key, body string, hdr map[string]string) (*http.Response, []byte, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	if err != nil {
		return nil, nil, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, nil, err
	}
	return resp, raw, nil
}

// createBackground posts one background:true creation and decodes the
// envelope's job id.
func (e *asyncE2EEnv) createBackground(t *testing.T, key, idemKey, body string) (*http.Response, []byte, string) {
	t.Helper()
	hdr := map[string]string{}
	if idemKey != "" {
		hdr["Idempotency-Key"] = idemKey
	}
	resp, raw := e.do(t, http.MethodPost, "/v1/responses", key, body, hdr)
	var out struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &out)
	return resp, raw, out.ID
}

// pollUntilGetStatus polls the public GET endpoint (never the store) until
// the envelope reports one of the wanted statuses.
func (e *asyncE2EEnv) pollUntilGetStatus(t *testing.T, jobID, key string, want ...string) (int, []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, raw := e.do(t, http.MethodGet, "/v1/responses/"+jobID, key, "", nil)
		var out struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(raw, &out); err == nil {
			for _, w := range want {
				if out.Status == w {
					return resp.StatusCode, raw
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %v over HTTP", jobID, want)
	return 0, nil
}

// awaitTerminalStore waits for the store row to leave the running set.
func (e *asyncE2EEnv) awaitTerminalStore(t *testing.T, jobID string) async.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := e.jobs.Get(context.Background(), jobID, time.Now())
		if err == nil && j.Status.Terminal() {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached a terminal state in the store", jobID)
	return async.Job{}
}

// ledgerRowsForJob reads the settle_status values of the job's ledger rows.
func (e *asyncE2EEnv) ledgerRowsForJob(t *testing.T, jobID string) []string {
	t.Helper()
	rows, err := e.db.Query(`SELECT settle_status FROM usage_ledger WHERE job_id = $1 ORDER BY id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// awaitNoReservedForJob waits until every ledger row of the job left the
// reserved state (the worker finalizes synchronously; the wait only absorbs
// scheduling jitter and keeps the assertions race-free).
func (e *asyncE2EEnv) awaitNoReservedForJob(t *testing.T, jobID string) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		states := e.ledgerRowsForJob(t, jobID)
		reserved := false
		for _, s := range states {
			if s == "reserved" {
				reserved = true
			}
		}
		if !reserved {
			return states
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s ledger rows still reserved: %v", jobID, e.ledgerRowsForJob(t, jobID))
	return nil
}

// --- Stage: flags off (V1.3-identical rollback posture) --------------------

// TestAsyncE2EFlagsOffRollbackPosture proves the all-flags-off deployment
// keeps the V1.3 wire contracts: background:true is refused with the stable
// 503 job_queue_unavailable envelope (never a job), the job query route does
// not exist, the synchronous Responses flow is unchanged, and /readyz
// reflects exactly the enabled features (no queue/worker checks; the
// settlement check rides the database-mode ledger, not the async flag).
func TestAsyncE2EFlagsOffRollbackPosture(t *testing.T) {
	env := newAsyncE2EEnv(t, e2eStage{})

	// Background acceptance: stable 503, no job persisted.
	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello","background":true}`)
	if resp.StatusCode != http.StatusServiceUnavailable || jobID != "" {
		t.Fatalf("flags-off background = %d %s (job %q)", resp.StatusCode, raw, jobID)
	}
	for _, want := range []string{`"code":"job_queue_unavailable"`, `"type":"service_unavailable"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("envelope missing %s: %s", want, raw)
		}
	}

	// The job query/cancel routes are unregistered (404), matching
	// cmd/gateway's wiring of ResponsesGet/ResponsesCancel only when async
	// is enabled.
	resp, raw = env.do(t, http.MethodGet, "/v1/responses/resp_x", e2eOwnerKey, "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("flags-off GET job = %d %s", resp.StatusCode, raw)
	}

	// Queue untouched: nothing was enqueued.
	if n, err := env.jobs.QueueDepth(context.Background()); err != nil || n != 0 {
		t.Fatalf("queue depth = %d err=%v, want 0", n, err)
	}

	// The synchronous V1.3 Responses flow is unchanged.
	resp, raw = env.do(t, http.MethodPost, "/v1/responses", e2eOwnerKey,
		`{"model":"`+e2eModel+`","input":"hello"}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sync responses = %d %s", resp.StatusCode, raw)
	}
	var syncOut struct {
		Object string `json:"object"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &syncOut); err != nil || syncOut.Object != "response" || syncOut.Status != "completed" {
		t.Fatalf("sync envelope = %s err=%v", raw, err)
	}

	// /readyz: no queue/worker checks while the feature is off; database,
	// catalog, and settlement (ledger capture rides database mode) are ok.
	resp, raw = env.do(t, http.MethodGet, "/readyz", "", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz = %d %s", resp.StatusCode, raw)
	}
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(raw, &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Status != "ready" {
		t.Fatalf("readyz status = %s (%s)", ready.Status, raw)
	}
	for _, absent := range []string{"queue", "worker"} {
		if _, ok := ready.Checks[absent]; ok {
			t.Fatalf("disabled feature %q must not appear in /readyz: %s", absent, raw)
		}
	}
	for _, present := range []string{"database", "catalog", "settlement"} {
		if ready.Checks[present] != "ok" {
			t.Fatalf("check %q = %q, want ok (%s)", present, ready.Checks[present], raw)
		}
	}
}

// --- Stage: async flag on ---------------------------------------------------

// TestAsyncE2ECreatePollResult is the flagged coverage gap: the full HTTP +
// PostgreSQL + worker journey. Create (202 queued) → idempotent replay and
// conflict over the real mapping table → poll the public GET until the
// worker claims, executes, and commits → the completed envelope with usage,
// exactly one settled ledger row with a real price version and cost, and
// readiness green including the queue and worker checks.
func TestAsyncE2ECreatePollResult(t *testing.T) {
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, budgetEnforce: true, budgetMicros: 1 << 40})

	body := `{"model":"` + e2eModel + `","input":"hello e2e","background":true,"max_output_tokens":16}`
	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "e2e-idem-1", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create = %d %s", resp.StatusCode, raw)
	}
	if !strings.HasPrefix(jobID, "resp_") {
		t.Fatalf("job id %q must be resp_-prefixed", jobID)
	}
	var created struct {
		Object string `json:"object"`
		Status string `json:"status"`
		Model  string `json:"model"`
	}
	if err := json.Unmarshal(raw, &created); err != nil ||
		created.Object != "response" || created.Status != "queued" || created.Model != e2eModel {
		t.Fatalf("202 envelope = %s err=%v", raw, err)
	}

	// Idempotent replay: same key + same request returns the original job.
	resp, raw, replayID := env.createBackground(t, e2eOwnerKey, "e2e-idem-1", body)
	if resp.StatusCode != http.StatusAccepted || replayID != jobID {
		t.Fatalf("replay = %d %s (id %q, want %q)", resp.StatusCode, raw, replayID, jobID)
	}
	// Conflict: same key, different request → 409 idempotency_conflict.
	resp, raw, _ = env.createBackground(t, e2eOwnerKey, "e2e-idem-1",
		`{"model":"`+e2eModel+`","input":"different","background":true}`)
	if resp.StatusCode != http.StatusConflict || !strings.Contains(string(raw), "idempotency_conflict") {
		t.Fatalf("conflict = %d %s", resp.StatusCode, raw)
	}

	// Poll the public endpoint through queued/running to completed.
	code, doneRaw := env.pollUntilGetStatus(t, jobID, e2eOwnerKey, "completed")
	if code != http.StatusOK {
		t.Fatalf("completed GET = %d %s", code, doneRaw)
	}
	var done struct {
		Status string `json:"status"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Usage *struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(doneRaw, &done); err != nil {
		t.Fatalf("completed envelope: %v %s", err, doneRaw)
	}
	if done.Status != "completed" || done.Error != nil || len(done.Output) == 0 ||
		done.Output[0].Type != "message" || done.Output[0].Content[0].Text == "" {
		t.Fatalf("completed envelope = %s", doneRaw)
	}
	if done.Usage == nil || done.Usage.TotalTokens <= 0 {
		t.Fatalf("completed envelope must carry reported usage: %s", doneRaw)
	}

	// Exactly one settled ledger row, priced with the seeded version, cost
	// computed from the reported usage — never fabricated, never duplicated.
	states := env.awaitNoReservedForJob(t, jobID)
	if len(states) != 1 || states[0] != "settled" {
		t.Fatalf("ledger rows for job = %v, want exactly one settled", states)
	}
	var (
		cost       sql.NullInt64
		version    sql.NullInt64
		currency   sql.NullString
		promptTok  sql.NullInt64
		completeTk sql.NullInt64
	)
	if err := env.db.QueryRow(`SELECT cost_micros, price_version, currency,
			prompt_tokens, completion_tokens FROM usage_ledger WHERE job_id = $1`, jobID).
		Scan(&cost, &version, &currency, &promptTok, &completeTk); err != nil {
		t.Fatal(err)
	}
	if !cost.Valid || cost.Int64 <= 0 || !version.Valid || version.Int64 != e2ePriceVersion ||
		!currency.Valid || currency.String != "USD" ||
		!promptTok.Valid || !completeTk.Valid {
		t.Fatalf("settled ledger row = cost=%v version=%v currency=%v prompt=%v completion=%v",
			cost, version, currency, promptTok, completeTk)
	}
	if want := int64(1000*promptTok.Int64 + 2000*completeTk.Int64); cost.Int64 != want {
		t.Fatalf("cost = %d, want prompt*1000+completion*2000 = %d", cost.Int64, want)
	}

	// The one terminal audit record for the job's execution (200, no class).
	var completedAudits int
	for _, ev := range env.sink.Snapshot() {
		if ev.Status == 200 && ev.ErrorClass == "" && ev.Protocol == "responses" {
			completedAudits++
		}
	}
	if completedAudits == 0 {
		t.Fatalf("no terminal audit record for the completed job: %+v", env.sink.Snapshot())
	}

	// Readiness is green with the async feature checks present and healthy.
	resp, raw = env.do(t, http.MethodGet, "/readyz", "", "", nil)
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(raw, &ready); err != nil || ready.Status != "ready" {
		t.Fatalf("readyz = %d %s err=%v", resp.StatusCode, raw, err)
	}
	for _, present := range []string{"database", "catalog", "queue", "worker", "settlement"} {
		if ready.Checks[present] != "ok" {
			t.Fatalf("check %q = %q, want ok (%s)", present, ready.Checks[present], raw)
		}
	}
}

// TestAsyncE2ECreateCancel covers the cancel journey over the real stack:
// cancel while running propagates to the provider context, the job lands
// cancelled with its lease cleared, ownership isolation stays non-leaky on
// the real store, repeat cancels are idempotent, and the billing evidence
// ends released-or-settled exactly once — never reserved, never duplicated.
func TestAsyncE2ECreateCancel(t *testing.T) {
	gated := newE2EGatedProvider()
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, providerOverride: gated})

	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello cancel","background":true}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create = %d %s", resp.StatusCode, raw)
	}

	// Wait until a worker claimed the job and the provider is executing.
	select {
	case <-gated.started:
	case <-time.After(10 * time.Second):
		t.Fatal("provider execution never started")
	}

	// Ownership isolation on the real store: another subject gets the same
	// non-leaky 404, never the job.
	resp, raw = env.do(t, http.MethodGet, "/v1/responses/"+jobID, e2eOtherKey, "", nil)
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(string(raw), "response_not_found") {
		t.Fatalf("cross-subject GET = %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOtherKey, "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-subject cancel = %d %s", resp.StatusCode, raw)
	}

	// The owner cancels: 200 cancelled, and the in-flight provider call
	// observes the propagated context cancellation.
	resp, raw = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOwnerKey, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel = %d %s", resp.StatusCode, raw)
	}
	var cancelled struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &cancelled); err != nil || cancelled.Status != "cancelled" {
		t.Fatalf("cancel envelope = %s err=%v", raw, err)
	}
	select {
	case <-gated.cancelObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("provider context was not cancelled by the client cancel")
	}

	// Terminal in the store with the lease released; repeat cancel stays a
	// 200 no-op reporting the final state.
	job := env.awaitTerminalStore(t, jobID)
	if job.Status != async.StatusCancelled || job.LeaseOwner != "" {
		t.Fatalf("terminal job = %+v, want cancelled with no lease", job)
	}
	resp, raw = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOwnerKey, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat cancel = %d %s", resp.StatusCode, raw)
	}

	// GET reports the cancelled status envelope; no result body is served.
	resp, raw = env.do(t, http.MethodGet, "/v1/responses/"+jobID, e2eOwnerKey, "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"status":"cancelled"`) {
		t.Fatalf("cancelled GET = %d %s", resp.StatusCode, raw)
	}

	// Billing evidence: exactly one final row, released or settled (the
	// cancel raced a completed execution) — never reserved, never duplicated.
	states := env.awaitNoReservedForJob(t, jobID)
	if len(states) > 1 {
		t.Fatalf("ledger rows for job = %v, at most one final row", states)
	}
	for _, s := range states {
		if s != "released" && s != "settled" {
			t.Fatalf("ledger row state %q is not final", s)
		}
	}
}

// TestAsyncE2ELegacyAdminTokenVisibility rehearses the legacy-token
// bootstrap path (the admin RBAC rollback identity) against the real job
// store: the legacy token authenticates as the platform admin on the query
// surface (visibility into any job) but can never cancel (visibility is not
// control).
func TestAsyncE2ELegacyAdminTokenVisibility(t *testing.T) {
	gated := newE2EGatedProvider()
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, providerOverride: gated})

	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello admin","background":true}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create = %d %s", resp.StatusCode, raw)
	}
	select {
	case <-gated.started:
	case <-time.After(10 * time.Second):
		t.Fatal("provider execution never started")
	}

	// Legacy token (not kba_-prefixed): API-key auth fails, the legacy
	// bootstrap identity authenticates as platform admin.
	resp, raw = env.do(t, http.MethodGet, "/v1/responses/"+jobID, e2eLegacyToken, "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"status":"running"`) {
		t.Fatalf("legacy admin GET = %d %s", resp.StatusCode, raw)
	}
	// Cancellation stays with the owning subject.
	resp, raw = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eLegacyToken, "", nil)
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(raw), "insufficient_scope") {
		t.Fatalf("legacy admin cancel = %d %s", resp.StatusCode, raw)
	}

	// Owner cancels; the legacy token keeps visibility of the final state.
	resp, _ = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOwnerKey, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("owner cancel = %d", resp.StatusCode)
	}
	env.awaitTerminalStore(t, jobID)
	resp, raw = env.do(t, http.MethodGet, "/v1/responses/"+jobID, e2eLegacyToken, "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"status":"cancelled"`) {
		t.Fatalf("legacy admin GET after cancel = %d %s", resp.StatusCode, raw)
	}
}

// --- Stage: budgets flag on --------------------------------------------------

// TestAsyncE2EBudgetEnforcementDenies is the budget flag-on progression
// check over the full stack: a configured 1-micro daily budget with a valid
// price denies at admission with 429 budget_exceeded before any job is
// created and before any provider invocation.
func TestAsyncE2EBudgetEnforcementDenies(t *testing.T) {
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, budgetEnforce: true, budgetMicros: 1})

	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello budget","background":true}`)
	if resp.StatusCode != http.StatusTooManyRequests || jobID != "" {
		t.Fatalf("budget denial = %d %s (job %q)", resp.StatusCode, raw, jobID)
	}
	if !strings.Contains(string(raw), `"code":"budget_exceeded"`) {
		t.Fatalf("envelope = %s", raw)
	}
	if n, err := env.jobs.QueueDepth(context.Background()); err != nil || n != 0 {
		t.Fatalf("queue depth = %d err=%v, want no job created", n, err)
	}
	// The other subject has no budget policy: unaffected.
	resp, raw, _ = env.createBackground(t, e2eOtherKey, "",
		`{"model":"`+e2eModel+`","input":"hello budget","background":true}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unbudgeted subject = %d %s", resp.StatusCode, raw)
	}
}

// --- Stage: budgets rollback (enforcement off, ledger retained) --------------

// TestAsyncE2EBudgetsOffStillCapturesLedger is the documented monetary
// rollback point: with the same 1-micro budget configured but enforcement
// off, no request is denied and the completed job still settles exactly one
// priced ledger row — operators keep cost evidence while enforcement is off.
func TestAsyncE2EBudgetsOffStillCapturesLedger(t *testing.T) {
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, budgetEnforce: false, budgetMicros: 1})

	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello rollback","background":true}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create under enforcement-off = %d %s", resp.StatusCode, raw)
	}
	env.pollUntilGetStatus(t, jobID, e2eOwnerKey, "completed")
	states := env.awaitNoReservedForJob(t, jobID)
	if len(states) != 1 || states[0] != "settled" {
		t.Fatalf("ledger rows = %v, want the settled capture row", states)
	}
}

// --- Stage: async rollback (stop intake, drain in-flight) ---------------------

// TestAsyncE2EShutdownDrainCommitsInFlight rehearses the async rollback
// order: stop accepting new work, give the in-flight execution its bounded
// drain window to commit, and lose nothing. The in-flight job completes with
// its stored result and settled ledger row even though intake stopped.
func TestAsyncE2EShutdownDrainCommitsInFlight(t *testing.T) {
	gated := newE2EGatedProvider()
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, providerOverride: gated})

	resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
		`{"model":"`+e2eModel+`","input":"hello drain","background":true}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create = %d %s", resp.StatusCode, raw)
	}
	select {
	case <-gated.started:
	case <-time.After(10 * time.Second):
		t.Fatal("provider execution never started")
	}

	// Rollback: stop intake and drain. Stop blocks until the in-flight
	// execution commits (it finishes the moment the gate opens), so the
	// result and settlement must both be durable when it returns.
	close(gated.release)
	done := make(chan struct{})
	go func() { env.pool.Stop(3 * time.Second); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pool stop with drain did not converge")
	}

	job := env.awaitTerminalStore(t, jobID)
	if job.Status != async.StatusCompleted {
		t.Fatalf("drained job = %s, want completed", job.Status)
	}
	if _, err := env.jobs.Result(context.Background(), jobID); err != nil {
		t.Fatalf("drained job lost its result: %v", err)
	}
	states := env.awaitNoReservedForJob(t, jobID)
	if len(states) != 1 || states[0] != "settled" {
		t.Fatalf("ledger rows after drain = %v", states)
	}
}

// --- Merged-whole race re-verification -----------------------------------------

// TestAsyncE2ECancelCommitRaceSettlesExactlyOnce re-verifies the child-2/3
// cancellation-versus-settlement discipline against the real database, not
// the in-memory mirror: cancel and worker-commit race across deterministic
// interleavings, and every terminal outcome keeps the invariants — exactly
// one terminal state, one released lease, at most one settled ledger row,
// and no reserved residue. This is the store-CAS promise the whole V1.4 job
// lifecycle leans on.
func TestAsyncE2ECancelCommitRaceSettlesExactlyOnce(t *testing.T) {
	// The provider holds every execution for 40ms; the cancel offset sweeps
	// 0..250ms across iterations, so the interleaving matrix covers
	// cancel-before-claim, cancel-during-execution, cancel-at-commit, and
	// commit-then-cancel (the no-op).
	slow := &e2eSlowProvider{delay: 40 * time.Millisecond}
	env := newAsyncE2EEnv(t, e2eStage{asyncEnabled: true, providerOverride: slow})

	cancelOffsets := []time.Duration{
		0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond,
		30 * time.Millisecond, 35 * time.Millisecond, 40 * time.Millisecond,
		45 * time.Millisecond, 50 * time.Millisecond, 60 * time.Millisecond,
		70 * time.Millisecond, 80 * time.Millisecond, 100 * time.Millisecond,
		150 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond,
	}

	var completed, cancelled, settledRows, releasedRows, noLedger int
	var cancels sync.WaitGroup
	for i, offset := range cancelOffsets {
		resp, raw, jobID := env.createBackground(t, e2eOwnerKey, "",
			`{"model":"`+e2eModel+`","input":"race `+fmt.Sprint(i)+`","background":true}`)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("iteration %d create = %d %s", i, resp.StatusCode, raw)
		}

		// Cancel races the execution at the iteration's deterministic offset.
		// Commit-then-cancel is a legitimate interleaving (the no-op), so the
		// goroutine may outlive the job's terminal transition; the WaitGroup
		// keeps its HTTP call inside the server's lifetime.
		cancels.Add(1)
		go func(offset time.Duration, jobID string) {
			defer cancels.Done()
			time.Sleep(offset)
			_, _, _ = env.tryDo(http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOwnerKey, "", nil)
		}(offset, jobID)

		job := env.awaitTerminalStore(t, jobID)
		switch job.Status {
		case async.StatusCompleted:
			completed++
		case async.StatusCancelled:
			cancelled++
		default:
			t.Fatalf("iteration %d terminal status = %s", i, job.Status)
		}
		if job.LeaseOwner != "" {
			t.Fatalf("iteration %d terminal job keeps lease %q", i, job.LeaseOwner)
		}

		states := env.awaitNoReservedForJob(t, jobID)
		settled := 0
		for _, s := range states {
			switch s {
			case "settled":
				settled++
			case "released":
				releasedRows++
			default:
				t.Fatalf("iteration %d unexpected ledger state %q", i, s)
			}
		}
		if settled > 1 {
			t.Fatalf("iteration %d has %d settled ledger rows", i, settled)
		}
		settledRows += settled
		if len(states) == 0 {
			noLedger++ // cancel won before the worker reserved: nothing to bill
		}

		// Idempotency of cancel over the final state.
		resp, _ = env.do(t, http.MethodPost, "/v1/responses/"+jobID+"/cancel", e2eOwnerKey, "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("iteration %d repeat cancel = %d", i, resp.StatusCode)
		}
	}
	cancels.Wait()
	t.Logf("race outcomes: completed=%d cancelled=%d settledRows=%d releasedRows=%d noLedgerRows=%d",
		completed, cancelled, settledRows, releasedRows, noLedger)
	if completed == 0 || cancelled == 0 {
		t.Fatalf("race coverage degenerate: completed=%d cancelled=%d — adjust delays", completed, cancelled)
	}
}

// --- Providers -----------------------------------------------------------------

// e2eGatedProvider blocks Complete until released; cancellation is reported
// on a dedicated channel so the test can assert propagation.
type e2eGatedProvider struct {
	started        chan struct{}
	release        chan struct{}
	cancelObserved chan struct{}
	once           sync.Once
	cancelOnce     sync.Once
}

func newE2EGatedProvider() *e2eGatedProvider {
	return &e2eGatedProvider{
		started:        make(chan struct{}),
		release:        make(chan struct{}),
		cancelObserved: make(chan struct{}),
	}
}

func (p *e2eGatedProvider) Name() string { return "e2e-gated" }

func (p *e2eGatedProvider) Capabilities(string) model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}
}

func (p *e2eGatedProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *e2eGatedProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return provider.Fake{}.Complete(ctx, req)
	case <-ctx.Done():
		p.cancelOnce.Do(func() { close(p.cancelObserved) })
		return model.Response{}, ctx.Err()
	}
}

func (p *e2eGatedProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return provider.Fake{}.Stream(ctx, req, emit)
}

// e2eSlowProvider delays Complete by a fixed duration so cancel and commit
// interleave deterministically; otherwise it behaves like Fake.
type e2eSlowProvider struct {
	delay time.Duration
}

func (p *e2eSlowProvider) Name() string { return "e2e-slow" }

func (p *e2eSlowProvider) Capabilities(string) model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Usage: true}
}

func (p *e2eSlowProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return provider.Fake{}.Embeddings(ctx, req)
}

func (p *e2eSlowProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return model.Response{}, ctx.Err()
	}
	return provider.Fake{}.Complete(ctx, req)
}

func (p *e2eSlowProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return provider.Fake{}.Stream(ctx, req, emit)
}
