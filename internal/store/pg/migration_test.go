package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// testDatabaseLockKey serializes this package's env-gated integration test
// against the cmd/gateway database-backed startup test, which runs under the
// same PostgreSQL advisory lock so concurrent `go test ./...` package runs
// cannot race on the shared TEST_DATABASE_URL schema. Keep both constants
// identical.
const testDatabaseLockKey int64 = 721534891

// lockTestDatabase takes a session-level advisory lock on one pinned
// connection for the duration of the test.
func lockTestDatabase(t *testing.T, dsn string) {
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
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", testDatabaseLockKey); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", testDatabaseLockKey)
		_ = conn.Close()
		_ = db.Close()
	})
}

// newTestMigrator builds the same golang-migrate instance the cmd/migrate CLI
// uses, pointed at the repository's migrations directory.
func newTestMigrator(t *testing.T, db *sql.DB) *migrate.Migrate {
	t.Helper()
	driver, err := pgx5.WithInstance(db, &pgx5.Config{})
	if err != nil {
		t.Fatalf("migrate driver: %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		migrationsDirURL(t), "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate instance: %v", err)
	}
	return m
}

func requireVersion(t *testing.T, m *migrate.Migrate, want uint, dirty bool) {
	t.Helper()
	version, gotDirty, err := m.Version()
	if err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != want || gotDirty != dirty {
		t.Fatalf("schema version = %d (dirty=%v), want %d (dirty=%v)", version, gotDirty, want, dirty)
	}
}

// TestMigrationsAndStores applies every forward migration through the
// versioned migration tool (fresh up to the latest version, second up as a
// no-op), exercises the key lifecycle, audit, and V1.2 management stores on
// the real schema, then rolls every boundary back down to the empty database
// (0011 through 0001) verifying each down script removes exactly its own
// artifacts, and re-applies the whole chain to confirm version tracking. It
// requires a real PostgreSQL instance and is skipped when TEST_DATABASE_URL
// is not set. The V1.4-specific upgrade path and schema constraints live in
// TestVersion5UpgradePath and TestV14SchemaConstraints.
func TestMigrationsAndStores(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL migration test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)

	// The migration tool owns schema changes: clean slate including its
	// bookkeeping table, then drive it like cmd/migrate does. The list covers
	// every table any migration (0001-0010) can create, dependents first.
	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = mdb.ExecContext(ctx, `DROP TABLE IF EXISTS data_exports, archive_runs, retention_policies,
		admin_credentials, budget_policies, usage_ledger, pricing_catalog,
		async_job_requests, idempotency_keys, async_job_results, async_jobs,
		llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers, admin_audit CASCADE`)

	m := newTestMigrator(t, mdb)
	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, m, 11, false)
	if err := m.Up(); !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("second up must be a no-op, got %v", err)
	}

	// Every V1.4 table from 0006-0009 exists after the full up.
	var v14Tables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('async_jobs', 'async_job_results', 'idempotency_keys',
		                     'pricing_catalog', 'usage_ledger', 'budget_policies',
		                     'admin_credentials', 'retention_policies', 'archive_runs',
		                     'data_exports')`).Scan(&v14Tables); err != nil {
		t.Fatalf("v1.4 table check: %v", err)
	}
	if v14Tables != 10 {
		t.Fatalf("V1.4 table count = %d, want 10", v14Tables)
	}

	// Store coverage on the migrated schema, through the production pool.
	pgw, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	defer pgw.Close()
	if err := pgw.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}

	// Key lifecycle on the persisted store.
	gen, err := auth.NewManager(pgw).Create(ctx, "subject_default", "tenant_default", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	principal, err := (&Authenticator{DB: pgw, Now: time.Now}).Authenticate(gen.Plaintext, time.Now())
	if err != nil || principal.SubjectID != "subject_default" || principal.KeyID != gen.Record.ID {
		t.Fatalf("authenticate: principal=%+v err=%v", principal, err)
	}
	if _, err := pgw.ResolveAuth(ctx, "kb_wrong", time.Now()); err != auth.ErrInvalid {
		t.Fatalf("wrong key must be invalid, got %v", err)
	}

	// Audit write with unknown usage stays NULL (never zero), and the
	// protocol label persists for management correlation.
	ev := audit.Event{
		RequestID: "req_test_1", SubjectID: "subject_default", KeyID: gen.Record.ID,
		Model: "gateway-echo", Provider: "fake-primary", Status: 200,
		LatencyMillis: 5, Streaming: false, CreatedAt: time.Now(),
		TraceID: "req_test_1", RouteAttempts: 1, Protocol: "responses",
	}
	if err := pgw.WriteAudit(ctx, ev); err != nil {
		t.Fatalf("write audit: %v", err)
	}
	var promptTokens *int
	var protocol string
	if err := pgw.Pool.QueryRow(ctx,
		`SELECT prompt_tokens, COALESCE(protocol,'') FROM llm_requests WHERE request_id='req_test_1'`).Scan(&promptTokens, &protocol); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if promptTokens != nil {
		t.Fatal("unknown usage must persist as NULL, not zero")
	}
	if protocol != "responses" {
		t.Fatalf("protocol = %q, want responses", protocol)
	}

	// V1.3 pipeline: first-token latency persists for streams, stays NULL when
	// unmeasured, and feeds true percentiles in the usage query.
	streamEv := audit.Event{
		RequestID: "req_test_stream", SubjectID: "subject_default", KeyID: gen.Record.ID,
		Model: "gateway-echo", Provider: "fake-primary", Status: 200,
		LatencyMillis: 30, FirstTokenMillis: int64Ptr(12), Streaming: true,
		CreatedAt: time.Now(), TraceID: "req_test_stream", RouteAttempts: 1, Protocol: "chat",
	}
	if err := pgw.WriteAudit(ctx, streamEv); err != nil {
		t.Fatalf("write stream audit: %v", err)
	}
	var persisted *int64
	if err := pgw.Pool.QueryRow(ctx,
		`SELECT first_token_millis FROM llm_requests WHERE request_id='req_test_stream'`).Scan(&persisted); err != nil {
		t.Fatalf("read stream audit: %v", err)
	}
	if persisted == nil || *persisted != 12 {
		t.Fatalf("first_token_millis = %v, want 12", persisted)
	}
	var unmeasured *int64
	if err := pgw.Pool.QueryRow(ctx,
		`SELECT first_token_millis FROM llm_requests WHERE request_id='req_test_1'`).Scan(&unmeasured); err != nil {
		t.Fatalf("read non-stream audit: %v", err)
	}
	if unmeasured != nil {
		t.Fatalf("unmeasured first token must persist as NULL, got %d", *unmeasured)
	}
	queried, err := pgw.QueryAudit(ctx, mgmt.AuditFilter{RequestID: "req_test_stream"})
	if err != nil || len(queried) != 1 || queried[0].FirstTokenMillis == nil || *queried[0].FirstTokenMillis != 12 {
		t.Fatalf("audit query first token = %+v err=%v", queried, err)
	}
	usageRows, err := pgw.Usage(ctx, mgmt.AuditFilter{Model: "gateway-echo"})
	if err != nil || len(usageRows) != 2 {
		t.Fatalf("usage rows = %+v err=%v", usageRows, err)
	}
	var chatRow *mgmt.UsageRow
	for i := range usageRows {
		if usageRows[i].Protocol == "chat" {
			chatRow = &usageRows[i]
		}
	}
	if chatRow == nil || chatRow.FirstTokenP50Millis == nil || *chatRow.FirstTokenP50Millis != 12 {
		t.Fatalf("usage chat row first-token p50 = %+v, want 12 (true percentile over measured rows)", chatRow)
	}
	if chatRow.CostMicros != nil {
		t.Fatalf("cost must stay null while no pricing data exists: %v", chatRow.CostMicros)
	}

	// V1.2: the seeded model declares its full capability matrix, and the
	// model-control-plane migration (0005) adds embeddings with a declared
	// dimension plus a demo retrieval profile.
	catalog, err := pgw.LoadCatalog(ctx)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if len(catalog) != 1 || catalog[0].PublicName != "gateway-echo" {
		t.Fatalf("catalog = %+v", catalog)
	}
	caps := catalog[0].Capabilities
	if !caps.Chat || !caps.Responses || !caps.Tools || !caps.StructuredOutput || !caps.Stream || !caps.Usage {
		t.Fatalf("seed capabilities incomplete: %+v", caps)
	}
	if caps.Vision || caps.Reasoning {
		t.Fatalf("seed capabilities must not claim vision/reasoning: %+v", caps)
	}
	if !caps.Embeddings || caps.EmbeddingDim != 256 {
		t.Fatalf("0005 must seed embeddings with a declared dim: %+v", caps)
	}
	if len(catalog[0].RetrievalProfile) == 0 {
		t.Fatal("0005 must seed a demo retrieval profile")
	}
	var profile map[string]float64
	if err := json.Unmarshal(catalog[0].RetrievalProfile, &profile); err != nil {
		t.Fatalf("retrieval profile = %s", catalog[0].RetrievalProfile)
	}
	if profile["vector_max_distance"] == 0 {
		t.Fatalf("retrieval profile content = %s", catalog[0].RetrievalProfile)
	}
	if catalog[0].ConfigVersion < 1 {
		t.Fatalf("config version not loaded: %d", catalog[0].ConfigVersion)
	}

	// V1.2 management queries round trip.
	events, err := pgw.QueryAudit(ctx, mgmt.AuditFilter{RequestID: "req_test_1"})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if len(events) != 1 || events[0].Protocol != "responses" || events[0].TraceID != "req_test_1" {
		t.Fatalf("audit query = %+v", events)
	}
	usage, err := pgw.Usage(ctx, mgmt.AuditFilter{Model: "gateway-echo"})
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	var responsesRow *mgmt.UsageRow
	for i := range usage {
		if usage[i].Protocol == "responses" {
			responsesRow = &usage[i]
		}
	}
	if responsesRow == nil || responsesRow.Requests != 1 {
		t.Fatalf("usage responses row = %+v", responsesRow)
	}
	models, err := pgw.Models(ctx)
	if err != nil || len(models) != 1 || models[0].Provider != "fake-primary" {
		t.Fatalf("admin models = %+v err=%v", models, err)
	}
	providers, err := pgw.Providers(ctx)
	if err != nil || len(providers) != 2 {
		t.Fatalf("admin providers = %+v err=%v", providers, err)
	}
	policies, err := pgw.Policies(ctx, "subject_default")
	if err != nil || len(policies) != 1 || policies[0].PublicModel != "gateway-echo" {
		t.Fatalf("policies = %+v err=%v", policies, err)
	}

	// Input quota safety: access_policies.max_input_tokens loads as the
	// subject input ceiling; NULL ceilings collapse to zero (unset). Rows are
	// removed again so the down migration below stays clean.
	if _, err := pgw.Pool.Exec(ctx,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_input_cap', 'tenant_default')`); err != nil {
		t.Fatalf("insert subject: %v", err)
	}
	if _, err := pgw.Pool.Exec(ctx,
		`INSERT INTO access_policies
			(subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens, max_input_tokens, max_output_tokens)
		 VALUES ('subject_input_cap', 'gateway-echo', 30, 2, 5000, 4096, 256)`); err != nil {
		t.Fatalf("insert policy: %v", err)
	}

	// Default-model slots: the atomic mutation writes the column and the
	// management-op row in one transaction; the loader carries the slots.
	if err := pgw.SetDefaultModelWithAudit(ctx, "subject_input_cap", "gateway-echo", "chat", mgmt.AdminOp{
		Action: "default_model_chat", Target: "subject_input_cap", Detail: json.RawMessage(`{"model":"gateway-echo","kind":"chat"}`),
	}); err != nil {
		t.Fatalf("set chat default: %v", err)
	}
	if err := pgw.SetDefaultModelWithAudit(ctx, "subject_input_cap", "gateway-echo", "embedding", mgmt.AdminOp{
		Action: "default_model_embedding", Target: "subject_input_cap",
	}); err != nil {
		t.Fatalf("set embedding default: %v", err)
	}
	limits, err := pgw.LoadLimits(ctx)
	if err != nil {
		t.Fatalf("load limits: %v", err)
	}
	if l := limits["subject_input_cap"]; l.MaxInputTokens != 4096 || l.MaxOutputTokens != 256 || l.DailyTokens != 5000 {
		t.Fatalf("subject_input_cap limits = %+v", l)
	}
	if l := limits["subject_input_cap"]; l.DefaultModel != "gateway-echo" || l.DefaultEmbeddingModel != "gateway-echo" {
		t.Fatalf("default slots = %+v", l)
	}
	if l := limits["subject_default"]; l.DefaultModel != "" || l.DefaultEmbeddingModel != "" {
		t.Fatalf("NULL defaults must load as empty, got %+v", l)
	}
	policies, err2 := pgw.Policies(ctx, "subject_input_cap")
	if err2 != nil || len(policies) == 0 || policies[0].DefaultModel != "gateway-echo" || policies[0].DefaultEmbeddingModel != "gateway-echo" {
		t.Fatalf("policies view defaults = %+v err=%v", policies, err2)
	}
	// Atomicity: a failing audit insert must roll the slot write back. The
	// attempted write targets a different model, so a successful commit would
	// be observable in the slot value.
	if _, err := pgw.Pool.Exec(ctx,
		`INSERT INTO model_catalog (public_name, provider, upstream_model, capabilities)
		 VALUES ('gateway-echo-2', 'fake-primary', 'echo-model-2', '{}')`); err != nil {
		t.Fatalf("insert second model: %v", err)
	}
	if err := pgw.SetDefaultModelWithAudit(ctx, "subject_input_cap", "gateway-echo-2", "chat", mgmt.AdminOp{
		Action: "default_model_chat", Target: "subject_input_cap", Detail: json.RawMessage(`not-valid-json`),
	}); err == nil {
		t.Fatal("atomic default-model mutation must fail when the audit insert fails")
	}
	fresh, err := pgw.LoadLimits(ctx)
	if err != nil {
		t.Fatalf("reload limits: %v", err)
	}
	if l := fresh["subject_input_cap"]; l.DefaultModel != "gateway-echo" {
		t.Fatalf("failed mutation must not overwrite the slot: %+v", l)
	}
	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM model_catalog WHERE public_name = 'gateway-echo-2'`); err != nil {
		t.Fatalf("cleanup second model: %v", err)
	}
	// Unknown model and unknown subject fail without writing anything.
	if err := pgw.SetDefaultModelWithAudit(ctx, "subject_input_cap", "no-such-model", "chat", mgmt.AdminOp{Action: "default_model_chat"}); err != mgmt.ErrNotFound {
		t.Fatalf("unknown model must be ErrNotFound, got %v", err)
	}
	if err := pgw.SetDefaultModelWithAudit(ctx, "no-such-subject", "gateway-echo", "chat", mgmt.AdminOp{Action: "default_model_chat"}); err != mgmt.ErrNotFound {
		t.Fatalf("unknown subject must be ErrNotFound, got %v", err)
	}
	if err := pgw.SetDefaultModelWithAudit(ctx, "subject_input_cap", "gateway-echo", "weird", mgmt.AdminOp{Action: "x"}); err == nil {
		t.Fatal("unknown kind must fail")
	}

	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM access_policies WHERE subject_id = 'subject_input_cap'`); err != nil {
		t.Fatalf("cleanup policy: %v", err)
	}
	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM subjects WHERE id = 'subject_input_cap'`); err != nil {
		t.Fatalf("cleanup subject: %v", err)
	}

	// Multi-row policy collapse (issue #7 + issue #8): a subject with several
	// access_policies rows keeps defaults declared on any of them — the
	// loader takes the first non-empty slot value in id order, chat and
	// embedding judged independently — while each ceiling folds to the
	// minimum declared value across the rows (a NULL cap on one row does not
	// constrain; a field no row declares stays zero/uncapped). Rows are
	// inserted in ascending id order.
	if _, err := pgw.Pool.Exec(ctx,
		`INSERT INTO model_catalog (public_name, provider, upstream_model, capabilities)
		 VALUES ('gateway-echo-2', 'fake-primary', 'echo-model-2', '{}')`); err != nil {
		t.Fatalf("insert second model: %v", err)
	}
	if _, err := pgw.Pool.Exec(ctx,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_multi_row', 'tenant_default'),
			('subject_row1_defaults', 'tenant_default'), ('subject_null_defaults', 'tenant_default')`); err != nil {
		t.Fatalf("insert multi-row subjects: %v", err)
	}
	// subject_multi_row: chat default on row 1 (NULL later), embedding default
	// on row 2 (NULL on row 1) — both must resolve; ceilings fold to the
	// minimum declared (row 1), and max_input_tokens declared only on row 2
	// must constrain even though row 1 leaves it NULL (NULL passthrough).
	if _, err := pgw.Pool.Exec(ctx, `
		INSERT INTO access_policies
			(subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens, max_input_tokens, default_model, default_embedding_model)
		VALUES
			('subject_multi_row', 'gateway-echo',   30, 3, 5000, NULL, 'gateway-echo',   NULL),
			('subject_multi_row', 'gateway-echo-2', 60, 6, 9000, 2048, NULL,             'gateway-echo-2')`); err != nil {
		t.Fatalf("insert subject_multi_row policies: %v", err)
	}
	// subject_row1_defaults: both defaults on row 1, both NULL on row 2 —
	// row 1's slots must survive the later NULL rows.
	if _, err := pgw.Pool.Exec(ctx, `
		INSERT INTO access_policies
			(subject_id, public_model, rate_per_minute, max_concurrent, default_model, default_embedding_model)
		VALUES
			('subject_row1_defaults', 'gateway-echo',   10, 1, 'gateway-echo',   'gateway-echo'),
			('subject_row1_defaults', 'gateway-echo-2', 20, 2, NULL,             NULL)`); err != nil {
		t.Fatalf("insert subject_row1_defaults policies: %v", err)
	}
	// subject_null_defaults: every row all-NULL for the optional slots — no
	// default resolves and the model-less request keeps its stable 400 path;
	// the NOT NULL ceilings fold to the minimum of the declared values.
	if _, err := pgw.Pool.Exec(ctx, `
		INSERT INTO access_policies
			(subject_id, public_model, rate_per_minute, max_concurrent)
		VALUES
			('subject_null_defaults', 'gateway-echo',   11, 1),
			('subject_null_defaults', 'gateway-echo-2', 22, 2)`); err != nil {
		t.Fatalf("insert subject_null_defaults policies: %v", err)
	}
	multiLimits, err := pgw.LoadLimits(ctx)
	if err != nil {
		t.Fatalf("load multi-row limits: %v", err)
	}
	if l := multiLimits["subject_multi_row"]; l.DefaultModel != "gateway-echo" || l.DefaultEmbeddingModel != "gateway-echo-2" {
		t.Fatalf("multi-row defaults must be first-non-empty per slot, got %+v", l)
	}
	if l := multiLimits["subject_multi_row"]; l.RatePerMinute != 30 || l.MaxConcurrent != 3 || l.DailyTokens != 5000 {
		t.Fatalf("multi-row ceilings must fold to the minimum declared, got %+v", l)
	}
	if l := multiLimits["subject_multi_row"]; l.MaxInputTokens != 2048 {
		t.Fatalf("a cap declared on one row must constrain despite NULLs on others, got %+v", l)
	}
	if l := multiLimits["subject_row1_defaults"]; l.DefaultModel != "gateway-echo" || l.DefaultEmbeddingModel != "gateway-echo" {
		t.Fatalf("row-1 defaults must survive NULL slots on later rows, got %+v", l)
	}
	if l := multiLimits["subject_row1_defaults"]; l.RatePerMinute != 10 || l.MaxConcurrent != 1 {
		t.Fatalf("row1_defaults ceilings must fold to the minimum declared, got %+v", l)
	}
	if l := multiLimits["subject_row1_defaults"]; l.DailyTokens != 0 || l.MonthlyTokens != 0 || l.MaxInputTokens != 0 || l.MaxOutputTokens != 0 {
		t.Fatalf("nullable caps no row declares must stay zero (uncapped), got %+v", l)
	}
	if l := multiLimits["subject_null_defaults"]; l.DefaultModel != "" || l.DefaultEmbeddingModel != "" {
		t.Fatalf("all-NULL rows must keep no default (400 path unchanged), got %+v", l)
	}
	if l := multiLimits["subject_null_defaults"]; l.RatePerMinute != 11 || l.MaxConcurrent != 1 {
		t.Fatalf("null_defaults ceilings must fold to the minimum declared, got %+v", l)
	}

	// The admin policies view must expose the folded effective ceilings per
	// row (issue #8 mitigation), identical on every row of the subject and
	// equal to what LoadLimits enforces.
	multiPolicies, err := pgw.Policies(ctx, "subject_multi_row")
	if err != nil || len(multiPolicies) != 2 {
		t.Fatalf("multi-row policies view = %+v err=%v", multiPolicies, err)
	}
	wantEffective := mgmt.EffectiveLimits{
		RatePerMinute: 30, MaxConcurrent: 3, DailyTokens: 5000,
		MonthlyTokens: 0, MaxInputTokens: 2048, MaxOutputTokens: 0,
	}
	for _, p := range multiPolicies {
		if p.EffectiveLimits != wantEffective {
			t.Fatalf("policies view effective limits = %+v, want %+v", p.EffectiveLimits, wantEffective)
		}
	}
	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM access_policies WHERE subject_id IN
		('subject_multi_row', 'subject_row1_defaults', 'subject_null_defaults')`); err != nil {
		t.Fatalf("cleanup multi-row policies: %v", err)
	}
	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM subjects WHERE id IN
		('subject_multi_row', 'subject_row1_defaults', 'subject_null_defaults')`); err != nil {
		t.Fatalf("cleanup multi-row subjects: %v", err)
	}
	if _, err := pgw.Pool.Exec(ctx, `DELETE FROM model_catalog WHERE public_name = 'gateway-echo-2'`); err != nil {
		t.Fatalf("cleanup second model: %v", err)
	}

	// Model enable/disable persists atomically with its management op, and
	// the runtime refresh path can re-read the row regardless of state.
	if err := pgw.SetModelEnabledWithAudit(ctx, "gateway-echo", false, mgmt.AdminOp{
		Action: "model_disable", Target: "gateway-echo", Detail: json.RawMessage(`{"enabled":false}`),
	}); err != nil {
		t.Fatalf("disable model: %v", err)
	}
	if cat2, _ := pgw.LoadCatalog(ctx); len(cat2) != 0 {
		t.Fatal("disabled model must disappear from the enabled catalog")
	}
	entry, found, err := pgw.ModelEntry(ctx, "gateway-echo")
	if err != nil || !found || entry.Enabled {
		t.Fatalf("model entry = %+v found=%v err=%v, want the disabled row", entry, found, err)
	}
	if err := pgw.SetModelEnabledWithAudit(ctx, "gateway-echo", true, mgmt.AdminOp{
		Action: "model_enable", Target: "gateway-echo",
	}); err != nil {
		t.Fatalf("enable model: %v", err)
	}

	// Atomicity: a failing audit insert must roll the mutation back, leaving
	// the model enabled and no failed-operation trace behind. Invalid JSON in
	// the detail column forces the admin_audit insert to fail inside the
	// transaction.
	if err := pgw.SetModelEnabledWithAudit(ctx, "gateway-echo", false, mgmt.AdminOp{
		Action: "model_disable", Target: "gateway-echo", Detail: json.RawMessage(`not-valid-json`),
	}); err == nil {
		t.Fatal("atomic mutation must fail when the audit insert fails")
	}
	if entry2, _, err := pgw.ModelEntry(ctx, "gateway-echo"); err != nil || !entry2.Enabled {
		t.Fatalf("failed mutation must not disable the model: entry=%+v err=%v", entry2, err)
	}
	if err := pgw.WriteOp(ctx, mgmt.AdminOp{Action: "model_disable", Target: "gateway-echo", Detail: json.RawMessage(`{"enabled":false}`)}); err != nil {
		t.Fatalf("write op: %v", err)
	}
	// Both atomic toggles and the explicit op are recorded; the aborted
	// mutation above is not.
	// Both atomic toggles and the explicit op are recorded; the aborted
	// mutations above are not. The two default-model mutations from the 0005
	// coverage above bring the total to 5.
	ops, err := pgw.Ops(ctx, 10)
	if err != nil || len(ops) != 5 || ops[0].Action != "model_disable" || ops[0].Target != "gateway-echo" {
		t.Fatalf("ops = %+v err=%v, want 5 with the explicit disable newest", ops, err)
	}
	if ops[0].AdminSubject == "" {
		t.Fatal("management op must default its admin subject")
	}

	// Roll back each V1.4 migration one boundary at a time and confirm every
	// down script removes exactly its own artifacts.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0011: %v", err)
	}
	requireVersion(t, m, 10, false)
	var backoffArtifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.columns
		        WHERE table_name = 'async_jobs' AND column_name = 'visible_at')
		     + (SELECT count(*) FROM pg_indexes
		        WHERE indexname = 'idx_async_jobs_claim_ready')`).Scan(&backoffArtifacts); err != nil {
		t.Fatalf("0011 down check: %v", err)
	}
	if backoffArtifacts != 0 {
		t.Fatal("0011 down migration left the backoff column or claim index behind")
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0010: %v", err)
	}
	requireVersion(t, m, 9, false)
	var requestTables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('async_job_requests')`).Scan(&requestTables); err != nil {
		t.Fatalf("0010 down check: %v", err)
	}
	if requestTables != 0 {
		t.Fatal("0010 down migration left async_job_requests behind")
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0009: %v", err)
	}
	requireVersion(t, m, 8, false)
	var lifecycleTables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('retention_policies','archive_runs','data_exports')`).Scan(&lifecycleTables); err != nil {
		t.Fatalf("0009 down check: %v", err)
	}
	if lifecycleTables != 0 {
		t.Fatal("0009 down migration left lifecycle metadata tables behind")
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0008: %v", err)
	}
	requireVersion(t, m, 7, false)
	var adminTables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('admin_credentials')`).Scan(&adminTables); err != nil {
		t.Fatalf("0008 down check: %v", err)
	}
	if adminTables != 0 {
		t.Fatal("0008 down migration left admin_credentials behind")
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0007: %v", err)
	}
	requireVersion(t, m, 6, false)
	var costTables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('pricing_catalog','usage_ledger','budget_policies')`).Scan(&costTables); err != nil {
		t.Fatalf("0007 down check: %v", err)
	}
	if costTables != 0 {
		t.Fatal("0007 down migration left cost governance tables behind")
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0006: %v", err)
	}
	requireVersion(t, m, 5, false)
	var asyncArtifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.tables
		        WHERE table_name IN ('async_jobs','async_job_results','idempotency_keys'))
		     + (SELECT count(*) FROM information_schema.table_constraints
		        WHERE constraint_name='uq_subjects_id_tenant')`).Scan(&asyncArtifacts); err != nil {
		t.Fatalf("0006 down check: %v", err)
	}
	if asyncArtifacts != 0 {
		t.Fatalf("0006 down migration left async artifacts behind: %d", asyncArtifacts)
	}

	// Roll back 0005 through the tool and confirm the model-control-plane
	// artifacts are gone.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0005: %v", err)
	}
	requireVersion(t, m, 4, false)
	var controlPlaneArtifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='access_policies' AND column_name IN ('default_model','default_embedding_model'))
		     + (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='model_catalog' AND column_name='retrieval_profile')`).Scan(&controlPlaneArtifacts); err != nil {
		t.Fatalf("0005 down check: %v", err)
	}
	if controlPlaneArtifacts != 0 {
		t.Fatal("0005 down migration left default-model or retrieval_profile columns behind")
	}

	// Roll back 0004 and confirm the first-token column is gone.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0004: %v", err)
	}
	requireVersion(t, m, 3, false)
	var firstTokenCols int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name='llm_requests' AND column_name='first_token_millis'`).Scan(&firstTokenCols); err != nil {
		t.Fatalf("0004 down check: %v", err)
	}
	if firstTokenCols != 0 {
		t.Fatal("0004 down migration left first_token_millis behind")
	}

	// Roll back 0003 and confirm the V1.2 artifacts are gone while the V1.1
	// columns remain.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0003: %v", err)
	}
	requireVersion(t, m, 2, false)
	var artifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='llm_requests' AND column_name IN ('trace_id','cost_micros','route_attempts'))
		     + (SELECT count(*) FROM information_schema.tables
		        WHERE table_name IN ('admin_audit'))`).Scan(&artifacts); err != nil {
		t.Fatalf("artifact check: %v", err)
	}
	if artifacts != 3 {
		// trace_id, cost_micros, route_attempts remain (v1.1); protocol and
		// admin_audit must be gone.
		t.Fatalf("down migration left unexpected artifacts: %d", artifacts)
	}

	// Roll back 0002: the V1.1 production tables and lifecycle columns are
	// gone and only the 0001 baseline remains.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0002: %v", err)
	}
	requireVersion(t, m, 1, false)
	var v11Artifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.tables
		        WHERE table_name IN ('providers','model_routes'))
		     + (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='llm_requests' AND column_name IN ('trace_id','cost_micros','route_attempts'))
		     + (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='api_keys' AND column_name IN ('tenant_id','rotated_from','revoked_at'))`).Scan(&v11Artifacts); err != nil {
		t.Fatalf("0002 down check: %v", err)
	}
	if v11Artifacts != 0 {
		t.Fatalf("0002 down migration left V1.1 artifacts behind: %d", v11Artifacts)
	}

	// Roll back 0001: the database is completely empty.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back 0001: %v", err)
	}
	if _, _, err := m.Version(); !errors.Is(err, migrate.ErrNilVersion) {
		t.Fatalf("after full rollback Version() = %v, want ErrNilVersion", err)
	}
	var baselineTables int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('tenants','subjects','api_keys','model_catalog','access_policies','llm_requests')`).Scan(&baselineTables); err != nil {
		t.Fatalf("0001 down check: %v", err)
	}
	if baselineTables != 0 {
		t.Fatalf("0001 down migration left %d baseline tables behind", baselineTables)
	}

	// Re-apply forward to prove version tracking recovers cleanly through the
	// whole chain (0001-0011).
	if err := m.Up(); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	requireVersion(t, m, 11, false)
}

// TestVersion5UpgradePath proves the V1.4 rollout contract: an existing
// version-5 (V1.3) deployment upgrades in place — the version-5 schema is
// startable and readable by the V1.3 gateway store paths before any V1.4
// migration runs, and applying 0006-0009 on top keeps every V1.3 artifact
// intact. It requires a real PostgreSQL instance.
func TestVersion5UpgradePath(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL migration test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)

	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = mdb.ExecContext(ctx, `DROP TABLE IF EXISTS data_exports, archive_runs, retention_policies,
		admin_credentials, budget_policies, usage_ledger, pricing_catalog,
		async_job_requests, idempotency_keys, async_job_results, async_jobs,
		llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers, admin_audit CASCADE`)

	m := newTestMigrator(t, mdb)

	// Migrate to exactly version 5 — the shipped V1.3 schema — and prove the
	// gateway store paths work against it with zero V1.4 knowledge.
	if err := m.Migrate(5); err != nil {
		t.Fatalf("migrate to version 5: %v", err)
	}
	requireVersion(t, m, 5, false)
	pgw, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect at version 5: %v", err)
	}
	catalog, err := pgw.LoadCatalog(ctx)
	if err != nil || len(catalog) != 1 || catalog[0].PublicName != "gateway-echo" {
		t.Fatalf("version-5 catalog = %+v err=%v", catalog, err)
	}
	if _, err := pgw.LoadLimits(ctx); err != nil {
		t.Fatalf("version-5 limits load: %v", err)
	}
	pgw.Close()

	// Apply the V1.4 chain on top: pure upgrade, V1.3 artifacts intact.
	if err := m.Up(); err != nil {
		t.Fatalf("upgrade 5 -> head: %v", err)
	}
	requireVersion(t, m, 11, false)
	var v13Artifacts int
	if err := mdb.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='llm_requests' AND column_name IN ('first_token_millis','protocol','trace_id'))
		     + (SELECT count(*) FROM information_schema.columns
		        WHERE table_name='access_policies' AND column_name IN ('default_model','default_embedding_model'))`).Scan(&v13Artifacts); err != nil {
		t.Fatalf("v1.3 artifact check: %v", err)
	}
	if v13Artifacts != 5 {
		t.Fatalf("upgrade must keep V1.3 columns intact, found %d/5", v13Artifacts)
	}
	// And the gateway store paths still work after the upgrade.
	pgw, err = Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect at head: %v", err)
	}
	defer pgw.Close()
	catalog, err = pgw.LoadCatalog(ctx)
	if err != nil || len(catalog) != 1 || catalog[0].PublicName != "gateway-echo" {
		t.Fatalf("post-upgrade catalog = %+v err=%v", catalog, err)
	}
}

// TestV14SchemaConstraints proves the V1.4 invariants are enforced at the
// database boundary: closed state sets, duplicate idempotency and final
// settlement keys, cross-owner foreign keys, negative amounts, malformed
// scopes, and degenerate lifecycle timestamps all fail to insert. It requires
// a real PostgreSQL instance.
func TestV14SchemaConstraints(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL migration test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)

	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = mdb.ExecContext(ctx, `DROP TABLE IF EXISTS data_exports, archive_runs, retention_policies,
		admin_credentials, budget_policies, usage_ledger, pricing_catalog,
		async_job_requests, idempotency_keys, async_job_results, async_jobs,
		llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers, admin_audit CASCADE`)

	m := newTestMigrator(t, mdb)
	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, m, 11, false)

	// Fixture rows: a second tenant so cross-owner attempts have a target.
	for _, stmt := range []string{
		`INSERT INTO tenants (id, name) VALUES ('tenant_other', 'Other Tenant')`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_other', 'tenant_other')`,
		`INSERT INTO model_catalog (public_name, provider, upstream_model, capabilities)
			VALUES ('model-constraint', 'fake-primary', 'upstream-constraint', '{}')`,
		`INSERT INTO async_jobs (job_id, subject_id, tenant_id, protocol, public_model, request_digest, status)
			VALUES ('job_ok', 'subject_default', 'tenant_default', 'responses', 'gateway-echo', 'digest-ok', 'queued')`,
	} {
		if _, err := mdb.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("fixture: %v: %v", stmt, err)
		}
	}

	// rejected runs stmt and asserts it fails with a PostgreSQL constraint
	// violation (check, unique, or foreign key) rather than inserting.
	rejected := func(t *testing.T, name, stmt string, args ...any) {
		t.Helper()
		if _, err := mdb.ExecContext(ctx, stmt, args...); err == nil {
			t.Fatalf("%s: insert must be rejected at the database boundary", name)
		}
	}

	// --- 0006 async core -----------------------------------------------------
	rejected(t, "invalid job status", `INSERT INTO async_jobs
		(job_id, subject_id, tenant_id, protocol, public_model, request_digest, status)
		VALUES ('job_bad_status', 'subject_default', 'tenant_default', 'responses', 'gateway-echo', 'd1', 'waiting')`)
	rejected(t, "invalid job protocol", `INSERT INTO async_jobs
		(job_id, subject_id, tenant_id, protocol, public_model, request_digest, status)
		VALUES ('job_bad_proto', 'subject_default', 'tenant_default', 'completions', 'gateway-echo', 'd2', 'queued')`)
	rejected(t, "job subject from another tenant (cross-owner FK)", `INSERT INTO async_jobs
		(job_id, subject_id, tenant_id, protocol, public_model, request_digest, status)
		VALUES ('job_cross', 'subject_other', 'tenant_default', 'responses', 'gateway-echo', 'd3', 'queued')`)
	rejected(t, "unknown job subject", `INSERT INTO async_jobs
		(job_id, subject_id, tenant_id, protocol, public_model, request_digest, status)
		VALUES ('job_nosubj', 'subject_missing', 'tenant_default', 'responses', 'gateway-echo', 'd4', 'queued')`)
	rejected(t, "negative attempt count", `INSERT INTO async_jobs
		(job_id, subject_id, tenant_id, protocol, public_model, request_digest, status, attempt_count)
		VALUES ('job_neg', 'subject_default', 'tenant_default', 'responses', 'gateway-echo', 'd5', 'queued', -1)`)

	if _, err := mdb.ExecContext(ctx, `INSERT INTO idempotency_keys
		(id, subject_id, key_hash, request_digest, job_id, expires_at)
		VALUES ('idem_1', 'subject_default', 'hash-a', 'digest-ok', 'job_ok', now() + interval '1 day')`); err != nil {
		t.Fatalf("idempotency fixture: %v", err)
	}
	rejected(t, "duplicate idempotency (subject, key hash)", `INSERT INTO idempotency_keys
		(id, subject_id, key_hash, request_digest, job_id, expires_at)
		VALUES ('idem_dup', 'subject_default', 'hash-a', 'digest-other', 'job_ok', now() + interval '1 day')`)

	// 0010: every job at most one request snapshot, and it must carry a payload.
	if _, err := mdb.ExecContext(ctx, `INSERT INTO async_job_requests (job_id, request)
		VALUES ('job_ok', '{"public_model":"gateway-echo"}')`); err != nil {
		t.Fatalf("job request fixture: %v", err)
	}
	rejected(t, "duplicate job request payload", `INSERT INTO async_job_requests (job_id, request)
		VALUES ('job_ok', '{"public_model":"gateway-echo"}')`)
	rejected(t, "job request without a payload", `INSERT INTO async_job_requests (job_id, request)
		VALUES ('job_ok', NULL)`)

	// --- 0007 cost governance --------------------------------------------------
	rejected(t, "negative input price", `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 1, 'USD', -1, 0, now())`)
	rejected(t, "negative output price", `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 1, 'USD', 0, -5, now())`)
	if _, err := mdb.ExecContext(ctx, `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 1, 'USD', 3, 12, now())`); err != nil {
		t.Fatalf("price fixture: %v", err)
	}
	if _, err := mdb.ExecContext(ctx, `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 2, 'USD', 2, 10, now())`); err != nil {
		t.Fatalf("second price fixture: %v", err)
	}
	rejected(t, "duplicate price version", `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 2, 'USD', 1, 9, now())`)
	rejected(t, "malformed currency", `INSERT INTO pricing_catalog
		(provider, public_model, price_version, currency, input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake-primary', 'gateway-echo', 3, 'dollars', 1, 9, now())`)

	rejected(t, "negative cost", `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, price_version, currency, cost_micros, settle_status, settled_at)
		VALUES ('req_negcost', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 1, 'USD', -9, 'settled', now())`)
	rejected(t, "invalid settle status", `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, settle_status)
		VALUES ('req_badstatus', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 'pending')`)
	rejected(t, "cost settled without a price version", `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, cost_micros, settle_status, settled_at)
		VALUES ('req_noversion', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 5, 'settled', now())`)
	rejected(t, "settled without settled_at", `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, price_version, currency, cost_micros, settle_status)
		VALUES ('req_nostamp', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 1, 'USD', 5, 'settled')`)
	if _, err := mdb.ExecContext(ctx, `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, price_version, currency, prompt_tokens, cost_micros, settle_status, settled_at)
		VALUES ('req_settle', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 1, 'USD', 10, 30, 'settled', now())`); err != nil {
		t.Fatalf("settled ledger fixture: %v", err)
	}
	// The same request identity must never produce a second final record.
	rejected(t, "duplicate final settlement for one request", `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, price_version, currency, prompt_tokens, cost_micros, settle_status, settled_at)
		VALUES ('req_settle', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 1, 'USD', 10, 30, 'settled', now())`)
	// A released row for the same identity is fine (reserve -> release ->
	// reserve -> settle is a legal lifecycle).
	if _, err := mdb.ExecContext(ctx, `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, settle_status)
		VALUES ('req_settle', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 'released')`); err != nil {
		t.Fatalf("released row for a settled identity must be allowed: %v", err)
	}
	// Unknown cost (NULL cost_micros) with known tokens never violates; it is
	// the explicit unknown representation.
	if _, err := mdb.ExecContext(ctx, `INSERT INTO usage_ledger
		(request_id, subject_id, tenant_id, protocol, public_model, prompt_tokens, settle_status, settled_at)
		VALUES ('req_unknown_cost', 'subject_default', 'tenant_default', 'chat', 'gateway-echo', 7, 'settled', now())`); err != nil {
		t.Fatalf("costless settled row (unknown pricing) must be allowed: %v", err)
	}

	rejected(t, "negative budget amount", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', -100, 'USD')`)
	rejected(t, "zero budget amount", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', 0, 'USD')`)
	rejected(t, "invalid budget period", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_default', 'tenant_default', 'weekly', 100, 'USD')`)
	rejected(t, "subject budget without a subject", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', NULL, 'tenant_default', 'daily', 100, 'USD')`)
	rejected(t, "tenant budget carrying a subject", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('tenant', 'subject_default', 'tenant_default', 'daily', 100, 'USD')`)
	rejected(t, "budget subject from another tenant (cross-owner FK)", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_other', 'tenant_default', 'daily', 100, 'USD')`)
	if _, err := mdb.ExecContext(ctx, `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', 1000, 'USD')`); err != nil {
		t.Fatalf("budget fixture: %v", err)
	}
	rejected(t, "duplicate budget target", `INSERT INTO budget_policies
		(scope, subject_id, tenant_id, period, amount_micros, currency)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', 2000, 'USD')`)

	// --- 0008 admin identity ------------------------------------------------
	rejected(t, "malformed scope", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_1', 'admin-a', '\x00'::bytea, 'kbap_', ARRAY['root'], 'active')`)
	rejected(t, "invented privileged scope", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_2', 'admin-a', '\x01'::bytea, 'kbap_', ARRAY['superuser'], 'active')`)
	rejected(t, "empty scope set", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_3', 'admin-a', '\x02'::bytea, 'kbap_', ARRAY[]::text[], 'active')`)
	rejected(t, "mixed valid and malformed scopes", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_4', 'admin-a', '\x03'::bytea, 'kbap_', ARRAY['viewer', 'wizard'], 'active')`)
	rejected(t, "invalid credential status", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_5', 'admin-a', '\x04'::bytea, 'kbap_', ARRAY['viewer'], 'disabled')`)
	if _, err := mdb.ExecContext(ctx, `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status, tenant_id)
		VALUES ('ac_ok', 'tenant-admin', '\x05'::bytea, 'kbap_', ARRAY['viewer', 'billing'], 'active', 'tenant_default')`); err != nil {
		t.Fatalf("admin credential fixture: %v", err)
	}
	rejected(t, "duplicate credential hash", `INSERT INTO admin_credentials
		(id, admin_subject, credential_hash, credential_prefix, scopes, status)
		VALUES ('ac_dup', 'admin-b', '\x05'::bytea, 'kbap_', ARRAY['viewer'], 'active')`)

	// --- 0009 lifecycle metadata --------------------------------------------
	rejected(t, "retention on an unmanaged table", `INSERT INTO retention_policies (table_name, ttl_seconds)
		VALUES ('tenants', 3600)`)
	rejected(t, "zero retention TTL", `INSERT INTO retention_policies (table_name, ttl_seconds)
		VALUES ('llm_requests', 0)`)
	rejected(t, "negative retention TTL", `INSERT INTO retention_policies (table_name, ttl_seconds)
		VALUES ('llm_requests', -1)`)
	if _, err := mdb.ExecContext(ctx, `INSERT INTO retention_policies (table_name, ttl_seconds)
		VALUES ('llm_requests', 86400)`); err != nil {
		t.Fatalf("retention fixture: %v", err)
	}
	rejected(t, "invalid archive status", `INSERT INTO archive_runs (table_name, status)
		VALUES ('llm_requests', 'cancelled')`)
	rejected(t, "negative archived row count", `INSERT INTO archive_runs (table_name, status, rows_archived)
		VALUES ('llm_requests', 'completed', -1)`)
	rejected(t, "terminal archive run without finished_at", `INSERT INTO archive_runs (table_name, status)
		VALUES ('llm_requests', 'completed')`)
	rejected(t, "invalid export status", `INSERT INTO data_exports (id, requested_by, tenant_id, status)
		VALUES ('dx_1', 'ops', 'tenant_default', 'running')`)
	rejected(t, "completed export without completed_at", `INSERT INTO data_exports (id, requested_by, tenant_id, status)
		VALUES ('dx_2', 'ops', 'tenant_default', 'completed')`)

	// Cleanup: leave the shared database at head, as the other tests expect.
	for _, stmt := range []string{
		`DELETE FROM data_exports`,
		`DELETE FROM archive_runs`,
		`DELETE FROM retention_policies`,
		`DELETE FROM admin_credentials`,
		`DELETE FROM budget_policies`,
		`DELETE FROM usage_ledger`,
		`DELETE FROM pricing_catalog`,
		`DELETE FROM idempotency_keys`,
		`DELETE FROM async_jobs`,
		`DELETE FROM model_catalog WHERE public_name = 'model-constraint'`,
		`DELETE FROM subjects WHERE id = 'subject_other'`,
		`DELETE FROM tenants WHERE id = 'tenant_other'`,
	} {
		if _, err := mdb.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("cleanup: %v: %v", stmt, err)
		}
	}
}

// int64Ptr is a test helper for optional audit fields.
func int64Ptr(v int64) *int64 { return &v }
