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
// versioned migration tool, exercises the key lifecycle, audit, and V1.2
// management stores on the real schema, verifies the 0004 first-token column
// round-trips (and rolls back), rolls 0003 back through the tool, verifies
// the new columns and tables are gone, and re-applies to confirm version
// tracking. It requires a real PostgreSQL instance and is skipped when
// TEST_DATABASE_URL is not set.
func TestMigrationsAndStores(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL migration test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)

	// The migration tool owns schema changes: clean slate including its
	// bookkeeping table, then drive it like cmd/migrate does.
	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers, admin_audit CASCADE")

	m := newTestMigrator(t, mdb)
	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, m, 5, false)
	if err := m.Up(); !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("second up must be a no-op, got %v", err)
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
		        WHERE table_name='llm_requests' AND column_name IN ('trace_id','cost_micros','route_attempts','protocol'))
		     + (SELECT count(*) FROM information_schema.tables
		        WHERE table_name IN ('admin_audit'))`).Scan(&artifacts); err != nil {
		t.Fatalf("artifact check: %v", err)
	}
	if artifacts != 3 {
		// trace_id, cost_micros, route_attempts remain (v1.1); protocol and
		// admin_audit must be gone.
		t.Fatalf("down migration left unexpected artifacts: %d", artifacts)
	}

	// Re-apply forward to prove version tracking recovers cleanly.
	if err := m.Up(); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	requireVersion(t, m, 5, false)
}

// int64Ptr is a test helper for optional audit fields.
func int64Ptr(v int64) *int64 { return &v }
