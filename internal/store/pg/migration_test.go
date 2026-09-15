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
// management stores on the real schema, rolls 0003 back through the tool,
// verifies the new columns and tables are gone, and re-applies to confirm
// version tracking. It requires a real PostgreSQL instance and is skipped
// when TEST_DATABASE_URL is not set.
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
	requireVersion(t, m, 3, false)
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

	// V1.2: the seeded model declares its full capability matrix.
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
	if len(usage) != 1 || usage[0].Requests != 1 || usage[0].Protocol != "responses" {
		t.Fatalf("usage = %+v", usage)
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

	// Model enable/disable persists and the management op is audited.
	if err := pgw.SetModelEnabled(ctx, "gateway-echo", false); err != nil {
		t.Fatalf("disable model: %v", err)
	}
	if cat2, _ := pgw.LoadCatalog(ctx); len(cat2) != 0 {
		t.Fatal("disabled model must disappear from the enabled catalog")
	}
	if err := pgw.SetModelEnabled(ctx, "gateway-echo", true); err != nil {
		t.Fatalf("enable model: %v", err)
	}
	if err := pgw.WriteOp(ctx, mgmt.AdminOp{Action: "model_disable", Target: "gateway-echo", Detail: json.RawMessage(`{"enabled":false}`)}); err != nil {
		t.Fatalf("write op: %v", err)
	}
	ops, err := pgw.Ops(ctx, 10)
	if err != nil || len(ops) != 1 || ops[0].Action != "model_disable" || ops[0].Target != "gateway-echo" {
		t.Fatalf("ops = %+v err=%v", ops, err)
	}

	// Roll back 0003 through the tool and confirm the new artifacts are gone.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back one version: %v", err)
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
	requireVersion(t, m, 3, false)
}
