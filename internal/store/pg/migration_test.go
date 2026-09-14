package pg

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
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
// versioned migration tool, exercises the key lifecycle and audit stores on
// the real schema, rolls 0002 back through the tool, verifies the v1.1
// columns are gone, and re-applies to confirm version tracking. It requires
// a real PostgreSQL instance and is skipped when TEST_DATABASE_URL is not
// set.
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
	_, _ = mdb.ExecContext(ctx, "DROP TABLE IF EXISTS llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers CASCADE")

	m := newTestMigrator(t, mdb)
	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	requireVersion(t, m, 2, false)
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

	// Audit write with unknown usage stays NULL (never zero).
	ev := audit.Event{
		RequestID: "req_test_1", SubjectID: "subject_default", KeyID: gen.Record.ID,
		Model: "gateway-echo", Provider: "fake-primary", Status: 200,
		LatencyMillis: 5, Streaming: false, CreatedAt: time.Now(),
		TraceID: "req_test_1", RouteAttempts: 1,
	}
	if err := pgw.WriteAudit(ctx, ev); err != nil {
		t.Fatalf("write audit: %v", err)
	}
	var promptTokens *int
	if err := pgw.Pool.QueryRow(ctx,
		`SELECT prompt_tokens FROM llm_requests WHERE request_id='req_test_1'`).Scan(&promptTokens); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if promptTokens != nil {
		t.Fatal("unknown usage must persist as NULL, not zero")
	}

	// Roll back 0002 through the tool and confirm the new columns are gone.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("roll back one version: %v", err)
	}
	requireVersion(t, m, 1, false)
	var cols int
	if err := mdb.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name='llm_requests' AND column_name IN ('trace_id','cost_micros','route_attempts')`).Scan(&cols); err != nil {
		t.Fatalf("column check: %v", err)
	}
	if cols != 0 {
		t.Fatalf("down migration must drop v1.1 columns, found %d", cols)
	}

	// Re-apply forward to prove version tracking recovers cleanly.
	if err := m.Up(); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	requireVersion(t, m, 2, false)
}
