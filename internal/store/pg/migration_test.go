package pg

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
)

// applyFile executes one migration file against the test database.
func applyFile(t *testing.T, conn *pgx.Conn, file string) {
	t.Helper()
	b, err := os.ReadFile("../../../../migrations/" + file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	if _, err := conn.Exec(context.Background(), string(b)); err != nil {
		t.Fatalf("apply %s: %v", file, err)
	}
}

// TestMigrationsAndStores applies 0001 and 0002 forward, exercises the key
// lifecycle and audit stores on the real schema, then reverts 0002. It
// requires a real PostgreSQL instance and is skipped when TEST_DATABASE_URL
// is not set.
func TestMigrationsAndStores(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL migration test")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Best-effort clean slate from a previous run.
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS llm_requests, access_policies, model_routes, model_catalog, api_keys, subjects, tenants, providers CASCADE")
	applyFile(t, conn, "0001_init.sql")
	applyFile(t, conn, "0002_v1_1_production.sql")
	conn.Close(ctx)

	db, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	defer db.Close()
	if err := db.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}

	// Key lifecycle on the persisted store.
	gen, err := auth.NewManager(db).Create(ctx, "subject_default", "tenant_default", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	principal, err := (&Authenticator{DB: db, Now: time.Now}).Authenticate(gen.Plaintext, time.Now())
	if err != nil || principal.SubjectID != "subject_default" || principal.KeyID != gen.Record.ID {
		t.Fatalf("authenticate: principal=%+v err=%v", principal, err)
	}
	if _, err := db.ResolveAuth(ctx, "kb_wrong", time.Now()); err != auth.ErrInvalid {
		t.Fatalf("wrong key must be invalid, got %v", err)
	}

	// Audit write with unknown usage stays NULL (never zero).
	ev := audit.Event{
		RequestID: "req_test_1", SubjectID: "subject_default", KeyID: gen.Record.ID,
		Model: "gateway-echo", Provider: "fake-primary", Status: 200,
		LatencyMillis: 5, Streaming: false, CreatedAt: time.Now(),
		TraceID: "req_test_1", RouteAttempts: 1,
	}
	if err := db.WriteAudit(ctx, ev); err != nil {
		t.Fatalf("write audit: %v", err)
	}
	var promptTokens *int
	if err := db.Pool.QueryRow(ctx,
		`SELECT prompt_tokens FROM llm_requests WHERE request_id='req_test_1'`).Scan(&promptTokens); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if promptTokens != nil {
		t.Fatal("unknown usage must persist as NULL, not zero")
	}

	// Roll back 0002 and confirm the new columns are gone.
	conn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer conn2.Close(ctx)
	applyFile(t, conn2, "0002_v1_1_production.down.sql")
	var cols int
	if err := conn2.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name='llm_requests' AND column_name IN ('trace_id','cost_micros','route_attempts')`).Scan(&cols); err != nil {
		t.Fatalf("column check: %v", err)
	}
	if cols != 0 {
		t.Fatalf("down migration must drop v1.1 columns, found %d", cols)
	}
}
