package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
)

// testDatabaseLockKey serializes the env-gated integration tests in this
// package against internal/store/pg, which drops and re-applies the shared
// TEST_DATABASE_URL schema under the same PostgreSQL advisory lock. Keep both
// constants identical.
const testDatabaseLockKey int64 = 721534891

// migrationsDir resolves the repository migrations directory relative to this
// file (cmd/gateway) and asserts it exists even when the database-gated tests
// skip, so a wrong relative depth fails unconditionally.
func migrationsDir(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		t.Fatalf("migrations dir missing at %s: %v", abs, err)
	}
	return abs
}

func TestMigrationsDirExists(t *testing.T) {
	migrationsDir(t)
}

// lockTestDatabase takes a session-level advisory lock on one pinned
// connection so concurrent `go test ./...` package runs cannot race on the
// shared test database schema.
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

// migrateToHead drives the same golang-migrate instance as cmd/migrate until
// the schema is at the latest version. It never drops anything: the shared
// test database may hold state from other tests, which must keep working.
func migrateToHead(t *testing.T, dsn string) {
	t.Helper()
	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	driver, err := pgx5.WithInstance(mdb, &pgx5.Config{})
	if err != nil {
		t.Fatalf("migrate driver: %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+migrationsDir(t), "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate instance: %v", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
	version, dirty, err := m.Version()
	if err != nil || version != 2 || dirty {
		t.Fatalf("schema version = %d (dirty=%v) err=%v, want 2 (dirty=false)", version, dirty, err)
	}
}

// seedProvider upserts one enabled provider registry row and removes it at
// cleanup, leaving the shared database as it was found.
func seedProvider(t *testing.T, db *sql.DB, name, kind, baseURL string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO providers (name, kind, base_url)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE
		SET kind = EXCLUDED.kind, base_url = EXCLUDED.base_url, enabled = true`,
		name, kind, baseURL); err != nil {
		t.Fatalf("seed provider %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM providers WHERE name = $1`, name)
	})
}

// freeAddr reserves an ephemeral port so concurrent test runs never collide.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// dbStartupConfig builds a database-mode startup configuration. The legacy
// GATEWAY_PROVIDER selector is deliberately set to openai with no OpenAI key:
// in database mode the persisted provider registry is authoritative and the
// selector must not influence startup.
func dbStartupConfig(t *testing.T, dsn string) config.Config {
	return config.Config{
		Addr:            freeAddr(t),
		RequestTimeout:  5 * time.Second,
		MaxBodyBytes:    1 << 20,
		MaxMessages:     64,
		MaxMessageChars: 32_000,
		RatePerMinute:   120,
		MaxConcurrent:   8,
		Provider:        "openai",
		DatabaseURL:     dsn,
		LimitsMode:      "local",
	}
}

// TestDatabaseStartupValidatesProviderRegistry proves, against a real
// PostgreSQL schema, that every enabled provider registry row is
// credential-checked before the HTTP server can report ready, without ever
// calling the upstream provider: a missing credential aborts startup before
// any listener exists, and a satisfied credential lets /readyz report ready.
func TestDatabaseStartupValidatesProviderRegistry(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping database-backed startup test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)
	migrateToHead(t, dsn)

	// Closing the pool is registered as a cleanup (not a defer) so it runs
	// after the seedProvider cleanups below: t.Cleanup runs in LIFO order
	// once the test function's defers have already executed.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A missing Anthropic credential must abort startup with an error that
	// names the missing configuration kind and leaks neither the secret nor
	// the DSN. Nothing may be listening afterwards.
	seedProvider(t, db, "startup-anthropic", "anthropic", "https://api.anthropic.test")
	missing := dbStartupConfig(t, dsn)
	err = run(ctx, missing, logger)
	if err == nil {
		t.Fatal("startup must fail without ANTHROPIC_API_KEY")
	}
	if !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Errorf("error must name the missing credential kind, got: %v", err)
	}
	if strings.Contains(err.Error(), dsn) || strings.Contains(err.Error(), "postgres://") {
		t.Errorf("error must not include the database DSN, got: %v", err)
	}
	if conn, derr := net.DialTimeout("tcp", missing.Addr, 500*time.Millisecond); derr == nil {
		conn.Close()
		t.Error("no listener may exist after failed provider validation")
	}

	// The same holds for the openai kind: fake needs no secret, but an
	// enabled openai row without OPENAI_API_KEY is a startup error.
	seedProvider(t, db, "startup-openai", "openai", "https://api.openai.test")
	_, _ = db.ExecContext(ctx, `DELETE FROM providers WHERE name = 'startup-anthropic'`)
	missingOpenAI := dbStartupConfig(t, dsn)
	err = run(ctx, missingOpenAI, logger)
	if err == nil {
		t.Fatal("startup must fail without OPENAI_API_KEY")
	}
	if !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Errorf("error must name the missing credential kind, got: %v", err)
	}
	_, _ = db.ExecContext(ctx, `DELETE FROM providers WHERE name = 'startup-openai'`)

	// With the credential present the registry validates, the listener
	// starts, and only then can /readyz report ready. No upstream provider
	// call is made: registry construction never contacts the provider.
	seedProvider(t, db, "startup-anthropic", "anthropic", "https://api.anthropic.test")
	valid := dbStartupConfig(t, dsn)
	valid.AnthropicKey = "startup-test-anthropic-key" // never leaves the process

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(runCtx, valid, logger) }()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	var lastErr error
	for time.Now().Before(deadline) {
		resp, rerr := client.Get("http://" + valid.Addr + "/readyz")
		if rerr == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if !strings.Contains(string(body), "ready") {
					t.Fatalf("readyz body = %q, want ready status", body)
				}
				ready = true
				break
			}
			lastErr = errors.New(resp.Status)
		} else {
			lastErr = rerr
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("run after successful startup: %v", runErr)
	}
	if !ready {
		t.Fatalf("server never reported ready within deadline, last error: %v", lastErr)
	}
}
