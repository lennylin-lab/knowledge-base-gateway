package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
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
	if err != nil || version != 3 || dirty {
		t.Fatalf("schema version = %d (dirty=%v) err=%v, want 3 (dirty=false)", version, dirty, err)
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

// quietLogger discards logs; tests assert on returned errors, not output.
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// localModeConfig builds a minimal in-memory (no database) configuration.
func localModeConfig(t *testing.T) config.Config {
	return config.Config{
		Addr:            freeAddr(t),
		RequestTimeout:  5 * time.Second,
		MaxBodyBytes:    1 << 20,
		MaxMessages:     64,
		MaxMessageChars: 32_000,
		RatePerMinute:   120,
		MaxConcurrent:   8,
		Provider:        "fake",
		LimitsMode:      "local",
	}
}

// TestConnectFailureErrorHidesDSN pins the startup contract that a failing
// database connect is logged as a classification only: no DSN, no password,
// no connection-string component (host, user, database, port) may appear.
// The bad host makes this hermetic — no database is needed to fail the dial.
func TestConnectFailureErrorHidesDSN(t *testing.T) {
	dsn := "postgres://kbconnectuser:connect-test-secret@db-host-nonexistent.invalid:5432/kbconnectdb?sslmode=disable&connect_timeout=2"
	cfg := localModeConfig(t)
	cfg.DatabaseURL = dsn

	err := run(context.Background(), cfg, quietLogger())
	if err == nil {
		t.Fatal("startup must fail for an unreachable database host")
	}
	msg := err.Error()
	for _, leak := range []string{dsn, "connect-test-secret", "postgres://", "db-host-nonexistent.invalid", "kbconnectuser", "kbconnectdb", "5432"} {
		if strings.Contains(msg, leak) {
			t.Errorf("connect error must not contain %q, got: %s", leak, msg)
		}
	}
	if !strings.Contains(msg, "database connect") {
		t.Errorf("connect error must name the failed operation, got: %s", msg)
	}
}

// TestConnectFailureBadCredentialsHidesSecret exercises the classification
// against a real PostgreSQL server that rejects the password: the secret and
// every other DSN component must stay out of the returned error.
func TestConnectFailureBadCredentialsHidesSecret(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping bad-credentials startup test")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	const wrongPassword = "wrong-password-smoke-test"
	host := u.Hostname()
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	u.User = url.UserPassword(user, wrongPassword)
	cfg := localModeConfig(t)
	cfg.DatabaseURL = u.String()

	err = run(context.Background(), cfg, quietLogger())
	if err == nil {
		t.Fatal("startup must fail with rejected credentials")
	}
	msg := err.Error()
	for _, leak := range []string{wrongPassword, dsn, "postgres://", host, user} {
		if leak != "" && strings.Contains(msg, leak) {
			t.Errorf("connect error must not contain %q, got: %s", leak, msg)
		}
	}
	if !strings.Contains(msg, "database connect") {
		t.Errorf("connect error must name the failed operation, got: %s", msg)
	}
}

// TestListenerFailureReturnsThroughRun pins the structured shutdown contract:
// when the HTTP listener cannot bind (port in use), run returns an error
// through the normal path — no os.Exit in a goroutine — and the sibling admin
// listener is shut down gracefully, releasing its port.
func TestListenerFailureReturnsThroughRun(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer occupied.Close()

	cfg := localModeConfig(t)
	cfg.Addr = occupied.Addr().String()
	cfg.AdminToken = "listener-test-admin-token"
	cfg.AdminAddr = freeAddr(t) // reserve-then-release; bind happens inside run

	done := make(chan error, 1)
	go func() { done <- run(context.Background(), cfg, quietLogger()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run must return an error when the HTTP listener cannot bind")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after HTTP listener bind failure")
	}

	// The admin listener must be gone: its port no longer accepts connections.
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, derr := net.DialTimeout("tcp", cfg.AdminAddr, 250*time.Millisecond)
		if derr != nil {
			break
		}
		conn.Close()
		if time.Now().After(deadline) {
			t.Error("admin listener still accepting after HTTP listener failure")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestDatabaseModePolicyGrantsPermitSeededModel proves the database-mode
// policy wiring end to end: a subject with an access_policies row completes a
// real chat request against the seeded fake model, and any other model stays
// a non-leaky 403. Requires a real PostgreSQL (see CI contract).
func TestDatabaseModePolicyGrantsPermitSeededModel(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping database-backed policy grant test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)
	migrateToHead(t, dsn)

	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	// Seed defensively: another test may have removed the migration's seed
	// rows. Cleanup only removes what this test added.
	const subject = "subject_smoke_grant"
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants (id, name) VALUES ('tenant_default', 'Default Tenant') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO subjects (id, tenant_id) VALUES ($1, 'tenant_default') ON CONFLICT (id) DO NOTHING`, []any{subject}},
		{`INSERT INTO access_policies (subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens)
		  VALUES ($1, 'gateway-echo', 120, 8, 1000000) ON CONFLICT (subject_id, public_model) DO NOTHING`, []any{subject}},
	} {
		if _, err := pool.Pool.Exec(ctx, stmt.query, stmt.args...); err != nil {
			t.Fatalf("seed policy rows: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Pool.Exec(context.Background(), `DELETE FROM access_policies WHERE subject_id = $1`, subject)
		_, _ = pool.Pool.Exec(context.Background(), `DELETE FROM subjects WHERE id = $1`, subject)
	})

	mgr := auth.NewManager(pool)
	gen, err := mgr.Create(ctx, subject, "", time.Time{})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Pool.Exec(context.Background(), `DELETE FROM api_keys WHERE id = $1`, gen.Record.ID)
	})

	cfg := dbStartupConfig(t, dsn)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(runCtx, cfg, quietLogger()) }()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, rerr := client.Get("http://" + cfg.Addr + "/readyz")
		if rerr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		cancel()
		<-done
		t.Fatal("server never became ready within deadline")
	}

	chat := func(model string) (int, string) {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"smoke"}]}`, model)
		req, err := http.NewRequest(http.MethodPost, "http://"+cfg.Addr+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			return 0, ""
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+gen.Plaintext)
		resp, err := client.Do(req)
		if err != nil {
			return 0, ""
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	status, body := chat("gateway-echo")
	if status != http.StatusOK {
		t.Fatalf("granted model: chat status = %d, want 200, body = %s", status, body)
	}
	if !strings.Contains(body, `"object":"chat.completion"`) || !strings.Contains(body, `"choices"`) {
		t.Fatalf("granted model: response is not an OpenAI-compatible completion: %s", body)
	}

	status, body = chat("model-without-grant")
	if status != http.StatusForbidden {
		t.Fatalf("non-granted model: status = %d, want non-leaky 403, body = %s", status, body)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run after graceful cancel: %v", err)
	}
}
