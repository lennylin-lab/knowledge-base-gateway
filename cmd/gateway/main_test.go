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
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
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

// latestMigrationVersion walks the repository migrations directory through
// the same source driver cmd/migrate uses and returns the highest version.
// Deriving the expectation keeps this test honest when new migration pairs
// land: no hardcoded version to forget at the next schema change.
func latestMigrationVersion(t *testing.T) uint {
	t.Helper()
	d, err := source.Open("file://" + migrationsDir(t))
	if err != nil {
		t.Fatalf("open migrations dir: %v", err)
	}
	defer d.Close()
	version, err := d.First()
	if err != nil {
		t.Fatalf("no first migration: %v", err)
	}
	for {
		next, err := d.Next(version)
		if errors.Is(err, os.ErrNotExist) {
			return version
		}
		if err != nil {
			t.Fatalf("next after %d: %v", version, err)
		}
		version = next
	}
}

// migrateToHead drives the same golang-migrate instance as cmd/migrate until
// the schema is at the latest version. It never drops anything: the shared
// test database may hold state from other tests, which must keep working.
// The gateway binary under test is the pre-V1.4-feature binary; starting it
// against the migrated head proves the additive V1.4 schema stays compatible
// with existing (flag-off) behavior.
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
	want := latestMigrationVersion(t)
	version, dirty, err := m.Version()
	if err != nil || version != want || dirty {
		t.Fatalf("schema version = %d (dirty=%v) err=%v, want %d (dirty=false)", version, dirty, err, want)
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

// TestDatabaseModeAdminToggleAffectsLiveServing proves the AC1 management
// contract against a real PostgreSQL schema: the audited admin model
// disable/enable takes effect for subsequent model resolution in the same
// running process — no restart — and the toggle lands in admin_audit.
func TestDatabaseModeAdminToggleAffectsLiveServing(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping database-backed admin toggle test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)
	migrateToHead(t, dsn)

	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	// Seed defensively: the shared database may hold state from other tests.
	const subject = "subject_smoke_toggle"
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants (id, name) VALUES ('tenant_default', 'Default Tenant') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO subjects (id, tenant_id) VALUES ($1, 'tenant_default') ON CONFLICT (id) DO NOTHING`, []any{subject}},
		{`INSERT INTO access_policies (subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens)
		  VALUES ($1, 'gateway-echo', 120, 8, 1000000) ON CONFLICT (subject_id, public_model) DO NOTHING`, []any{subject}},
		{`UPDATE model_catalog SET enabled = true WHERE public_name = 'gateway-echo'`, nil},
	} {
		if _, err := pool.Pool.Exec(ctx, stmt.query, stmt.args...); err != nil {
			t.Fatalf("seed rows: %v", err)
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
	cfg.AdminToken = "toggle-test-admin-token"
	cfg.AdminAddr = freeAddr(t)
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

	admin := func(path string) int {
		req, err := http.NewRequest(http.MethodPost, "http://"+cfg.AdminAddr+path, nil)
		if err != nil {
			t.Fatalf("admin request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+cfg.AdminToken)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("admin call %s: %v", path, err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	chat := func() (int, string) {
		body := `{"model":"gateway-echo","messages":[{"role":"user","content":"smoke"}]}`
		req, err := http.NewRequest(http.MethodPost, "http://"+cfg.Addr+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			t.Fatalf("chat request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+gen.Plaintext)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("chat call: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if status, body := chat(); status != http.StatusOK {
		t.Fatalf("model must serve before the toggle: status = %d body = %s", status, body)
	}

	// Disable: audited, and immediately enforced by the running process.
	if status := admin("/admin/models/gateway-echo/disable"); status != http.StatusOK {
		t.Fatalf("admin disable status = %d", status)
	}
	if status, body := chat(); status != http.StatusForbidden {
		t.Fatalf("disabled model must stop serving without a restart: status = %d body = %s", status, body)
	}
	var ops int
	if err := pool.Pool.QueryRow(ctx,
		`SELECT count(*) FROM admin_audit WHERE action = 'model_disable' AND target = 'gateway-echo'`).Scan(&ops); err != nil || ops < 1 {
		t.Fatalf("disable must be management-audited: ops=%d err=%v", ops, err)
	}

	// Re-enable: serving resumes in the same process.
	if status := admin("/admin/models/gateway-echo/enable"); status != http.StatusOK {
		t.Fatalf("admin enable status = %d", status)
	}
	if status, body := chat(); status != http.StatusOK {
		t.Fatalf("re-enabled model must serve again: status = %d body = %s", status, body)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run after graceful cancel: %v", err)
	}
}

// TestCredentialEnvName pins the per-provider variable naming convention:
// <KIND>_API_KEY__<PROVIDER_NAME> with the provider name uppercased and every
// non-alphanumeric rune mapped to '_'.
func TestCredentialEnvName(t *testing.T) {
	tests := []struct {
		kind, name, want string
	}{
		{"openai", "chat", "OPENAI_API_KEY__CHAT"},
		{"openai", "openai", "OPENAI_API_KEY__OPENAI"}, // dev-mode selector name
		{"openai", "openai-embed", "OPENAI_API_KEY__OPENAI_EMBED"},
		{"anthropic", "eu.claude", "ANTHROPIC_API_KEY__EU_CLAUDE"},
		{"openai", "Embed V2", "OPENAI_API_KEY__EMBED_V2"},
	}
	for _, tt := range tests {
		if got := credentialEnvName(tt.kind, tt.name); got != tt.want {
			t.Errorf("credentialEnvName(%q, %q) = %q, want %q", tt.kind, tt.name, got, tt.want)
		}
	}
}

// TestNewProviderFromRegistryPerProviderCredential pins credential resolution
// for one registry row: the per-provider variable wins over the kind-level
// credential, the kind-level fallback keeps deployments without per-provider
// variables behaving exactly as before, the name mapping applies through the
// real env lookup for both kinds, and both-absent refuses startup naming the
// checked variables without ever echoing a value.
func TestNewProviderFromRegistryPerProviderCredential(t *testing.T) {
	tests := []struct {
		name         string
		kind         string
		provName     string
		baseURL      string
		setupEnv     map[string]string
		openAIKey    string
		anthropicKey string
		wantKey      string
		wantErr      []string
	}{
		{
			name: "per-provider openai variable wins over kind level",
			kind: "openai", provName: "chat", baseURL: "https://chat.example.test",
			setupEnv:  map[string]string{"OPENAI_API_KEY__CHAT": "per-provider-chat-key"},
			openAIKey: "kind-level-openai-key",
			wantKey:   "per-provider-chat-key",
		},
		{
			name: "openai falls back to kind level when per-provider unset",
			kind: "openai", provName: "chat", baseURL: "https://chat.example.test",
			// Empty counts as unset: pins hermeticity against ambient env.
			setupEnv:  map[string]string{"OPENAI_API_KEY__CHAT": ""},
			openAIKey: "kind-level-openai-key",
			wantKey:   "kind-level-openai-key",
		},
		{
			name: "openai name with hyphen maps to underscore",
			kind: "openai", provName: "openai-embed", baseURL: "https://embed.example.test",
			setupEnv: map[string]string{"OPENAI_API_KEY__OPENAI_EMBED": "embed-key"},
			wantKey:  "embed-key",
		},
		{
			name: "anthropic name with dot maps to underscore",
			kind: "anthropic", provName: "eu.claude", baseURL: "https://eu.example.test",
			setupEnv:     map[string]string{"ANTHROPIC_API_KEY__EU_CLAUDE": "eu-key"},
			anthropicKey: "kind-level-anthropic-key",
			wantKey:      "eu-key",
		},
		{
			name: "anthropic falls back to kind level",
			kind: "anthropic", provName: "claude", baseURL: "https://claude.example.test",
			setupEnv:     map[string]string{"ANTHROPIC_API_KEY__CLAUDE": ""},
			anthropicKey: "kind-level-anthropic-key",
			wantKey:      "kind-level-anthropic-key",
		},
		{
			name: "both absent refuses openai naming both variables",
			kind: "openai", provName: "fresh", baseURL: "https://fresh.example.test",
			wantErr: []string{"OPENAI_API_KEY__FRESH", "OPENAI_API_KEY"},
		},
		{
			name: "both absent refuses anthropic naming both variables",
			kind: "anthropic", provName: "fresh", baseURL: "https://fresh.example.test",
			wantErr: []string{"ANTHROPIC_API_KEY__FRESH", "ANTHROPIC_API_KEY"},
		},
	}
	const (
		perProviderValue = "per-provider-chat-key"
		kindLevelValue   = "kind-level-openai-key"
	)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.setupEnv {
				t.Setenv(k, v)
			}
			cfg := config.Config{OpenAIKey: tt.openAIKey, AnthropicKey: tt.anthropicKey}
			p, err := newProviderFromRegistry(cfg, tt.kind, tt.provName, tt.baseURL)
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatal("newProviderFromRegistry must fail when both variables are absent")
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q must name checked variable %q", err, want)
					}
				}
				for _, secret := range []string{perProviderValue, kindLevelValue} {
					if strings.Contains(err.Error(), secret) {
						t.Errorf("error must never echo a credential value, got %q", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("newProviderFromRegistry: %v", err)
			}
			switch keyed := p.(type) {
			case *provider.OpenAI:
				if keyed.APIKey != tt.wantKey {
					t.Errorf("OpenAI APIKey = %q, want %q", keyed.APIKey, tt.wantKey)
				}
			case *provider.Anthropic:
				if keyed.APIKey != tt.wantKey {
					t.Errorf("Anthropic APIKey = %q, want %q", keyed.APIKey, tt.wantKey)
				}
			default:
				t.Fatalf("unexpected provider type %T", p)
			}
		})
	}

	// The fake kind stays credential-free: seeded internal:// rows validate
	// with no environment at all.
	t.Run("fake needs no credential", func(t *testing.T) {
		p, err := newProviderFromRegistry(config.Config{}, "fake", "seeded-fake", "internal://fake")
		if err != nil {
			t.Fatalf("fake provider: %v", err)
		}
		if _, ok := p.(provider.Fake); !ok {
			t.Fatalf("want provider.Fake, got %T", p)
		}
	})
}

// authRecorder records Authorization headers under a mutex so the handler
// goroutine and the test goroutine never race.
type authRecorder struct {
	mu       sync.Mutex
	headerss []string
}

func (a *authRecorder) add(v string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.headerss = append(a.headerss, v)
}

func (a *authRecorder) all() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.headerss...)
}

// stubOpenAIUpstream starts an httptest server answering OpenAI chat
// completion requests and recording the Authorization header of every call.
func stubOpenAIUpstream(t *testing.T) (url string, rec *authRecorder) {
	t.Helper()
	rec = &authRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-stub","object":"chat.completion","created":1,`+
			`"model":"stub","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},`+
			`"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, rec
}

// TestPerProviderCredentialReachesUpstream proves the resolved per-provider
// credential is the one actually sent upstream: two openai-kind registry rows
// with different OPENAI_API_KEY__<NAME> variables each dial their own stub
// upstream carrying their own Bearer token, while the kind-level fallback key
// is never used.
func TestPerProviderCredentialReachesUpstream(t *testing.T) {
	chatURL, chatAuth := stubOpenAIUpstream(t)
	embedURL, embedAuth := stubOpenAIUpstream(t)

	// The kind-level key is a decoy: if resolution regressed to kind-level
	// only, both stubs would see it instead of the per-provider keys.
	cfg := config.Config{OpenAIKey: "kind-level-fallback-key", AllowInsecure: true}
	t.Setenv("OPENAI_API_KEY__CHAT_UPSTREAM", "chat-key-1")
	t.Setenv("OPENAI_API_KEY__EMBED_UPSTREAM", "embed-key-2")

	chatProvider, err := newProviderFromRegistry(cfg, "openai", "chat-upstream", chatURL)
	if err != nil {
		t.Fatalf("chat provider: %v", err)
	}
	embedProvider, err := newProviderFromRegistry(cfg, "openai", "embed-upstream", embedURL)
	if err != nil {
		t.Fatalf("embed provider: %v", err)
	}

	req := model.Request{Model: "stub", Input: []model.InputItem{{Role: "user", Text: "ping"}}}
	ctx := context.Background()
	for _, p := range []provider.Provider{chatProvider, embedProvider} {
		if _, err := p.Complete(ctx, req); err != nil {
			t.Fatalf("%s complete: %v", p.Name(), err)
		}
	}

	if got := chatAuth.all(); len(got) != 1 || got[0] != "Bearer chat-key-1" {
		t.Errorf("chat upstream authorization = %v, want [Bearer chat-key-1]", got)
	}
	if got := embedAuth.all(); len(got) != 1 || got[0] != "Bearer embed-key-2" {
		t.Errorf("embed upstream authorization = %v, want [Bearer embed-key-2]", got)
	}
	for _, got := range append(chatAuth.all(), embedAuth.all()...) {
		if strings.Contains(got, "kind-level-fallback-key") {
			t.Errorf("kind-level fallback key must never reach an upstream that declares its own credential, got %q", got)
		}
	}
}

// TestDatabaseStartupPerProviderCredentials proves the convention against a
// real PostgreSQL registry: an enabled openai row with neither the
// per-provider nor the kind-level variable aborts startup naming both checked
// variables (never a value), while two openai rows with different base URLs
// start healthy when one resolves its own OPENAI_API_KEY__<NAME> variable and
// the other falls back to the kind-level key.
func TestDatabaseStartupPerProviderCredentials(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping database-backed per-provider credential test")
	}
	ctx := context.Background()
	lockTestDatabase(t, dsn)
	migrateToHead(t, dsn)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	logger := quietLogger()

	// Both absent: startup refuses before any listener exists, naming the
	// per-provider variable and the kind-level fallback, leaking neither a
	// value nor the DSN.
	seedProvider(t, db, "cred-chat", "openai", "https://cred-chat.example.test")
	err = run(ctx, dbStartupConfig(t, dsn), logger)
	if err == nil {
		t.Fatal("startup must fail when neither the per-provider nor the kind-level variable is set")
	}
	if !strings.Contains(err.Error(), "OPENAI_API_KEY__CRED_CHAT") || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Errorf("error must name both checked variables, got: %v", err)
	}
	if strings.Contains(err.Error(), dsn) || strings.Contains(err.Error(), "postgres://") {
		t.Errorf("error must not include the database DSN, got: %v", err)
	}

	// Mixed resolution in one process: cred-chat resolves from its own
	// per-provider variable while cred-embed falls back to the kind-level
	// key. Startup succeeds and /readyz reports ready.
	t.Setenv("OPENAI_API_KEY__CRED_CHAT", "per-provider-chat-key")
	seedProvider(t, db, "cred-embed", "openai", "https://cred-embed.example.test")
	cfg := dbStartupConfig(t, dsn)
	cfg.OpenAIKey = "kind-level-openai-key"

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(runCtx, cfg, logger) }()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	var lastErr error
	for time.Now().Before(deadline) {
		resp, rerr := client.Get("http://" + cfg.Addr + "/readyz")
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
		t.Fatalf("run with per-provider credentials: %v", runErr)
	}
	if !ready {
		t.Fatalf("server never reported ready within deadline, last error: %v", lastErr)
	}
}
