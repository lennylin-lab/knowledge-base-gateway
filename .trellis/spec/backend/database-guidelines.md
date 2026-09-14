# Database Guidelines

The planned persistence layer is PostgreSQL, with Redis for distributed rate limiting and short-lived counters. No schema or ORM is present yet, so migrations and store implementations must be introduced explicitly.

Use snake_case table and column names and versioned files under `migrations/`. Initial entities are `tenants`/ `subjects`, `api_keys`, `model_catalog`, `access_policies`, and `llm_requests`. Store API-key hashes, prefixes, status and metadata only; never provider secrets or plaintext keys.

Keep SQL under `internal/store`, expose domain-focused store interfaces, use parameterized queries and explicit contexts/deadlines. Group atomic changes in transactions. Redis is an optimization for distributed limits, not the source of credential or policy truth.

Every schema change requires a new forward migration and a real PostgreSQL-compatible migration test. Do not edit applied migrations or auto-create schema at process startup. Do not build SQL by concatenation, log secrets, or treat unavailable token usage as zero; it remains unknown.

### Convention: Migrations are applied by cmd/migrate, never by the process

**What**: Schema changes live in `migrations/` as golang-migrate files
(`<version>_<name>.up.sql` plus `<version>_<name>.down.sql`). They are applied
and rolled back by `go run ./cmd/migrate -dsn "$GATEWAY_DATABASE_URL"
up|down|steps N|version`, which uses `golang-migrate/migrate/v4` on the
pgx/v5 driver with version tracking (`schema_migrations`) and advisory
locking. The gateway process never runs migrations at startup.

**Why**: A versioned, locked migration tool makes concurrent deployments safe
and every rollback path explicit; startup schema mutation hides drift and
violates least privilege.

**Boundary**: The integration test `internal/store/pg/migration_test.go`
drives the same migrator (up, version assertions, `steps -1`, re-up) and is
gated on `TEST_DATABASE_URL`. New migrations must also keep
`TestMigrationFilesParse` passing: complete up/down pairs, golang-migrate
naming.

### Common Mistake: Resolving the migrations dir with a relative path

**Symptom**: Migration tests skip in CI (no `TEST_DATABASE_URL`), then fail
with "source driver: unknown driver" or "no such file" the first time they
actually run — the latent bug can survive many merges.

**Cause**: `internal/store/pg/migration_test.go` resolved `migrations/` with
`../../../../migrations` (one `..` too many). Wrong relative depth is invisible
while the test skips.

**Fix / Prevention**: Resolve from the current file:
`filepath.Abs(filepath.Join("..", "..", "..", "migrations"))` from
`internal/store/pg`, and assert the directory exists unconditionally (see
`migrations_source_test.go`) so a bad path fails even when the DB-gated test
skips.

### Common Mistake: Lazy pool defers connect errors past sanitization

**Symptom**: A bad `GATEWAY_DATABASE_URL` fails at "load providers" (or the
first query) with a pgx error that embeds DSN details — the sanitized connect
path never runs.

**Cause**: pgx v5 pools are lazy (`MinConns=0` default): `pgxpool.New` succeeds
without dialing, so the first real error surfaces wherever the first query
executes, bypassing any sanitization applied at the connect site.

**Fix / Prevention**: `pgstore.Connect` pings eagerly with a bounded timeout
(10s) and closes the pool on failure, so every connect failure is classified
by one site (`describeDBConnectFailure`: credentials rejected / unreachable /
invalid string / other) and never wraps the original error. The classifier
lives in `internal/dberr` and `cmd/migrate` follows the same pattern (eager
ping in `newMigrator`, `dberr.DescribeConnectFailure` at the connect fatal).
When sanitizing library errors, verify where the library actually surfaces
the failure — classification must cover the real error site, not the intended
one.

### Common Mistake: Down migration deletes seed parent rows before dependents

**Symptom**: `Steps(-1)` fails with `SQLSTATE 23503` (foreign key violation)
when any rows were created against seed data after migration — e.g.
`api_keys` referencing the seed `subjects` row.

**Cause**: The 0002 down script deleted only seed rows and filtered
`llm_requests` by new-column values; rows written later that didn't match the
filter (or keys referencing the seed subject) survived and blocked the parent
deletes.

**Fix / Prevention**: A down script must delete dependents before the seed
parents they reference (`llm_requests` → `api_keys` → `access_policies` →
`subjects`/`model_catalog`), scoped to the seed subject so non-seed audit data
survives. Verify with the env-gated up/down test against real PostgreSQL
(`TEST_DATABASE_URL=... go test ./internal/store/pg/`).

