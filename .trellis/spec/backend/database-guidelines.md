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

### Convention: Lifecycle sweeps archive, verify, then delete

**What**: Retention sweeps (internal/lifecycle + cmd/maintain) follow a
fixed discipline per batch: select eligible rows (terminal jobs only,
settled-or-released ledger rows) → write the archive artifact → read-back
verify (row count + SHA-256) → only then delete, bounded per run. Archives
are content-free by construction (shared column projections — response
bodies and prompt/completion content never selected); the manifest carries
schema version, range, and checksum. Absent policy row or `enabled=false`
is a legal hold that skips the table. Exports are two-phase (tenant
validated before bytes, management-log platform-scope only) with the same
checksum trailer.

**Why**: Billing ledger rows are the durable settlement record and audit
rows are evidence — a sweep must never destroy either, and a partial or
corrupt archive must never trigger deletes (verify-before-publish makes
deletes unreachable on sink failure).

**Boundary**: Physical partitioning of `llm_requests` stays deferred —
bounded PK-range archive sweeps are the rolling-upgrade-safe equivalent.
`retention_policies.archive_before_delete` is reserved (archiving is
unconditional). Idempotency `KeyTTL` is enforced at the replay lookup
boundary, reclaimed on write, and swept by the worker cadence and
`cmd/maintain`.

### Convention: Migration batches walk every down boundary and freeze wire first

**What**: When a task lands a batch of migrations (e.g. the V1.4 0006-0009
foundation), the env-gated lifecycle test must (a) walk **every** down
boundary (9→8→…→0) with per-boundary artifact checks, not just one step,
then re-up; (b) include an upgrade-path test from the previous release
version (e.g. fresh up to exactly version 5, V1.3 store reads work, upgrade,
reads still work); and (c) prove schema invariants (closed state sets,
uniqueness, owner-scoped FKs, money checks) with DB-level rejection tests,
not application logic. Wire behavior is frozen with golden fixtures
**before** feature children start.

**Why**: Constraint regressions and partial-down corruption are invisible to
single-step tests; the golden freeze catches accidental wire drift across a
long roadmap.

**Boundary**: Never edit applied migrations; edits to a not-yet-merged batch
after a PostgreSQL rehearsal require a fresh full down-boundary run.

### Convention: Multi-row subject policies fold deterministically

**What**: A subject may have one `access_policies` row per granted model.
`LoadLimits` folds a subject's rows via `policy.Limits.FoldPolicyRow`:
`default_model` / `default_embedding_model` take the **first non-empty
value** in `id` order (chat and embedding slots judged independently), and
every rate/ceiling field (`rate_per_minute`, `max_concurrent`,
`daily_tokens`, `monthly_tokens`, `max_input_tokens`, `max_output_tokens`)
folds to the **minimum declared value** across the subject's rows (issue #8
decision). A ceiling constrains only when a row declares it — a NULL/0 cap
never constrains, and a field no row declares stays 0 (uncapped); the
ceiling fold itself is order-independent.

**Why**: Row-level NULLs must never erase defaults declared on another row
(issue #7: any NULL on the winning row killed subject-level backfill).
Min-of-declared gives ceilings intersection semantics — adding a row can
never raise a quota — and removes the ordering accident of last-row-wins,
where the final row by `id` silently resized the subject.

**Boundary**: Both pg and any in-memory row source must fold through the
shared `FoldPolicyRow` so the modes cannot drift; the admin policies view
attaches the folded block (`mgmt.EffectiveLimits`, computed through
`LoadLimits`) to every row so tightening is visible to operators instead of
silent; and the admin default-model mutation updates all of a subject's rows
and can only set existing models, so it never conflicts with
first-non-empty-wins.

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

