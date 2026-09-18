-- V1.4 lifecycle metadata schema: retention policies, archive runs, and
-- export records for the large audit/usage/async tables. Metadata only: the
-- retention sweeper, partition maintenance, and export jobs are feature
-- work. Physical partitioning of existing tables is deliberately NOT done
-- here — converting llm_requests to a partitioned table would rewrite the
-- table and break rolling upgrades; the registry below carries the declared
-- partition intent instead. Additive only.
--
-- Invariants encoded at the database boundary:
--   - the managed-table set is closed (only tables this roadmap governs);
--   - TTLs are positive integers (a zero/negative TTL is a configuration
--     error, not "keep forever": absence of a row means no policy);
--   - archive/run/export states are closed and runs are end-stamped.

-- Configurable retention per governed table. archive_before_delete encodes
-- the roadmap rule that expiry archives first, then deletes.
CREATE TABLE retention_policies (
    table_name           TEXT PRIMARY KEY CHECK (table_name IN ('llm_requests', 'async_jobs', 'async_job_results', 'usage_ledger')),
    ttl_seconds          INTEGER NOT NULL CHECK (ttl_seconds > 0),
    archive_before_delete BOOLEAN NOT NULL DEFAULT true,
    enabled              BOOLEAN NOT NULL DEFAULT true,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per archive/delete sweep. started rows without finished_at are
-- either running or crashed; the sweeper owns recovery.
CREATE TABLE archive_runs (
    id            BIGSERIAL PRIMARY KEY,
    table_name    TEXT NOT NULL CHECK (table_name IN ('llm_requests', 'async_jobs', 'async_job_results', 'usage_ledger')),
    status        TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    rows_archived BIGINT NOT NULL DEFAULT 0 CHECK (rows_archived >= 0),
    rows_deleted  BIGINT NOT NULL DEFAULT 0 CHECK (rows_deleted >= 0),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    detail        JSONB NOT NULL DEFAULT '{}',
    -- Terminal runs are end-stamped.
    CHECK ((status IN ('running')) OR (status IN ('completed', 'failed') AND finished_at IS NOT NULL))
);

CREATE INDEX idx_archive_runs_table_started ON archive_runs (table_name, started_at DESC);

-- Desensitized export records: who requested which scope and filters. The
-- filters JSONB carries query metadata only (tenant/subject/model/time
-- bounds), never exported content.
CREATE TABLE data_exports (
    id           TEXT PRIMARY KEY,
    requested_by TEXT NOT NULL,
    tenant_id    TEXT REFERENCES tenants (id),
    filters      JSONB NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL CHECK (status IN ('queued', 'completed', 'failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    -- Terminal exports are end-stamped.
    CHECK ((status = 'queued') OR (status IN ('completed', 'failed') AND completed_at IS NOT NULL))
);
