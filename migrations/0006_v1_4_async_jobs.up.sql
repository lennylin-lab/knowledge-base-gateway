-- V1.4 async foundation: persisted background jobs, their stored results,
-- and idempotency keys. Schema only: no worker, endpoint, or accounting
-- behavior ships in this migration. Additive only; the down migration drops
-- exactly what the up migration created and preserves all pre-V1.4 data.
--
-- Invariants are encoded at the database boundary:
--   - closed state sets via CHECK (queued/running/completed/failed/
--     cancelled/expired; protocol labels already fixed by V1.2/V1.3);
--   - tenant/subject ownership via a composite foreign key, so a job can
--     never claim a subject that belongs to a different tenant;
--   - one idempotency key per (subject, key hash), pointing at the one job
--     it created;
--   - UTC timestamps as TIMESTAMPTZ NOT NULL DEFAULT now().

-- Composite uniqueness for ownership FKs. Purely additive: (id) is already
-- the primary key, so this adds no new restriction on existing rows. Named
-- explicitly so the down migration drops it by name.
ALTER TABLE subjects
    ADD CONSTRAINT uq_subjects_id_tenant UNIQUE (id, tenant_id);

-- One row per background task. protocol carries the public protocol label
-- ("chat" / "responses" / "embeddings"); the closed set matches the audit
-- protocol labels already in use. request_digest is the canonical request
-- hash used for idempotency conflict detection. lease_owner/lease_expires_at
-- hold the single active worker lease (NULL when no worker holds it).
CREATE TABLE async_jobs (
    job_id            TEXT PRIMARY KEY,
    subject_id        TEXT NOT NULL,
    tenant_id         TEXT NOT NULL,
    protocol          TEXT NOT NULL CHECK (protocol IN ('chat', 'responses', 'embeddings')),
    public_model      TEXT NOT NULL REFERENCES model_catalog(public_name),
    request_digest    TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled', 'expired')),
    attempt_count     INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    final_request_id  TEXT,
    result_expires_at TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (subject_id, tenant_id) REFERENCES subjects (id, tenant_id)
);

-- Worker lease claim set: at most one live lease per job is enforced by the
-- store's conditional updates; this index makes claim scans cheap. Partial:
-- only unfinished jobs participate in recovery.
CREATE INDEX idx_async_jobs_status_lease ON async_jobs (status, lease_expires_at)
    WHERE status IN ('queued', 'running');
CREATE INDEX idx_async_jobs_subject_created ON async_jobs (subject_id, created_at);
CREATE INDEX idx_async_jobs_tenant_created ON async_jobs (tenant_id, created_at);

-- The stored unified gateway response for a terminal job. response holds the
-- normalized public response object (never provider-private fields); usage
-- columns stay NULL when the upstream did not report usage (unknown is never
-- fabricated as zero). Exactly zero or one result per job.
CREATE TABLE async_job_results (
    job_id               TEXT PRIMARY KEY REFERENCES async_jobs (job_id) ON DELETE CASCADE,
    response             JSONB NOT NULL,
    error_class          TEXT,
    usage_prompt_tokens     BIGINT CHECK (usage_prompt_tokens IS NULL OR usage_prompt_tokens >= 0),
    usage_completion_tokens BIGINT CHECK (usage_completion_tokens IS NULL OR usage_completion_tokens >= 0),
    usage_total_tokens      BIGINT CHECK (usage_total_tokens IS NULL OR usage_total_tokens >= 0),
    result_bytes         INTEGER NOT NULL CHECK (result_bytes >= 0),
    retention_expires_at TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_async_job_results_retention ON async_job_results (retention_expires_at);

-- Idempotency keys for background job creation. key_hash is a keyed hash of
-- the caller's Idempotency-Key scoped by subject; request_digest must match
-- the original request or the store reports a conflict (the 409 comparison
-- itself is feature behavior, the uniqueness here is the schema boundary).
-- job_id references the one task the key created.
CREATE TABLE idempotency_keys (
    id             TEXT PRIMARY KEY,
    subject_id     TEXT NOT NULL REFERENCES subjects (id),
    key_hash       TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    job_id         TEXT NOT NULL REFERENCES async_jobs (job_id) ON DELETE CASCADE,
    expires_at     TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject_id, key_hash)
);

CREATE INDEX idx_idempotency_keys_expiry ON idempotency_keys (expires_at);
