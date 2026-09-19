-- V1.4 async execution input: the normalized gateway request snapshot for a
-- background job. Child-1's async_jobs carries the request_digest (conflict
-- detection) but not the payload; durable recovery of queued/running jobs
-- after a process restart requires the admitted request itself. One row per
-- job; deletion cascades from async_jobs. Additive only; the down migration
-- drops exactly what the up migration created.
--
-- The payload is the post-admission normalized domain request (default model
-- backfilled, output clamps applied) serialized by internal/async — never raw
-- client bytes, never provider-private fields, never credentials.

CREATE TABLE async_job_requests (
    job_id     TEXT PRIMARY KEY REFERENCES async_jobs (job_id) ON DELETE CASCADE,
    request    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
