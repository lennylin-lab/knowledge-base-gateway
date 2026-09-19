-- V1.4 async retry backoff: a queued job is not claimable before visible_at.
-- Every requeue that expects a retry (transient admission failure, rate-limit
-- denial, retryable upstream failure, limiter/quota/policy infrastructure
-- outage) delays the next claim so a repeatedly failing job cannot occupy the
-- head of the queue in a tight loop or starve younger jobs. The store sets
-- visible_at explicitly on creation (immediately visible, in the caller's
-- clock domain, like every other timestamp this store compares); the column
-- default covers out-of-band inserts and keeps lease-recovery requeues
-- immediately visible. Additive only; existing rows default to immediately
-- visible.

ALTER TABLE async_jobs ADD COLUMN visible_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Claim scans filter queued + visible and order by creation time; this
-- partial index keeps the ready set (typically small) cheap to scan.
CREATE INDEX idx_async_jobs_claim_ready ON async_jobs (visible_at, created_at)
    WHERE status = 'queued';
