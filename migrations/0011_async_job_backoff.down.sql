-- Revert the retry-backoff column. Dependents before parents: drop the
-- claim-serving index, then the column. All V1.4 behavior degrades to
-- immediately-visible requeues (the pre-backoff semantics).
DROP INDEX IF EXISTS idx_async_jobs_claim_ready;
ALTER TABLE async_jobs DROP COLUMN IF EXISTS visible_at;
