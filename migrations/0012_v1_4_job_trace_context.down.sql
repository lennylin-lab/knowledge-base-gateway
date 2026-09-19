-- Reverse of 0012: drop the job trace-context columns.

ALTER TABLE async_jobs DROP COLUMN IF EXISTS trace_id;
ALTER TABLE async_jobs DROP COLUMN IF EXISTS parent_span_id;
ALTER TABLE async_jobs DROP COLUMN IF EXISTS trace_sampled;
