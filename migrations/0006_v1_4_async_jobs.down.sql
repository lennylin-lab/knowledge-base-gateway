-- Revert the V1.4 async foundation. Dependents before parents; every table
-- and constraint created by 0006 is dropped, and no pre-V1.4 row is touched
-- (async tables hold only V1.4 data).
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS async_job_results;
DROP TABLE IF EXISTS async_jobs;

ALTER TABLE subjects
    DROP CONSTRAINT uq_subjects_id_tenant;
