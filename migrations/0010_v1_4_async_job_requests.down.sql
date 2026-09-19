-- Revert the async execution-input table. Dependents before parents: this
-- table is itself a dependent of async_jobs and holds only V1.4 data, so the
-- drop is unconditional and touches no pre-V1.4 row.
DROP TABLE IF EXISTS async_job_requests;
