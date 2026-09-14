-- Reverts 0002_v1_1_production.sql. Seed rows are removed, along with any
-- api_keys and llm_requests created against the seed subject after migration:
-- the rollback drops their lifecycle columns (revoked_at, rotated_from,
-- trace_id, ...), so those rows cannot survive the schema change. Requests by
-- non-seed subjects are preserved.
DELETE FROM llm_requests WHERE subject_id = 'subject_default';
DELETE FROM api_keys WHERE subject_id = 'subject_default';
DELETE FROM access_policies WHERE subject_id = 'subject_default';
DELETE FROM model_routes WHERE public_model = 'gateway-echo';
DELETE FROM model_catalog WHERE public_name = 'gateway-echo';
DELETE FROM providers WHERE name IN ('fake-primary', 'fake-backup');
DELETE FROM subjects WHERE id = 'subject_default';
DELETE FROM tenants WHERE id = 'tenant_default';
ALTER TABLE llm_requests
    DROP COLUMN route_attempts,
    DROP COLUMN cost_micros,
    DROP COLUMN trace_id;
ALTER TABLE api_keys
    DROP COLUMN revoked_at,
    DROP COLUMN rotated_from,
    DROP COLUMN tenant_id;
DROP INDEX idx_model_routes_model_priority;
DROP TABLE model_routes;
DROP TABLE providers;
