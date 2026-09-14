-- Reverts 0002_v1_1_production.sql. Seed rows created here are removed; the
-- llm_requests rows written while the new columns existed are deleted because
-- the columns (and their data) disappear.
DELETE FROM llm_requests WHERE route_attempts <> 1 OR trace_id <> '' OR cost_micros IS NOT NULL;
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
