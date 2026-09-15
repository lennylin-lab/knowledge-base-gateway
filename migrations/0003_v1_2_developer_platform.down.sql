-- Revert the V1.2 developer platform schema.

DROP TABLE IF EXISTS admin_audit;

ALTER TABLE llm_requests
    DROP COLUMN protocol;

UPDATE model_catalog
SET capabilities = '{"stream": true}'::jsonb,
    config_version = config_version - 1
WHERE public_name = 'gateway-echo';
