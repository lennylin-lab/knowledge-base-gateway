-- Reverts 0001_init.up.sql, dropping the initial schema in reverse dependency
-- order. All data in these tables is destroyed; table indexes disappear with
-- their tables.
DROP TABLE IF EXISTS llm_requests;
DROP TABLE IF EXISTS access_policies;
DROP TABLE IF EXISTS model_catalog;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS subjects;
DROP TABLE IF EXISTS tenants;
