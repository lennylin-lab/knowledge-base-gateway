-- Revert the V1.4 admin identity schema. The table holds only hashed
-- management credentials created after this migration; dropping it does not
-- touch tenants, API keys, or the development-mode admin token.
DROP TABLE IF EXISTS admin_credentials;
