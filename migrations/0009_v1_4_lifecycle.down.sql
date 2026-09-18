-- Revert the V1.4 lifecycle metadata schema. These tables hold only
-- policy/record metadata created after this migration; dropping them does
-- not touch the governed tables themselves.
DROP TABLE IF EXISTS data_exports;
DROP TABLE IF EXISTS archive_runs;
DROP TABLE IF EXISTS retention_policies;
