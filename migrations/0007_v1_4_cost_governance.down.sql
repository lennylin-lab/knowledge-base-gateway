-- Revert the V1.4 cost governance schema. Dependents (ledger, budgets)
-- before the price catalog; no pre-V1.4 data is touched.
DROP TABLE IF EXISTS budget_policies;
DROP TABLE IF EXISTS usage_ledger;
DROP TABLE IF EXISTS pricing_catalog;
