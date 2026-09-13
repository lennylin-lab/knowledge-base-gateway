# Database Guidelines

The planned persistence layer is PostgreSQL, with Redis for distributed rate limiting and short-lived counters. No schema or ORM is present yet, so migrations and store implementations must be introduced explicitly.

Use snake_case table and column names and versioned files under `migrations/`. Initial entities are `tenants`/ `subjects`, `api_keys`, `model_catalog`, `access_policies`, and `llm_requests`. Store API-key hashes, prefixes, status and metadata only; never provider secrets or plaintext keys.

Keep SQL under `internal/store`, expose domain-focused store interfaces, use parameterized queries and explicit contexts/deadlines. Group atomic changes in transactions. Redis is an optimization for distributed limits, not the source of credential or policy truth.

Every schema change requires a new forward migration and a real PostgreSQL-compatible migration test. Do not edit applied migrations or auto-create schema at process startup. Do not build SQL by concatenation, log secrets, or treat unavailable token usage as zero; it remains unknown.

