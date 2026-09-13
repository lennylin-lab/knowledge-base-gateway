-- Initial schema for the LLM gateway MVP.
-- Forward-only baseline; every future schema change gets a new migration file.

CREATE TABLE tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE subjects (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id            TEXT PRIMARY KEY,
    subject_id    TEXT NOT NULL REFERENCES subjects(id),
    key_salt      BYTEA NOT NULL,
    key_hash      BYTEA NOT NULL UNIQUE,
    key_prefix    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    expires_at    TIMESTAMPTZ,
    last_used_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE model_catalog (
    public_name     TEXT PRIMARY KEY,
    provider        TEXT NOT NULL,
    upstream_model  TEXT NOT NULL,
    capabilities    JSONB NOT NULL DEFAULT '{}',
    enabled         BOOLEAN NOT NULL DEFAULT true,
    config_version  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE access_policies (
    id              BIGSERIAL PRIMARY KEY,
    subject_id      TEXT NOT NULL REFERENCES subjects(id),
    public_model    TEXT NOT NULL REFERENCES model_catalog(public_name),
    rate_per_minute INTEGER NOT NULL,
    max_concurrent  INTEGER NOT NULL,
    daily_tokens    BIGINT,
    monthly_tokens  BIGINT,
    max_input_tokens  INTEGER,
    max_output_tokens INTEGER,
    UNIQUE (subject_id, public_model)
);

CREATE TABLE llm_requests (
    request_id        TEXT PRIMARY KEY,
    subject_id        TEXT NOT NULL,
    key_id            TEXT NOT NULL,
    model             TEXT NOT NULL,
    provider          TEXT NOT NULL,
    status            INTEGER NOT NULL,
    error_class       TEXT,
    latency_ms        BIGINT NOT NULL,
    prompt_tokens     BIGINT,          -- NULL when upstream did not report usage
    completion_tokens BIGINT,          -- NULL when upstream did not report usage
    streaming         BOOLEAN NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_llm_requests_subject_created ON llm_requests (subject_id, created_at);
CREATE INDEX idx_llm_requests_model_created ON llm_requests (model, created_at);
