-- V1.1 production schema: provider registry, routes, key lifecycle, trace/cost audit.
-- Additive only; down migration reverts every change made here.

CREATE TABLE providers (
    name             TEXT PRIMARY KEY,
    kind             TEXT NOT NULL CHECK (kind IN ('openai', 'anthropic', 'fake')),
    base_url         TEXT NOT NULL,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    health_path      TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE model_routes (
    id              BIGSERIAL PRIMARY KEY,
    public_model    TEXT NOT NULL REFERENCES model_catalog(public_name) ON DELETE CASCADE,
    provider        TEXT NOT NULL REFERENCES providers(name),
    upstream_model  TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 100,
    capabilities    JSONB NOT NULL DEFAULT '{}',
    timeout_ms      INTEGER NOT NULL DEFAULT 60000,
    max_output_tokens INTEGER,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    config_version  INTEGER NOT NULL DEFAULT 1,
    UNIQUE (public_model, provider)
);
CREATE INDEX idx_model_routes_model_priority ON model_routes (public_model, priority);

ALTER TABLE api_keys
    ADD COLUMN tenant_id   TEXT,
    ADD COLUMN rotated_from TEXT REFERENCES api_keys(id),
    ADD COLUMN revoked_at  TIMESTAMPTZ;

ALTER TABLE llm_requests
    ADD COLUMN trace_id   TEXT NOT NULL DEFAULT '',
    ADD COLUMN cost_micros BIGINT,
    ADD COLUMN route_attempts INTEGER NOT NULL DEFAULT 1;

-- Seed data: default tenant/subject and a fake primary/backup route pair so a
-- fresh deployment passes readiness without external provider credentials.
INSERT INTO tenants (id, name) VALUES ('tenant_default', 'Default Tenant')
    ON CONFLICT (id) DO NOTHING;
INSERT INTO subjects (id, tenant_id) VALUES ('subject_default', 'tenant_default')
    ON CONFLICT (id) DO NOTHING;

INSERT INTO providers (name, kind, base_url) VALUES
    ('fake-primary', 'fake', 'internal://fake-primary'),
    ('fake-backup',  'fake', 'internal://fake-backup')
    ON CONFLICT (name) DO NOTHING;

INSERT INTO model_catalog (public_name, provider, upstream_model, capabilities)
VALUES ('gateway-echo', 'fake-primary', 'echo-model', '{"stream": true}')
    ON CONFLICT (public_name) DO NOTHING;

INSERT INTO model_routes (public_model, provider, upstream_model, priority, timeout_ms)
VALUES ('gateway-echo', 'fake-primary', 'echo-model', 10, 60000),
       ('gateway-echo', 'fake-backup',  'echo-model', 20, 60000)
    ON CONFLICT (public_model, provider) DO NOTHING;

INSERT INTO access_policies (subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens)
VALUES ('subject_default', 'gateway-echo', 120, 8, 1000000)
    ON CONFLICT (subject_id, public_model) DO NOTHING;
