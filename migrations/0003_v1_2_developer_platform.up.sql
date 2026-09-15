-- V1.2 developer platform schema: protocol-aware audit, full capability
-- declarations for the seeded mock models, and the management-operation
-- audit trail. Additive only; the down migration reverts every change.

-- Protocol label ("chat" / "responses") so management queries can correlate
-- usage per public protocol. Existing rows stay valid with the default.
ALTER TABLE llm_requests
    ADD COLUMN protocol TEXT NOT NULL DEFAULT 'chat';

-- The seeded mock model now declares its full public capability matrix
-- (previously only {"stream": true}), so /v1/responses, tool calling, and
-- structured output are usable out of the box in seeded deployments.
UPDATE model_catalog
SET capabilities = jsonb_build_object(
        'chat', true,
        'responses', true,
        'stream', true,
        'tools', true,
        'structured_output', true,
        'json_mode', true,
        'vision', false,
        'reasoning', false,
        'usage', true,
        'context_tokens', 8192,
        'max_output_tokens', 2048,
        'max_tools', 8
    ),
    config_version = config_version + 1
WHERE public_name = 'gateway-echo';

-- Management-operation audit: who changed what through the admin API.
-- Detail carries metadata only (action target names), never secrets or
-- prompt/completion content.
CREATE TABLE admin_audit (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    action      TEXT NOT NULL,
    target      TEXT NOT NULL DEFAULT '',
    admin_subject TEXT NOT NULL DEFAULT 'admin-token',
    detail      JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_admin_audit_created ON admin_audit (created_at);
