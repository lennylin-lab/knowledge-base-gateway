-- V1.4 cost governance schema: versioned price catalog, the settlement
-- ledger shared by sync and async requests, and money budgets. Schema only:
-- reserve/settle/release logic and budget enforcement are feature work.
-- Additive only; the down migration drops exactly what was created.
--
-- Invariants encoded at the database boundary:
--   - money is integer micro-units (micros); no floating-point amounts;
--   - negative amounts are rejected by CHECK on every money column;
--   - a priced settlement always records the price version it used
--     (cost_micros NOT NULL implies price_version NOT NULL);
--   - the same request/task settles at most once: a partial unique index
--     keeps exactly one 'settled' row per request identity;
--   - budgets belong to exactly one subject (of the tenant) or the tenant;
--   - closed state sets for currency shape, settle status, budget scope,
--     and budget period.

-- Versioned price rows: one row per (provider, public model, version).
-- Prices are positive-integer micro units per token; the optional columns
-- only apply when the provider reports those token classes. effective_from
-- selects the version in force at settlement time.
CREATE TABLE pricing_catalog (
    id            BIGSERIAL PRIMARY KEY,
    provider      TEXT NOT NULL,
    public_model  TEXT NOT NULL REFERENCES model_catalog (public_name),
    price_version INTEGER NOT NULL CHECK (price_version > 0),
    currency      TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    input_micros_per_token         BIGINT NOT NULL CHECK (input_micros_per_token >= 0),
    output_micros_per_token        BIGINT NOT NULL CHECK (output_micros_per_token >= 0),
    reasoning_micros_per_token     BIGINT CHECK (reasoning_micros_per_token IS NULL OR reasoning_micros_per_token >= 0),
    cached_input_micros_per_token  BIGINT CHECK (cached_input_micros_per_token IS NULL OR cached_input_micros_per_token >= 0),
    effective_from TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, public_model, price_version)
);

CREATE INDEX idx_pricing_catalog_lookup ON pricing_catalog (provider, public_model, effective_from DESC);

-- One row per reserve/settle/release lifecycle. request_id identifies the
-- sync request; job_id links an async task; both may be NULL for a pure
-- reservation that was released without an identity. cost_micros stays NULL
-- when usage or an effective price is missing (unknown cost is never
-- fabricated as zero, and never as a priceless number).
CREATE TABLE usage_ledger (
    id                  BIGSERIAL PRIMARY KEY,
    request_id          TEXT,
    job_id              TEXT REFERENCES async_jobs (job_id),
    subject_id          TEXT NOT NULL,
    tenant_id           TEXT NOT NULL,
    protocol            TEXT NOT NULL CHECK (protocol IN ('chat', 'responses', 'embeddings')),
    public_model        TEXT NOT NULL REFERENCES model_catalog (public_name),
    price_version       INTEGER,
    currency            TEXT CHECK (currency IS NULL OR currency ~ '^[A-Z]{3}$'),
    prompt_tokens       BIGINT CHECK (prompt_tokens IS NULL OR prompt_tokens >= 0),
    completion_tokens   BIGINT CHECK (completion_tokens IS NULL OR completion_tokens >= 0),
    reasoning_tokens    BIGINT CHECK (reasoning_tokens IS NULL OR reasoning_tokens >= 0),
    cached_input_tokens BIGINT CHECK (cached_input_tokens IS NULL OR cached_input_tokens >= 0),
    cost_micros         BIGINT CHECK (cost_micros IS NULL OR cost_micros >= 0),
    settle_status       TEXT NOT NULL CHECK (settle_status IN ('reserved', 'settled', 'released')),
    settled_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (subject_id, tenant_id) REFERENCES subjects (id, tenant_id),
    -- A settled row with a cost must name the price version it used.
    CHECK ((cost_micros IS NULL) OR (price_version IS NOT NULL AND currency IS NOT NULL)),
    -- Settlement state is timestamped.
    CHECK ((settle_status = 'settled') = (settled_at IS NOT NULL))
);

-- At most one final (settled) usage/cost record per request identity: the
-- same sync request or async job can reserve and release many times but
-- settles exactly once. Duplicate final settlement keys fail here.
CREATE UNIQUE INDEX uq_usage_ledger_final_per_request
    ON usage_ledger (COALESCE(request_id, job_id))
    WHERE settle_status = 'settled';

CREATE INDEX idx_usage_ledger_subject_created ON usage_ledger (subject_id, created_at);
CREATE INDEX idx_usage_ledger_tenant_created ON usage_ledger (tenant_id, created_at);
CREATE INDEX idx_usage_ledger_settled_at ON usage_ledger (settled_at);

-- Money budgets per subject or per tenant, daily or monthly on UTC
-- boundaries. amount_micros must be positive (a zero or negative budget is a
-- configuration error, not "no budget": absence of a row means uncapped).
-- scope='subject' rows carry the owning subject; scope='tenant' rows must
-- not. The composite FK makes a subject budget pointing at a foreign or
-- nonexistent subject impossible.
CREATE TABLE budget_policies (
    id            BIGSERIAL PRIMARY KEY,
    scope         TEXT NOT NULL CHECK (scope IN ('subject', 'tenant')),
    subject_id    TEXT,
    tenant_id     TEXT NOT NULL REFERENCES tenants (id),
    period        TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    amount_micros BIGINT NOT NULL CHECK (amount_micros > 0),
    currency      TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    enabled       BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (subject_id, tenant_id) REFERENCES subjects (id, tenant_id),
    CHECK (
        (scope = 'subject' AND subject_id IS NOT NULL)
        OR (scope = 'tenant' AND subject_id IS NULL)
    )
);

-- One budget per (target, period, currency): duplicates would make the
-- pre-invocation budget check order-dependent.
CREATE UNIQUE INDEX uq_budget_policies_target
    ON budget_policies (scope, COALESCE(subject_id, ''), tenant_id, period, currency);
