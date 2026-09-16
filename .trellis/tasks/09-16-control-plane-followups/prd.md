# Control plane follow-ups: dimensions injection and defaults collapse

## Goal

Fix the two integration findings from the v1.3 control plane rollout (issues
#6 and #7), both root-caused in the previous session's verification.

## Background and Confirmed Facts

- Issue #6: `EmbeddingsRequest` (internal/model/embeddings.go) and the openai
  wire request have no `dimensions` field — the standard OpenAI parameter is
  dropped. MRL upstreams (e.g. Qwen3-Embedding, native 2560) then return
  native width and the gateway's `CheckEmbeddingDim` fails against the
  declared `embedding_dim` with 500 `embedding_dim_mismatch`.
- Issue #7: `LoadLimits` (internal/store/pg/pg.go:343-365) collapses
  `access_policies` rows per subject with `out[subject] = l` (last row by
  `subject_id, id` wins). If the winning row's `default_model` is NULL, the
  subject's chat default resolves empty → 400 `invalid_request` on
  model-less requests, even when other rows declare defaults.
- Pre-existing, deliberately unchanged: the same row collapse also decides
  rate/ceiling values ("last row wins") for multi-row subjects. Changing
  that would silently alter existing deployments and is out of scope; it is
  recorded as a follow-up decision issue instead.

## Requirements

### Fix 1 (issue #6): inject catalog dimension upstream

- The gateway injects the catalog-declared `embedding_dim` as `dimensions`
  into the upstream embeddings request (OpenAI wire field); client-passed
  `dimensions` is ignored — the catalog remains the dimension authority, and
  `CheckEmbeddingDim` closes the loop.
- `EmbeddingsRequest` carries the declared dimension so the adapter can
  inject it; adapters without dimension support (fake, anthropic-unsupported)
  unaffected.
- Non-MRL upstreams (fixed width equal to the declaration) behave exactly as
  today; declaration/upstream mismatch still fails loud 500
  `embedding_dim_mismatch`.

### Fix 2 (issue #7): defaults survive multi-row policies

- When collapsing `access_policies` rows per subject, `default_model` and
  `default_embedding_model` take the **first non-empty value** in `id` order
  across that subject's rows; a NULL slot on one row never erases a default
  declared on another. Chat and embedding slots are judged independently.
- The memory policy service mirrors the same semantics.
- Rate/ceiling collapse semantics stay "as-is" this task (documented in the
  follow-up issue text); only defaults change.

### Follow-up recording

- File (or update) an issue documenting the ceilings row-collapse semantics
  decision (unchanged this task, needs a future product call) — drafted for
  the main session to post.

## Acceptance Criteria

- [ ] Contract-suite scenario: an openai stub upstream asserting the request
  body carries `"dimensions": <declared embedding_dim>`; client-passed
  `dimensions` is ignored (not forwarded, no error).
- [ ] MRL-shaped scenario: upstream honoring `dimensions` returns the
  declared width → 200; upstream returning native (different) width → 500
  `embedding_dim_mismatch`.
- [ ] pg loader test: subject with rows (chat default on row 1, NULL on
  later rows) still resolves both defaults; a subject with all-NULL keeps
  no default (400 path unchanged); memory service matches.
- [ ] No wire change for chat/responses; golden fixtures byte-identical;
  strict decoding intact.
- [ ] `gofmt`, `go vet`, `go test -race ./...` pass with env-gated tests run
  for real (`TEST_DATABASE_URL=postgres://kb:kb@127.0.0.1:5432/
  kb_gateway_test?sslmode=disable`, `TEST_REDIS_ADDR=127.0.0.1:6379`, zero
  skips); replay 12/12.

## Out of Scope

- Ceiling row-collapse semantics change; per-subject retrieval profiles;
  client `dimensions` passthrough (rejected in favor of injection).

## Key Decisions

- Issue #6 option 1 (catalog injection), per the filer's preference and the
  dimension-authority philosophy.
- Issue #7 first-non-empty-wins for defaults only; ceilings unchanged with a
  recorded follow-up issue.
- **Ceilings decision (issue #8, server, 2026-09-16)**: short-term option 1 —
  rate/ceiling fields fold to the **minimum declared value** per field
  (order-independent, intersection semantics; adding a row can never raise
  quotas). Mid-term option 4 (subject-level ceilings table) is a separate
  future migration; per-model quotas noted as a future interface. Mitigation
  for silent tightening: the admin policies view must make the folded
  effective values visible (per-row effective annotation or per-subject
  effective-limits block — implementer's choice, tested).

## Scope Addendum (2026-09-16, post-#8 decision)

- `FoldPolicyRow` ceiling fields change from last-row-wins to min-of-declared
  (0/unset rows do not constrain: a field is only constrained by rows that
  declare it); defaults keep first-non-empty-wins.
- Admin policies view gains tightened-value visibility per the mitigation
  above.
- Spec folding convention (database-guidelines) updated accordingly.

## Open Questions (blocking)

- None.
