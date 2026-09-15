# PRD: Fix V1.2 Input Quotas and Provider Safety

## Goal

Resolve issue #1 safety gaps in input-limit enforcement, route circuit-breaker recovery, and provider base URL validation without changing the public client contract.

## Requirements

- R1: Enforce model ContextTokens before provider invocation using the gateway's deterministic input estimate. Evidence: internal/model/model.go:224-226 and internal/httpapi/common.go:126-138.
- R2: Load and enforce access_policies.max_input_tokens as a subject-level input ceiling. Evidence: migrations/0001_init.up.sql:45 and internal/store/pg/pg.go:327-344.
- R3: Preserve output-token clamping and existing quota reservation semantics while adding input-ceiling rejection.
- R4: Fix all-routes-open behavior so router.Available never bypasses circuit-breaker permits and still allows exactly one half-open probe after cooldown. Evidence: internal/router/routes.go:59-68 and internal/router/breaker.go:45-67.
- R5: Harden provider.ValidateBaseURL so production configuration rejects loopback, private, link-local, multicast, unspecified, and metadata-service IP destinations by default. Evidence: internal/provider/urlsafe.go:31-41.
- R6: Keep local-development exceptions explicit, narrow, documented, and tested; clients must still be unable to provide arbitrary provider URLs.

## Acceptance Criteria

- AC1: Requests exceeding model context or subject max input ceiling return a stable invalid/capability-style error before provider invocation and before quota reservation where possible.
- AC2: PostgreSQL policy loading includes max_input_tokens and in-memory policy limits can represent it.
- AC3: Existing max_output_tokens clamping, daily/monthly quota reserve/settle behavior, and no-provider-call tests continue to pass.
- AC4: Concurrent calls while every route breaker is open admit no traffic until a half-open permit is available, and exactly one probe after cooldown.
- AC5: Provider base URL tests cover unsafe IPv4/IPv6 literals and documented local-development allowances without exposing real endpoints.
- AC6: go test ./internal/policy ./internal/store/pg ./internal/httpapi ./internal/router ./internal/provider and full repository tests pass where environment permits.

## Out of Scope

- Protocol stream validation, Anthropic usage, OpenAI [DONE], management mutations, replay tooling, and archived PRD cleanup.

## Open Questions

None blocking. Use deterministic character/token approximation already present in quota.Estimate unless implementation evidence shows a better existing helper.
