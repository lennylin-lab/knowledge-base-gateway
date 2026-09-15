# PRD: Complete V1.2 Management and Replay Follow-Ups

## Goal

Resolve issue #1 management and verification follow-ups: make management mutations live and auditable, align operational metrics/reported limitations, add standalone protocol replay coverage, clean stale task artifacts, and close the issue with a redacted final status.

## Requirements

- R1: Make database-backed model enable/disable mutations visible to the running gateway without requiring restart, or introduce an explicit refresh boundary that the admin mutation invokes. Evidence: internal/store/pg/mgmt.go:46-56 and cmd/gateway/main.go route/catalog wiring.
- R2: Make management mutation plus management-operation audit atomic, or provide explicit rollback/failed-operation semantics that cannot leave a successful mutation reported as failed. Evidence: internal/httpapi/admin.go:120-140 and internal/store/pg/mgmt.go:175-194.
- R3: Complete or explicitly stage the management metrics contract for provider health, recent errors, breaker state, first-token latency, cost, and true percentile behavior. Evidence: docs/v1.2-developer-platform-roadmap.md management requirements and internal/mgmt/mgmt.go:38-43 and :195-244.
- R4: Add a deterministic offline protocol replay command or tool that can run provider/protocol fixtures through adapters and assert normalized events without calling real providers.
- R5: Clean the archived v1.2 PRD placeholder block containing Requirements: TBD and Acceptance Criteria: TBD while preserving completed task history.
- R6: Update GitHub issue #1 only with public, redacted status after all child tasks are verified.

## Acceptance Criteria

- AC1: Admin model enable/disable changes affect subsequent model resolution in the same running process, and tests cover database-backed or injected-refresh behavior.
- AC2: A management audit-write failure cannot leave an already-committed mutation reported as failed without explicit rollback/status semantics; tests cover the failure path.
- AC3: Management provider/usage endpoints either expose the roadmap operational fields or document staged/unavailable fields in API/docs/tests without misleading names.
- AC4: A standalone replay entry point exists, runs offline, uses deterministic fixtures, and is included in validation instructions or CI where appropriate.
- AC5: The archived V1.2 PRD no longer has the stale TBD header block and remains readable as a completed planning artifact.
- AC6: Final issue #1 update summarizes completed items and any environment-gated checks without leaking secrets, DSNs, private URLs, prompt/completion content, or local paths.

## Out of Scope

- Implementing full billing/pricing beyond surfaced/staged management fields.
- Creating public management APIs; the admin surface remains token-gated and internal.
- Reworking protocol admission, input quotas, circuit breakers, or URL safety owned by sibling tasks.

## Open Questions

None blocking. If full cost/first-token metrics are not feasible in this stage, implementation may document staged fields as long as endpoint names are not misleading.
