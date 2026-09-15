# Design: Issue #1 Staged Resolution

## Architecture and Boundaries

The parent task is an integration and coordination task only. Product code changes belong to the child tasks, each aligned to a small set of code boundaries: protocol/adapters, policy/router/provider safety, and management/tooling. The parent owns cross-stage acceptance, final validation, and the public issue update.

## Stage Boundaries

1. Protocol and streaming correctness owns HTTP admission validation, strict request decoding, provider stream terminal semantics, usage presence detection, and final streamed-output validation.
2. Quota and provider safety owns input-limit policy plumbing, context-window rejection, circuit-breaker route admission, and provider URL SSRF protections.
3. Management and replay finish owns runtime refresh/atomic audit behavior, operational management views, offline replay tooling, task-artifact cleanup, and final issue reconciliation.

## Cross-Stage Contracts

- Shared protocol errors must keep the existing stable error envelope and request ID behavior.
- Rejections that can be decided from request/model/policy metadata must happen before provider invocation.
- Streaming must never fail over after output reaches the client and must not mark invalid or truncated streams as successful.
- Management responses and issue updates remain metadata-only and redacted.

## Rollout and Rollback

Each child task should remain independently revertible. If a stage needs to defer a roadmap field, it must document the staged behavior and keep the public API explicit rather than silently widening contracts.
