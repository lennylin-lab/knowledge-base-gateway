# Design: Management and Replay Finish

## Boundaries

Management mutation consistency spans the admin HTTP boundary, mgmt service, policy catalog/router runtime state, and PostgreSQL transaction behavior. Replay tooling belongs outside request-serving paths and must be deterministic/offline. Artifact cleanup stays under .trellis/tasks/archive.

## Management Mutation Flow

The preferred design is one operation that persists the change and its audit record atomically, then refreshes the runtime catalog/routes or invokes an injected runtime updater before returning success. If refresh fails after persistence, the handler must surface a clear operational failure and leave audit evidence.

## Metrics Contract

Avoid misleading fields. Either populate provider health/recent error/breaker state and true usage percentiles or mark unavailable fields explicitly in docs and tests. Development-only approximations should not masquerade as production percentiles.

## Replay Tooling

Add a small command/testable package that reads deterministic fixtures and verifies normalized provider events. It must not require real API keys, network calls, or private data.

## Final Issue Update

When all stages pass, update the public issue with a concise checklist/status comment. Include commands run and environment-gated skips only in generic terms.
