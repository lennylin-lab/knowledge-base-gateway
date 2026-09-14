# Gateway Container Smoke Design

## Runtime Image

Build the Go Gateway and migration binaries in a multi-stage builder, then
copy them into a minimal runtime image with an explicit non-root user. Runtime
configuration is injected by Compose or the deployment environment; no Secret
is copied into the image.

## Compose Dependency Graph

```text
postgres healthy ----\
                      -> migrate completed successfully -> gateway ready
redis healthy -------/
```

The migration service runs the existing `cmd/migrate` CLI with `up` and exits.
The Gateway uses the seeded fake Provider/catalog, PostgreSQL persistence, and
Redis limits mode. Normal startup never executes a destructive rollback.

## Smoke Contract

A bounded script waits for the Gateway endpoint, asserts `/healthz` is live,
asserts `/readyz` is ready, and prints the relevant service logs on timeout or
failure. Stopping either dependency must make readiness fail while liveness
continues to respond.
