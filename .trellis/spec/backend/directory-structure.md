# Backend Directory Structure

The repository currently contains documentation only; no Go module has been checked in yet. New backend work follows this planned layout:

```text
cmd/gateway/             # process entrypoint and graceful shutdown
internal/http/           # handlers, middleware, request/response and SSE
internal/auth/           # API-key verification and Principal creation
internal/policy/         # model access, rate and quota decisions
internal/gateway/        # provider routing, deadlines, retries, cancellation
internal/provider/       # provider interface and adapters
internal/audit/          # audit events and token usage metadata
internal/store/          # PostgreSQL/Redis interfaces and implementations
internal/config/         # environment/secret configuration and validation
migrations/              # versioned PostgreSQL migrations
```

Keep transport concerns in `internal/http`; provider SDK types and URLs must not leak into handlers. Business decisions belong in the corresponding package. Use lowercase Go package names, exported names only for package contracts, and colocated `*_test.go` files. Do not create a frontend tree for gateway-only work.

The provider boundary described in `docs/agent-start.md` remains similar to:

```go
type Provider interface {
    Complete(context.Context, ChatRequest) (ChatResponse, error)
    Stream(context.Context, ChatRequest, func([]byte) error) error
    Name() string
}
```

