# Implementation Plan: Input Quotas and Provider Safety

## Ordered Checklist

1. Add policy/store tests showing max_input_tokens loads and is enforced before provider calls.
2. Add HTTP admission tests for model ContextTokens and subject MaxInputTokens rejection.
3. Add router concurrency tests for all-routes-open behavior before and after cooldown.
4. Add provider URL safety tests for unsafe IPv4/IPv6 literals and allowed development cases.
5. Implement policy limit field plumbing and admission checks without disturbing output clamps or quota settlement.
6. Implement router.Available permit semantics without adding provider-specific branches.
7. Implement URL safety helper(s) and update docs only if development behavior changes.
8. Run focused tests and full validation commands.

## Validation Commands

- GOCACHE=/tmp/kb-gateway-go-cache go test ./internal/httpapi ./internal/policy ./internal/store/pg ./internal/router ./internal/provider ./internal/quota
- GOCACHE=/tmp/kb-gateway-go-cache go test ./...
- GOCACHE=/tmp/kb-gateway-go-cache go test -race ./...
- go vet ./...
- git diff --check

## Rollback Points

- Policy input ceiling plumbing can be reverted independently from router and URL-safety changes.
- If hostname/IP handling proves too broad, keep production rejection strict and narrow only the documented development path.
