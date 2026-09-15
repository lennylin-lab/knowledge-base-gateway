# Implementation Plan: Protocol and Streaming Correctness

## Ordered Checklist

1. Add failing tests for Chat/Responses tool validation before provider calls.
2. Add failing test for trailing JSON in /v1/responses.
3. Add provider contract test for OpenAI stream EOF without [DONE].
4. Add Anthropic non-streaming and streaming tests for missing usage fields.
5. Add streamed final-output validation tests for invalid tool arguments and structured output where applicable.
6. Implement the smallest shared validation and adapter changes required by the tests.
7. Run focused package tests, then go test ./... and go vet ./... before handoff.

## Validation Commands

- GOCACHE=/tmp/kb-gateway-go-cache go test ./internal/httpapi ./internal/model ./internal/provider ./internal/quota
- GOCACHE=/tmp/kb-gateway-go-cache go test ./...
- GOCACHE=/tmp/kb-gateway-go-cache go test -race ./...
- go vet ./...
- git diff --check

## Rollback Points

- Revert HTTP admission wiring separately from provider stream terminal changes if either breaks existing compatibility.
- Keep provider usage-presence changes local to Anthropic adapter wire structs and normalization.
