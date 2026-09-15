# Technical Design: V1.2 Unified Protocol and Developer Platform

## Architecture and Boundaries

Keep `internal/httpapi` responsible for authentication middleware, bounded decoding, public schemas, error envelopes, and SSE framing. Add a provider-neutral protocol/domain layer for model requests, responses, events, tool definitions, response specifications, usage, and capabilities. Add a Responses handler/encoder and a models/discovery handler; adapt the existing Chat handler into the same domain layer without changing its external encoding. Extend `internal/provider` with capability discovery and normalized complete/stream operations while retaining vendor-specific translation inside adapters. Reuse V1.1 auth, policy, router, limits, audit, metrics, and store boundaries; add read-only management query services over those interfaces.

## Request and Event Flow

1. Assign request/trace IDs and authenticate the caller.
2. Decode and bound either Chat Completions or Responses input, then translate it to a domain request.
3. Resolve the public model and capability matrix, enforce protocol/capability/size/schema/tool limits, and run existing tenant/policy/limit checks.
4. Route through the V1.1 provider router and invoke a normalized provider operation with the total deadline and cancellation context.
5. Encode a domain response as Chat JSON, Responses JSON, or protocol-specific SSE events. For streaming, emit `response.created`, deltas/done events, and completed/failed terminal events; emit a unified pre-first-event failure and never fail over after the first event.
6. Persist metadata-only audit and publish metrics/traces with model, provider, protocol, capability, status, usage, and request ID.

## Domain Contracts

The domain request carries public model, normalized input items, instructions, generation controls, tools/tool choice, stream flag, response specification, metadata, and capability requirements. The domain response carries stable text, tool-call, refusal, status, and usage data. Domain events represent created, text delta/done, function argument delta/done, completed, and failed states. Capabilities include supported protocols, stream, tools, structured output, JSON mode, vision, reasoning, context/output limits, input types, tool count, and usage support.

## Tool and Structured Output Handling

Validate tool names, JSON schemas, count, byte size, and nesting before routing. Translate definitions at provider edges and normalize returned calls. Accumulate streaming argument fragments with deterministic final JSON validation; return a protocol error for incomplete/invalid arguments. Validate response-format schema/version/size before invocation and validate final JSON/schema output when configured. Tool results are input for a later request; no gateway execution or persistence of content is added.

## Discovery and Management

`GET /v1/models` filters the persisted catalog by caller authorization and returns only public capability/limit metadata. `GET /v1/models/{model}` includes protocol support, context/output limits, status, and public configuration version while omitting provider names, URLs, secrets, backup details, and other tenants' policies. Management endpoints or CLI use separate admin authentication and authorization, call domain-focused query services, and append management-operation audit events.

## Provider Tests and Fixtures

Define one offline contract suite against a mock transport/fixture interface and run it for each adapter. Fixtures cover normal and streaming text, usage, tool calls and deltas, JSON/schema output, unsupported capability, cancellation, malformed payloads, timeout, 400/401/429/5xx, and provider-private error mapping. Protocol replay and end-to-end tests use the same fixtures as documentation smoke examples.

## Compatibility, Rollout, and Rollback

First lock V1 Chat behavior with golden tests, then introduce the domain translation internally, then expose Responses and individual capabilities behind model/tenant flags. New fields remain additive under `/v1`; semantic breaks require `/v2`, and an explicit deprecation window is documented. Disable a capability/model route or the Responses endpoint independently if a provider or encoder fails; revert additive code/migrations without deleting audit history.

## Security and Operations

Continue to redact authorization headers, secrets, URLs, prompts, completions, and tool arguments. Bound body size, nested depth, schema/tool counts, and upstream response size. Preserve auth-before-provider ordering, request/trace correlation, total deadlines, cancellation propagation, and the existing retry/no-switch-after-output rules.
