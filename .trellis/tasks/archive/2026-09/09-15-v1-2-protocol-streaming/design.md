# Design: Protocol and Streaming Correctness

## Boundaries

Keep validation at the earliest boundary that has enough information: HTTP decoders own exact JSON shape, shared admission owns model capability/tool/schema prechecks, provider adapters own upstream terminal semantics and usage presence, and protocol encoders own stream framing.

## Data Flow

Requests decode into domain model.Request values, admission validates capabilities and tools before route invocation, providers emit normalized model.Event values, stream encoders assemble final model.Response values, and handlers validate final output before recording success.

## Compatibility Notes

Chat Completions must keep its existing public chunks and data: [DONE] terminal marker. Responses streams may emit response.failed when a final validation/protocol failure is discovered. Non-streaming output validation remains audit-visible and must not leak content.

## Risk Controls

Regression tests must prove rejected requests do not increment provider call counters. Stream terminal tests must distinguish upstream truncation from a valid provider completion. Usage tests must assert nil/unknown usage is preserved through audit and quota settlement.
