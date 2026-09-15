# Design: Input Quotas and Provider Safety

## Boundaries

Policy owns persisted subject ceilings, admission owns before-provider rejection, quota owns reserve/settle budget accounting, router owns breaker permit semantics, and provider URL validation owns startup-time SSRF protection for trusted configuration.

## Input-Limit Flow

Load max_input_tokens from access_policies into policy.Limits. During admission, compute the existing deterministic input size signal and compare it against subject and model ceilings before rate/quota/provider work. Preserve output clamping as a separate step.

## Router Flow

Routes.Available should return only routes whose breaker admitted a permit. If every breaker rejects, return no route until a breaker itself transitions to half-open and TryAcquirePermit succeeds. Recovery stays delegated to Breaker.Record.

## URL Safety

Reject unsafe IP ranges by default for provider base URLs. Development allowances should be explicit configuration behavior, not an accidental result of allowing all IP literals.

## Compatibility Notes

No public endpoint shapes change. The intended behavior change is earlier rejection of requests/configurations that exceeded documented ceilings or unsafe routing assumptions.
