# Pricing ledger and monetary budgets

## Goal

Produce reproducible request costs and enforce subject/tenant monetary budgets
for synchronous and asynchronous work without treating unknown cost as zero.

## Requirements

- Version prices by provider/public model, effective interval, currency, and
  input/output/reasoning/cached-input token category using integer micro-units.
- Persist immutable usage and selected price version with idempotent
  reserve/settle/release; one request/job has at most one final ledger record.
- Preserve nullable usage/cost. A configured cost budget with no applicable
  price fails before provider work as `pricing_unavailable`.
- Enforce daily/monthly UTC subject and tenant budgets atomically across
  instances; policy rows fold min-of-declared and both dimensions must pass.
- Extend admin usage with known/unknown cost, price version, and budget usage.
- Settlement failures remain retryable and observable, never silently successful.

## Acceptance Criteria

- [ ] Every known cost can be recomputed exactly from tokens and price version;
  reasoning/cached tokens are charged only when reported.
- [ ] Concurrent instances cannot oversell either subject or tenant budgets.
- [ ] Cancel/pre-output failure releases; output settles known usage; unknown
  portions remain unknown; repeated finalization is a no-op.
- [ ] Redis/database failure maps to 503, not `budget_exceeded`; true exhaustion
  returns 429 with the correct UTC `Retry-After`.

## Out of Scope

- Payments, invoices, exchange rates, taxes, recharge, and revenue recognition.
