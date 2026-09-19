# Design: V1.4 integration architecture

## Boundaries

- `internal/httpapi`: additive wire contracts, stable errors, auth first.
- `internal/asyncjob`: state machine, leases, cancellation, worker runtime.
- `internal/accounting`: pricing, reservation/finalization, cost calculation.
- `internal/adminauth`: admin credential verification and scope decisions.
- `internal/store/pg`: authoritative repositories and transactions.
- `internal/quota`: Redis/local atomic budget counters behind interfaces.
- `internal/telemetry`: trace setup and propagation; existing metrics expands.

## Core Data Flow

```text
POST background -> auth/admission -> DB job + idempotency + reserve -> 202
worker claim -> normalized provider call -> result + audit + settle -> terminal
GET/cancel -> ownership/scope check -> conditional transition -> stable response
```

Terminal transitions and ledger finalization use conditional database updates
with unique request/job keys. Provider cancellation is best effort; database
state determines the observable winner.

## Persistence and Migrations

Migrations after version 5 add entities in dependency order. Constraints encode
closed states/scopes, idempotency and settlement uniqueness, non-negative integer
amounts, and indexed ownership/time queries. Versions 1-5 remain untouched.

## Compatibility and Security

`background` is optional and false by default. Sync routes retain their current
path and golden output. Stored results use normalized gateway types; logs and
audit retain metadata only. A non-owner lookup is indistinguishable from absence.

## Operations and Rollback

Async acceptance, workers, monetary enforcement, and OTLP export have separate
flags. Workers stop claiming before HTTP shutdown, finish or relinquish leases,
and readiness reports required dependencies. Data-bearing migrations roll back
only after writers are disabled and retention requirements are satisfied.
