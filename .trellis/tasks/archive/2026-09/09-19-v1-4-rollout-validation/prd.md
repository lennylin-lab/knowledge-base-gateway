# V1.4 end-to-end rollout verification

## Goal

Prove the integrated release under realistic infrastructure, provider, failure,
load, security, migration, and rollback conditions before enabling tenants.

## Requirements

- Independent global, model, and tenant controls for async acceptance and cost
  enforcement; disabled behavior preserves V1.3 contracts.
- End-to-end scenarios with fake, OpenAI-compatible, and Anthropic adapters plus
  real PostgreSQL/Redis, multiple gateway/worker instances, restarts, and faults.
- Load tests cover queue latency/depth, lease churn, budget contention, admin
  limits, export/partition maintenance, and online request latency.
- Produce operator upgrade, configuration, capacity, monitoring, incident,
  rollback, and client integration documentation.
- Rollout sequence: dark writes/metrics, selected models, internal tenant,
  progressive tenants, then defaults; every stage has stop/rollback thresholds.

## Acceptance Criteria

- [ ] Full roadmap acceptance matrix passes, including permissions, fault
  injection, load, migration up/down, container startup, and zero skipped gates.
- [ ] V1.3 goldens remain byte-identical and flags-off deployments behave as V1.3.
- [ ] Capacity results establish worker count, lease/heartbeat, result-size,
  polling, settlement backlog, and readiness thresholds.
- [ ] A staged rollout and rollback rehearsal completes without lost jobs,
  duplicate ledger finalization, budget oversell, or data leakage.

## Out of Scope

- Production deployment execution and cross-region active-active certification.
