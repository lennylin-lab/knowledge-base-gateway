# Implementation plan: observability

1. Define metric names/labels, span topology, readiness rules, and redaction tests.
2. Add OTLP config and tracer lifecycle with no-op defaults and bounded export.
3. Instrument HTTP/job/provider/store/accounting boundaries and DB collectors.
4. Extend readiness and implement ordered graceful shutdown/drain.
5. Document dashboards, alert suggestions, sampling, and troubleshooting queries.
6. Run unit/race, test-exporter trace, cardinality, dependency fault, shutdown,
   container smoke, and full compatibility tests.

Rollback: disable OTLP by config; instrumentation becomes no-op. New readiness
checks can be independently disabled only when the corresponding feature is off.
