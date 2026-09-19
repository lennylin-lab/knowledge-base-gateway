# Design: telemetry and process lifecycle

Use OpenTelemetry SDK/exporters behind a small internal setup package; domain
packages accept contexts and record spans through narrow helpers, never exporter
types. Prometheus remains the metrics surface and uses custom collectors for DB
queue depth/oldest age to avoid per-job labels. Trace IDs are stored as metadata
on jobs/audit rows and W3C trace context is normalized before persistence.

Readiness aggregates named checks with short independent deadlines. Queue health
means DB claim/query viability plus worker heartbeat when workers are enabled;
settlement backlog has an explicit unhealthy threshold. Shutdown ordering is
encoded in the process coordinator and covered with deterministic clocks/hooks.
