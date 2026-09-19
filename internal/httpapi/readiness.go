package httpapi

// Readiness implements /readyz: named dependency checks run per request with
// short independent deadlines, and the response body distinguishes which
// dependency degraded. A required check failing makes the process not ready
// (the load balancer denies traffic — the client-facing denial); a
// non-required check failing reports "degraded" without withdrawing
// readiness, so an auxiliary signal can never take the gateway offline.
// Checks are registered only while their feature is enabled — a disabled
// feature has no check and can never fail readiness.
//
// Liveness (/healthz) stays process-only by contract: it never runs these
// dependency probes.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// checkDeadline bounds each dependency probe independently so one slow
// dependency cannot starve the others' deadline budget.
const checkDeadline = 2 * time.Second

// CheckFunc runs one dependency probe. Returning a non-nil error marks the
// check failed (or degraded, when not required).
type CheckFunc func(context.Context) error

type readinessCheck struct {
	name     string
	required bool
	fn       CheckFunc
}

// Readiness aggregates named checks. Register before the server starts
// serving; ServeHTTP is safe for concurrent use.
type Readiness struct {
	Logger *slog.Logger

	mu     sync.Mutex
	checks []readinessCheck
}

// NewReadiness builds an empty aggregate.
func NewReadiness(logger *slog.Logger) *Readiness { return &Readiness{Logger: logger} }

// Register adds one named check. required=true failures withdraw readiness
// (503); required=false failures report degraded but stay ready.
func (r *Readiness) Register(name string, required bool, fn CheckFunc) {
	r.mu.Lock()
	r.checks = append(r.checks, readinessCheck{name: name, required: required, fn: fn})
	r.mu.Unlock()
}

type readinessBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// ServeHTTP runs every check and renders the aggregated verdict.
func (r *Readiness) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	checks := make([]readinessCheck, len(r.checks))
	copy(checks, r.checks)
	r.mu.Unlock()

	status := http.StatusOK
	body := readinessBody{Status: "ready", Checks: make(map[string]string, len(checks))}
	for _, c := range checks {
		ctx, cancel := context.WithTimeout(req.Context(), checkDeadline)
		err := c.fn(ctx)
		cancel()
		switch {
		case err == nil:
			body.Checks[c.name] = "ok"
		case !c.required:
			body.Checks[c.name] = "degraded"
			if r.Logger != nil {
				r.Logger.Warn("readiness: optional check degraded", "check", c.name, "error", err.Error())
			}
		default:
			body.Checks[c.name] = "failed"
			status = http.StatusServiceUnavailable
			body.Status = "unavailable"
			if r.Logger != nil {
				r.Logger.Warn("readiness: required check failed", "check", c.name, "error", err.Error())
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
