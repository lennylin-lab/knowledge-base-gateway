package httpapi

// Background Responses jobs: creation from POST /v1/responses
// (background:true), status retrieval, and idempotent cancellation. The
// synchronous Responses path is untouched; admission is shared verbatim
// through admit(), so async requests pass exactly the same authentication,
// model authorization, capability checks, and limiter/quota gates. Only the
// gateway's normalized request and public response envelopes are ever
// persisted — never raw client bytes, credentials, or provider fields.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/tracing"
)

// Async bundles the background-job collaborators shared by the Responses
// create path and the query/cancel endpoints. A nil Jobs store disables
// background acceptance with the stable 503 job_queue_unavailable (the
// documented rollback posture).
type Async struct {
	Jobs           async.Store
	Cancels        *async.CancelRegistry
	Wake           func() // wakes the local worker pool after creation
	ResultTTL      time.Duration
	KeyTTL         time.Duration
	MaxResultBytes int
	MaxKeyBytes    int
}

// asyncEncoder implements async.ResultEncoder with the frozen Responses wire
// types, so stored results are byte-identical in shape to sync responses.
type asyncEncoder struct{}

// NewAsyncEncoder builds the Responses envelope encoder for the worker pool.
func NewAsyncEncoder() async.ResultEncoder { return asyncEncoder{} }

// EncodeResult renders a completed domain response as the public envelope.
// The job ID (resp_-prefixed) is the gateway-owned response identifier.
func (asyncEncoder) EncodeResult(publicModel, id string, resp model.Response, metadata []byte) ([]byte, error) {
	b, err := json.Marshal(encodeResponse(id, publicModel, resp, metadata))
	if err != nil {
		return nil, err
	}
	return b, nil
}

// EncodeFailure renders the public failure envelope for an error class.
func (asyncEncoder) EncodeFailure(publicModel, id, class string) ([]byte, error) {
	obj := encodeResponse(id, publicModel, model.Response{Status: "failed"}, nil)
	obj.Error = map[string]string{
		"code":    asyncFailureCode(class),
		"message": asyncFailureMessage(class),
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// asyncStatusOut is the non-terminal observable state envelope (the 202 body
// and the GET shape for queued/running/cancelled).
type asyncStatusOut struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Status    string `json:"status"`
	Model     string `json:"model"`
	Created   int64  `json:"created"`
	RequestID string `json:"request_id"`
}

// statusEnvelope renders the observable state envelope for one job.
func statusEnvelope(job async.Job, requestID string) asyncStatusOut {
	return asyncStatusOut{
		ID: job.ID, Object: "response", Status: string(job.Status),
		Model: job.PublicModel, Created: job.CreatedAt.Unix(), RequestID: requestID,
	}
}

// writeAsyncStatus writes the status envelope with the given code.
func writeAsyncStatus(w http.ResponseWriter, requestID string, code int, job async.Job, retryAfter time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	if retryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())+1))
	}
	w.Header().Set("X-Request-ID", requestID)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(statusEnvelope(job, requestID))
}

// Stable failure classes surfaced through stored results and audit records.
// Provider classes reuse the sync audit vocabulary; job-level classes are
// content-free and self-describing.
func asyncFailureCode(class string) string {
	switch class {
	case "timeout":
		return "timeout_error"
	case "network", "server":
		return "upstream_unavailable"
	case "rate_limited":
		return "upstream_rate_limited"
	case "invalid":
		return "upstream_rejected_request"
	default:
		return class
	}
}

func asyncFailureMessage(class string) string {
	switch asyncFailureCode(class) {
	case "timeout_error":
		return "the upstream request timed out"
	case "upstream_unavailable":
		return "the upstream is temporarily unavailable"
	case "upstream_rate_limited":
		return "the upstream is temporarily rate limited"
	case "upstream_rejected_request":
		return "the request was rejected by the model provider"
	case "quota_exceeded":
		return "token quota exceeded for this subject"
	case "rate_limit_exceeded":
		return "rate limit exceeded"
	case "model_not_allowed":
		return "the requested model is not available for this principal"
	case "no_route_available":
		return "no provider route is currently available"
	case "capability_not_supported":
		return "the requested capability is not supported by this model"
	case "schema_validation_failed":
		return "the response did not satisfy the requested structured-output specification"
	case "result_too_large":
		return "the response exceeded the stored result size limit"
	case "lease_lost":
		return "the task lost its worker lease and exhausted its attempts"
	default:
		return "the request failed"
	}
}

// getJobID extracts the {id} path value from the mux.
func getJobID(r *http.Request) string {
	return r.PathValue("id")
}

// createBackground accepts a background:true request: the shared admission
// already ran (its limiter/quota resources are probes only, released here),
// so persistence happens with the exact post-admission request snapshot.
func (h *ResponsesHandler) createBackground(w http.ResponseWriter, r *http.Request, deps admissionDeps, requestID, traceID string, principal auth.Principal, adm *admitted, publicModel string, mreq model.Request, start time.Time) {
	as := h.Async
	// Creation does not hold sync resources: the admission reservation was a
	// gate, the worker re-reserves at execution time.
	adm.refund()
	adm.release()

	record := func(status int, err error) {
		h.record(deps, requestID, traceID, principal, publicModel, "", status, err, adm.attempts(), nil, false, start, nil)
	}

	if as == nil || as.Jobs == nil {
		// Flag-off / no queue configured: the documented rollback posture.
		mapError(w, requestID, errQueueUnavailable)
		record(http.StatusServiceUnavailable, errQueueUnavailable)
		return
	}

	snap, err := async.EncodeSnapshot(mreq)
	if err != nil {
		mapError(w, requestID, err)
		record(http.StatusInternalServerError, err)
		return
	}
	keyHash := ""
	if key := strings.TrimSpace(r.Header.Get("Idempotency-Key")); key != "" {
		if len(key) > as.MaxKeyBytes {
			err := fmt.Errorf("%w: idempotency key too long", errValidation)
			mapError(w, requestID, err)
			record(http.StatusBadRequest, err)
			return
		}
		keyHash = async.HashIdempotencyKey(principal.SubjectID, key)
	}

	// The enqueue span is the trace's persistence boundary: its W3C identity
	// (normalized) rides the job row so the worker joins the caller's trace.
	// Baggage and request content never reach the row.
	ectx, enqueueSpan := tracing.Start(r.Context(), "async.enqueue",
		attribute.String(tracing.AttrModel, publicModel),
		attribute.String(tracing.AttrProtocol, protocolResponses),
	)
	defer enqueueSpan.End()
	tc := tracing.Current(ectx)

	out, err := as.Jobs.Create(ectx, async.CreateInput{
		JobID:         newJobID(),
		SubjectID:     principal.SubjectID,
		TenantID:      principal.TenantID,
		Protocol:      protocolResponses,
		PublicModel:   publicModel,
		RequestDigest: async.DigestRequest(snap),
		Request:       snap,
		KeyHash:       keyHash,
		TraceID:       tc.TraceID,
		SpanID:        tc.SpanID,
		TraceSampled:  tc.Sampled,
		ResultTTL:     as.ResultTTL,
		KeyTTL:        as.KeyTTL,
		Now:           time.Now(),
	})
	switch {
	case errors.Is(err, async.ErrConflict):
		enqueueSpan.SetAttributes(attribute.String(tracing.AttrOutcome, "idempotency_conflict"))
		mapError(w, requestID, errIdempotencyConflict)
		record(http.StatusConflict, errIdempotencyConflict)
		return
	case errors.Is(err, async.ErrUnavailable):
		enqueueSpan.SetAttributes(attribute.String(tracing.AttrOutcome, "queue_unavailable"))
		mapError(w, requestID, errQueueUnavailable)
		record(http.StatusServiceUnavailable, errQueueUnavailable)
		return
	case err != nil:
		enqueueSpan.SetAttributes(attribute.String(tracing.AttrOutcome, "error"))
		mapError(w, requestID, err)
		record(http.StatusInternalServerError, err)
		return
	}
	enqueueSpan.SetAttributes(
		attribute.String(tracing.AttrJobID, out.Job.ID),
		attribute.String(tracing.AttrOutcome, "queued"),
	)
	if as.Wake != nil {
		as.Wake()
	}
	writeAsyncStatus(w, requestID, http.StatusAccepted, out.Job, 0)
	record(http.StatusAccepted, nil)
}

// newJobID generates the gateway-owned public job/response identifier.
var jobCounter atomic.Int64

func newJobID() string {
	return fmt.Sprintf("resp_%d_%d", time.Now().UnixNano(), jobCounter.Add(1))
}

// AsyncJobsHandler serves GET /v1/responses/{id} and
// POST /v1/responses/{id}/cancel. Only the owning subject may see or cancel a
// job; any other caller — including for existing jobs — receives the same
// response_not_found, so existence never leaks across owners. Scoped admin
// credentials (V1.4) extend visibility: a caller presenting an admin
// credential with the viewer scope may GET any job (tenant-bound admins only
// jobs of their tenant), still under the non-leaky not-found envelope;
// cancellation stays with the owning subject. The legacy bootstrap token
// authenticates as the platform-admin identity on this surface too.
type AsyncJobsHandler struct {
	Auth    Authenticator
	Jobs    async.Store
	Cancels *async.CancelRegistry
	Audit   audit.Sink
	Metrics *metrics.Registry
	// AdminAuth authenticates scoped admin credentials on GET; nil disables
	// admin access and keeps the owner-only behavior.
	AdminAuth *adminauth.Authenticator
	// PollHint is the suggested query interval surfaced as Retry-After for
	// queued/running jobs.
	PollHint time.Duration
	Now      func() time.Time
}

// jobCaller is the resolved GET/cancel identity: the API-key subject, plus
// the admin principal when the caller presented an admin credential.
type jobCaller struct {
	subjectID string
	admin     *adminauth.Principal // nil for API-key callers
}

// ServeHTTP dispatches by method (the mux registers both routes).
func (h *AsyncJobsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.get(w, r)
	case http.MethodPost:
		h.cancel(w, r)
	default:
		writeError(w, requestIDOf(r), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST cancel")
	}
}

// authenticate resolves the caller or writes the error envelope. Routing:
// "kba_"-prefixed tokens are admin credentials (never valid API keys — the
// hex alphabet cannot contain '_'); any other token is tried as an API key
// first and, on failure and when the admin authenticator is wired, as the
// legacy bootstrap token (constant-time comparison happens inside the admin
// authenticator). requireViewer enforces the GET visibility scope for the
// admin path.
func (h *AsyncJobsHandler) authenticate(w http.ResponseWriter, r *http.Request, requireViewer bool) (jobCaller, bool) {
	requestID := requestIDOf(r)
	key, ok := bearer(r)
	if !ok {
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid API key")
		return jobCaller{}, false
	}
	if adminauth.HasCredentialPrefix(key) {
		return h.authenticateAdmin(w, r, key, requireViewer)
	}
	principal, err := h.Auth.Authenticate(key, time.Now())
	if err == nil {
		return jobCaller{subjectID: principal.SubjectID}, true
	}
	if h.AdminAuth != nil {
		if caller, ok := h.authenticateAdmin(w, r, key, requireViewer); ok {
			return caller, true
		}
		// The admin path wrote its own envelope (401/403/429); keep it.
		return jobCaller{}, false
	}
	mapError(w, requestID, err)
	return jobCaller{}, false
}

// authenticateAdmin resolves the admin credential or bootstrap token and
// enforces the caller's scope for the operation at hand.
func (h *AsyncJobsHandler) authenticateAdmin(w http.ResponseWriter, r *http.Request, key string, requireViewer bool) (jobCaller, bool) {
	requestID := requestIDOf(r)
	unauthorized := func() {
		// Uniform envelope: admin states are never enumerable on /v1 routes.
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid API key")
	}
	if h.AdminAuth == nil {
		unauthorized()
		return jobCaller{}, false
	}
	principal, post, err := h.AdminAuth.Authenticate(r.Context(), key, clientKey(r))
	if post != nil {
		go post() // bounded bookkeeping off the request path (success and failure)
	}
	var rlErr *adminauth.RateLimitError
	switch {
	case errors.As(err, &rlErr):
		if rlErr.RetryAfter > 0 {
			w.Header().Set("Retry-After", retryAfterText(rlErr.RetryAfter))
		}
		writeError(w, requestID, http.StatusTooManyRequests, "rate_limit_error", "admin_rate_limited", "too many failed admin authentications; retry later")
		return jobCaller{}, false
	case err != nil:
		unauthorized()
		return jobCaller{}, false
	}
	if requireViewer && !principal.Has(adminauth.ScopeViewer) {
		writeError(w, requestID, http.StatusForbidden, "permission_error", "insufficient_scope", "this admin credential is not authorized for the requested operation")
		return jobCaller{}, false
	}
	return jobCaller{subjectID: principal.AdminSubject, admin: &principal}, true
}

// loadOwnedJob fetches the job and enforces visibility with the non-leaky
// response_not_found: owning subject, or — for scoped admin callers — any
// job within a platform admin's reach, or the tenant boundary for
// tenant-bound admins. Job-store outages surface as job_queue_unavailable.
func (h *AsyncJobsHandler) loadOwnedJob(w http.ResponseWriter, r *http.Request, caller jobCaller) (async.Job, bool) {
	requestID := requestIDOf(r)
	job, err := h.Jobs.Get(r.Context(), getJobID(r), h.now())
	switch {
	case errors.Is(err, async.ErrNotFound):
		writeError(w, requestID, http.StatusNotFound, "invalid_request_error", "response_not_found", "the response does not exist for this principal")
		return async.Job{}, false
	case errors.Is(err, async.ErrUnavailable):
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "job_queue_unavailable", "the service is temporarily unable to accept requests")
		return async.Job{}, false
	case err != nil:
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "internal_error", "internal server error")
		return async.Job{}, false
	}
	if caller.admin != nil {
		// Scoped admin visibility: platform admins see every job;
		// tenant-bound admins only jobs of their tenant (same non-leaky
		// envelope as absence — the boundary never leaks job existence).
		if !caller.admin.Global() && job.TenantID != caller.admin.TenantID {
			writeError(w, requestID, http.StatusNotFound, "invalid_request_error", "response_not_found", "the response does not exist for this principal")
			return async.Job{}, false
		}
		return job, true
	}
	if job.SubjectID != caller.subjectID {
		// Same non-leaky envelope as absence: ownership never leaks.
		writeError(w, requestID, http.StatusNotFound, "invalid_request_error", "response_not_found", "the response does not exist for this principal")
		return async.Job{}, false
	}
	return job, true
}

func (h *AsyncJobsHandler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// get serves GET /v1/responses/{id}. Queries never trigger execution or
// polling work: queued/running carry a Retry-After hint, expired results
// return the stable 410. Scoped admin credentials with the viewer scope may
// read jobs within their tenant reach (the contained scope-check seam).
func (h *AsyncJobsHandler) get(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("X-Trace-ID", traceIDOf(r, requestID))
	_, span := tracing.Start(r.Context(), "async.job_query")
	defer func() {
		span.SetAttributes(attribute.String(tracing.AttrRequestID, requestID))
		span.End()
	}()
	caller, ok := h.authenticate(w, r, true)
	if !ok {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "unauthorized"))
		return
	}
	if h.Jobs == nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "queue_unavailable"))
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "job_queue_unavailable", "the service is temporarily unable to accept requests")
		return
	}
	job, ok := h.loadOwnedJob(w, r, caller)
	if !ok {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "not_found"))
		return
	}
	span.SetAttributes(attribute.String(tracing.AttrJobID, job.ID), attribute.String(tracing.AttrOutcome, string(job.Status)))
	switch job.Status {
	case async.StatusExpired:
		writeError(w, requestID, http.StatusGone, "invalid_request_error", "response_expired", "the response result has expired")
	case async.StatusQueued, async.StatusRunning:
		writeAsyncStatus(w, requestID, http.StatusOK, job, h.PollHint)
	case async.StatusCompleted, async.StatusFailed:
		res, err := h.Jobs.Result(r.Context(), job.ID)
		if err != nil {
			// A terminal job whose result row is missing falls back to the
			// status envelope rather than inventing an outcome.
			writeAsyncStatus(w, requestID, http.StatusOK, job, 0)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(res.Response)
	default:
		writeAsyncStatus(w, requestID, http.StatusOK, job, 0)
	}
}

// cancel serves POST /v1/responses/{id}/cancel. The store's conditional
// update decides cancel-versus-completion races; the winner of that decision
// writes the job's single cancellation handoff (audit + registry signal).
// Repeat cancels are idempotent and return the final observable state.
// Cancellation stays with the owning subject: admin credentials authenticate
// but are denied (the scoped-admin grant is visibility, not control).
func (h *AsyncJobsHandler) cancel(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("X-Trace-ID", traceIDOf(r, requestID))
	_, span := tracing.Start(r.Context(), "async.job_cancel")
	defer func() {
		span.SetAttributes(attribute.String(tracing.AttrRequestID, requestID))
		span.End()
	}()
	caller, ok := h.authenticate(w, r, false)
	if !ok {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "unauthorized"))
		return
	}
	if caller.admin != nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "forbidden"))
		writeError(w, requestID, http.StatusForbidden, "permission_error", "insufficient_scope", "admin credentials may query but not cancel background responses")
		return
	}
	if h.Jobs == nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "queue_unavailable"))
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "job_queue_unavailable", "the service is temporarily unable to accept requests")
		return
	}
	job, ok := h.loadOwnedJob(w, r, caller)
	if !ok {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "not_found"))
		return
	}
	span.SetAttributes(attribute.String(tracing.AttrJobID, job.ID))
	if job.Status == async.StatusExpired {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "expired"))
		writeError(w, requestID, http.StatusGone, "invalid_request_error", "response_expired", "the response result has expired")
		return
	}
	fresh, outcome, err := h.Jobs.Cancel(r.Context(), job.ID, h.now())
	switch {
	case errors.Is(err, async.ErrUnavailable):
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "queue_unavailable"))
		writeError(w, requestID, http.StatusServiceUnavailable, "service_unavailable", "job_queue_unavailable", "the service is temporarily unable to accept requests")
		return
	case err != nil:
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "error"))
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "internal_error", "internal server error")
		return
	}
	span.SetAttributes(attribute.String(tracing.AttrOutcome, map[async.CancelOutcome]string{
		async.CancelledQueued: "cancelled", async.CancelledRunning: "cancelled",
		async.CancelNoop: "noop",
	}[outcome]))
	if outcome != async.CancelNoop {
		// The cancel won the transition: it owns the job's single cancellation
		// handoff. Content-free metadata only.
		if h.Audit != nil {
			h.Audit.Write(audit.Event{
				RequestID: orJobID(fresh.FinalRequestID, fresh.ID),
				SubjectID: fresh.SubjectID, Model: fresh.PublicModel,
				ErrorClass: "cancelled", LatencyMillis: h.now().Sub(fresh.CreatedAt).Milliseconds(),
				CreatedAt: fresh.CreatedAt, TraceID: orJobID(fresh.FinalRequestID, fresh.ID),
				RouteAttempts: fresh.AttemptCount, Protocol: fresh.Protocol,
			})
		}
		if h.Cancels != nil {
			h.Cancels.Signal(fresh.ID) // no-op unless the lease is local
		}
	}
	writeAsyncStatus(w, requestID, http.StatusOK, fresh, 0)
}

func orJobID(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
