package httpapi

// Shared admission pipeline for model-serving handlers. The security ordering
// is fixed and identical for every protocol, and authentication comes before
// any body read:
//
//	authentication -> bounded decoding (caller) -> default-model backfill
//	-> model/policy resolution -> capability precheck -> tool/schema validation
//	-> input ceilings -> policy/model output clamps -> rate limit
//	-> token quota reservation
//
// Invalid, expired, and revoked keys therefore receive their 401 before the
// caller drives any bounded parse work. Denials never reach a provider and
// are recorded by the caller through the audit sink.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/tracing"
)

// Protocol labels used by the admission pipeline and audit records. Existing
// labels ("chat", "responses") keep their wire meaning.
const (
	protocolChat       = "chat"
	protocolResponses  = "responses"
	protocolEmbeddings = "embeddings"
)

// statusWriter records the response status so the request span carries the
// final code when it ends.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Flush forwards to the wrapped writer when it supports flushing, so the
// SSE streaming paths keep their http.Flusher behavior through the wrapper.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// startHTTPSpan opens the server root span for one protocol request. The
// incoming W3C traceparent (when the caller supplied one) makes the span a
// child of the caller's trace; only trace identity is extracted — baggage is
// never read. Attributes are metadata only: request ID and protocol here,
// final status on finish.
func startHTTPSpan(w http.ResponseWriter, r *http.Request, protocol string) (*statusWriter, *http.Request, trace.Span) {
	ctx := tracing.Extract(r.Context(), r.Header)
	ctx, span := tracing.StartServer(ctx, "http."+protocol,
		attribute.String(tracing.AttrRequestID, requestIDOf(r)),
		attribute.String(tracing.AttrProtocol, protocol),
	)
	sw := &statusWriter{ResponseWriter: w}
	return sw, r.WithContext(ctx), span
}

// finishHTTPSpan stamps the recorded status and closes the span.
func finishHTTPSpan(sw *statusWriter, span trace.Span) {
	status := sw.status
	if status == 0 {
		status = http.StatusOK
	}
	span.SetAttributes(attribute.Int(tracing.AttrStatus, status))
	span.End()
}

// resolvePublicModel performs the default-model backfill for requests that
// omit `model`: chat/responses fall back to the subject's default_model and
// embeddings to default_embedding_model, before model resolution. An explicit
// model keeps the current behavior. Neither a configured default nor an
// explicit model is a stable 400 invalid_request.
func resolvePublicModel(d admissionDeps, protocol, subject, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	if d.Policy != nil {
		if def, ok := d.Policy.DefaultModelFor(subject, protocol); ok {
			return def, nil
		}
	}
	return "", fmt.Errorf("%w: model is required", model.ErrValidation)
}

// admissionDeps carries the collaborators shared by every model handler.
type admissionDeps struct {
	Auth       Authenticator
	Service    *gateway.Service
	Policy     *policy.Policy
	Limiter    limiter.Gate
	Quota      quota.Gate
	Accounting *accounting.Gate
	Audit      audit.Sink
	Metrics    *metrics.Registry
}

// fromChat builds the shared deps from a ChatHandler's fields.
func fromChat(h *ChatHandler) admissionDeps {
	return admissionDeps{
		Auth: h.Auth, Service: h.Service, Policy: h.Policy,
		Limiter: h.Limiter, Quota: h.Quota, Accounting: h.Accounting,
		Audit: h.Audit, Metrics: h.Metrics,
	}
}

// fromResponses builds the shared deps from a ResponsesHandler's fields.
func fromResponses(h *ResponsesHandler) admissionDeps {
	return admissionDeps{
		Auth: h.Auth, Service: h.Service, Policy: h.Policy,
		Limiter: h.Limiter, Quota: h.Quota, Accounting: h.Accounting,
		Audit: h.Audit, Metrics: h.Metrics,
	}
}

// bearer extracts the bearer token from the Authorization header.
func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}

// authenticate resolves the bearer key to a Principal. Handlers must run it
// before reading or decoding the request body so authentication failures are
// answered 401 with zero parse work done for the caller.
func authenticate(r *http.Request, d admissionDeps) (auth.Principal, error) {
	key, ok := bearer(r)
	if !ok {
		return auth.Principal{}, fmt.Errorf("%w: missing bearer key", auth.ErrInvalid)
	}
	return d.Auth.Authenticate(key, time.Now())
}

// admitted is the outcome of a successful admission: the ordered route plan,
// the token-quota reservation, the money-budget reservation, the limiter
// release func, the metrics registry for settlement observability, and the
// admission context so post-response settlement spans join the request trace.
type admitted struct {
	plan    gateway.Plan
	qres    quota.Reservation
	ares    accounting.Reservation
	release func()
	metrics *metrics.Registry
	ctx     context.Context
}

// settle finalizes both reservations to the reported usage exactly once
// (idempotent finalization; unknown usage keeps the conservative token
// reservation and the money counters unchanged). A settlement failure is
// never silent: it is counted and logged, and the ledger row stays
// 'reserved' as the retryable record.
func (a *admitted) settle(providerName string, u *model.Usage) {
	a.qres.Settle(usageTotal(u))
	if a.ares == nil {
		return
	}
	_, span := tracing.Start(a.ctx, "ledger.settle",
		attribute.String(tracing.AttrProvider, providerName))
	defer span.End()
	if err := a.ares.Settle(providerName, accounting.UsageFrom(u)); err != nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "failed"))
		if a.metrics != nil {
			a.metrics.IncSettlementFailure()
		}
		slog.Error("accounting: usage settlement failed; ledger row remains reserved",
			"error", err)
		return
	}
	span.SetAttributes(attribute.String(tracing.AttrOutcome, "settled"))
}

// refund releases both reservations (no billable outcome); repeated calls
// are no-ops. A failed ledger release is counted and logged: the row stays
// 'reserved' as evidence instead of being silently marked released.
func (a *admitted) refund() {
	a.qres.Release()
	if a.ares == nil {
		return
	}
	_, span := tracing.Start(a.ctx, "ledger.release")
	defer span.End()
	if err := a.ares.Release(); err != nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "failed"))
		if a.metrics != nil {
			a.metrics.IncSettlementFailure()
		}
		slog.Error("accounting: reservation release failed; ledger row remains reserved",
			"error", err)
		return
	}
	span.SetAttributes(attribute.String(tracing.AttrOutcome, "released"))
}

// attempts reports the maximum number of provider attempts the plan allows
// (candidates + bounded retries on the primary) for audit.
func (a *admitted) attempts() int {
	n := len(a.plan.Candidates)
	if n == 0 {
		return 1
	}
	return n
}

// admit runs the shared pipeline after authentication and bounded decoding:
// model/policy resolution -> capability precheck -> output clamps -> rate
// limit -> quota reserve. The subject scoping comes from the authenticated
// principal. Denials write the stable error envelope themselves and return a
// non-nil auditErr; the caller still owns audit recording (it owns the
// request/trace IDs, the principal, and the audit fields).
func admit(w http.ResponseWriter, r *http.Request, d admissionDeps, protocol, publicModel string, mreq *model.Request, principal auth.Principal) (out *admitted, outErr error) {
	// Admission span: the ordered pipeline is one trace segment; downstream
	// limiter/quota/accounting calls join it through the request context.
	// Attributes stay metadata-only (protocol, model, denial class).
	actx, span := tracing.Start(r.Context(), "http.admission",
		attribute.String(tracing.AttrProtocol, protocol),
		attribute.String(tracing.AttrModel, publicModel),
	)
	r = r.WithContext(actx)
	defer func() {
		if outErr != nil {
			span.SetAttributes(attribute.String(tracing.AttrErrorClass, classifyErr(outErr)))
		}
		span.End()
	}()
	subject := principal.SubjectID

	// 1. Model existence + subject policy (non-leaky combined errors).
	plan, err := d.Service.Resolve(subject, publicModel)
	if err != nil {
		return &admitted{}, err
	}
	if d.Policy != nil && !d.Policy.Permitted(subject, publicModel) {
		return &admitted{}, gateway.ErrNotPermitted
	}

	// 2. Capability + tool/schema precheck: rejected features and invalid
	// tool inputs never reach a provider.
	caps, ok := d.Service.Capabilities(publicModel)
	if !ok {
		return &admitted{}, gateway.ErrNoRoute
	}
	if err := model.CheckCapabilities(caps, protocol, *mreq); err != nil {
		return &admitted{}, err
	}
	if err := model.ValidateTools(mreq.Tools, caps.MaxTools); err != nil {
		return &admitted{}, err
	}

	// 3. Input ceilings: the deterministic input estimate (chars/4, the same
	// signal quota uses) must fit both the subject's persisted input ceiling
	// and the model's declared context window. Rejection happens before any
	// rate-limit, quota reservation, or provider work. Messages stay generic
	// and never echo ceiling values.
	inputTokens := quota.InputTokens(mreq.InputChars())
	if d.Policy != nil {
		if limits, ok := d.Policy.LimitsFor(subject); ok && limits.MaxInputTokens > 0 && inputTokens > int64(limits.MaxInputTokens) {
			return &admitted{}, fmt.Errorf("%w: input exceeds the maximum input size for this principal", model.ErrValidation)
		}
	}
	if caps.ContextTokens > 0 && inputTokens > int64(caps.ContextTokens) {
		return &admitted{}, fmt.Errorf("%w: input exceeds the model context window", model.ErrValidation)
	}

	// 4. Output clamps (chat/responses only: embeddings has no output budget).
	if protocol != protocolEmbeddings {
		if d.Policy != nil {
			if limits, ok := d.Policy.LimitsFor(subject); ok && limits.MaxOutputTokens > 0 {
				if mreq.MaxTokens == nil || *mreq.MaxTokens > limits.MaxOutputTokens {
					capped := limits.MaxOutputTokens
					mreq.MaxTokens = &capped
				}
			}
		}
		if caps.MaxOutputTokens > 0 && (mreq.MaxTokens == nil || *mreq.MaxTokens > caps.MaxOutputTokens) {
			capped := caps.MaxOutputTokens
			mreq.MaxTokens = &capped
		}
	}

	// 5. Rate/concurrency limits.
	ok, retryAfter, release, limErr := d.Limiter.Allow(r.Context(), subject, time.Now())
	if limErr != nil {
		// Limiter infrastructure failure: 503-class, never a rate-limit 429.
		return &admitted{}, limErr
	}
	if !ok {
		if retryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())+1))
		}
		if d.Metrics != nil {
			d.Metrics.IncRateLimit(publicModel)
		}
		return &admitted{}, &limiter.Error{Code: "rate_limit_exceeded"}
	}

	// 6. Token quota: reserve a deterministic bounded estimate before any
	// provider invocation. Subjects without a configured budget skip quota.
	// The reservation draws from the subject's single daily/monthly token
	// pool for every protocol (chat, responses, embeddings share one budget).
	qres := quota.Done
	if d.Quota != nil && d.Policy != nil {
		// Limit resolution goes through policy.Resolver.LimitsFor — the
		// single composition point for any future per-model quota override.
		limits, lErr := policy.NewResolver(d.Policy).LimitsFor(r.Context(), subject, publicModel)
		if lErr != nil {
			// Resolver infrastructure failure: fail closed after releasing
			// the limiter slot; never reserve against unknown limits.
			release()
			return &admitted{}, lErr
		}
		ql := quota.Limits{DailyTokens: limits.DailyTokens, MonthlyTokens: limits.MonthlyTokens}
		if ql.Configured() {
			// Embeddings usage is input-token only, so the conservative
			// reservation is the deterministic input estimate alone (no
			// output reserve).
			est := quota.InputTokens(mreq.InputChars())
			if protocol != protocolEmbeddings {
				est = quota.Estimate(mreq.MaxTokens, limits.MaxOutputTokens, mreq.InputChars())
			}
			res, qErr := d.Quota.Reserve(r.Context(), subject, ql, est, time.Now())
			if qErr != nil {
				var denial *quota.Error
				if errors.As(qErr, &denial) {
					if d.Metrics != nil {
						d.Metrics.IncRateLimit(publicModel)
					}
					if denial.RetryAfter > 0 {
						w.Header().Set("Retry-After", fmt.Sprintf("%d", int(denial.RetryAfter.Seconds())+1))
					}
				}
				release()
				return &admitted{}, qErr
			}
			qres = res
		}
	}

	// 7. Monetary budgets (V1.4): subject and tenant day/month money
	// budgets checked and reserved before any provider invocation, against
	// the same deterministic token estimate the token quota charged. A
	// configured budget with no applicable price fails here as
	// pricing_unavailable — a priceless request never bypasses the budget.
	// Ledger capture runs regardless of enforcement (the rollback posture);
	// without an accounting gate the request keeps its pre-V1.4 behavior.
	ares := accounting.Done
	if d.Accounting != nil {
		// The estimate mirrors the token quota's precedence exactly:
		// declared max_tokens, then the policy output ceiling, then the
		// default reserve. Embeddings reserve input tokens only.
		outputCeiling := 0
		if d.Policy != nil {
			limits, lErr := policy.NewResolver(d.Policy).LimitsFor(r.Context(), subject, publicModel)
			if lErr != nil {
				// Fail closed after refunding the earlier reservations; never
				// reserve money against an unknown ceiling.
				qres.Release()
				release()
				return &admitted{}, lErr
			}
			outputCeiling = limits.MaxOutputTokens
		}
		in := quota.InputTokens(mreq.InputChars())
		estimate := accounting.Usage{PromptTokens: &in}
		if protocol != protocolEmbeddings {
			out := int64(quota.DefaultOutputReserve)
			if mreq.MaxTokens != nil && *mreq.MaxTokens > 0 {
				out = int64(*mreq.MaxTokens)
			} else if outputCeiling > 0 {
				out = int64(outputCeiling)
			}
			estimate.CompletionTokens = &out
		}
		res, aErr := d.Accounting.Reserve(r.Context(), accounting.ReserveInput{
			Identity:    accounting.Identity{RequestID: mreq.RequestID},
			SubjectID:   principal.SubjectID,
			TenantID:    principal.TenantID,
			Protocol:    protocol,
			PublicModel: publicModel,
			Provider:    plan.Primary(),
			Estimate:    estimate,
			Now:         time.Now(),
		})
		if aErr != nil {
			// Refund everything reserved so a denial leaves no residue.
			qres.Release()
			release()
			if denial := (*accounting.Error)(nil); errors.As(aErr, &denial) {
				if d.Metrics != nil {
					d.Metrics.IncBudgetDenial(denial.Scope)
				}
				if denial.RetryAfter > 0 {
					w.Header().Set("Retry-After", fmt.Sprintf("%d", int(denial.RetryAfter.Seconds())+1))
				}
			}
			return &admitted{}, aErr
		}
		ares = res
	}
	return &admitted{plan: plan, qres: qres, ares: ares, release: release, ctx: actx}, nil
}

// classifyErr names an error for the audit error_class column without
// leaking content.
func classifyErr(err error) string {
	switch {
	case errors.Is(err, model.ErrOutputValidation):
		return "schema_validation_failed"
	case errors.Is(err, model.ErrCapabilityNotSupported):
		return "capability_not_supported"
	case err == nil:
		return ""
	default:
		return provider.ClassOf(err).String()
	}
}

// recordRequest writes the audit event and metrics for one handled request.
// The protocol label ("chat"/"responses") is metadata only.
func recordRequest(d admissionDeps, evt audit.Event, usage *model.Usage) {
	if d.Audit != nil {
		d.Audit.Write(evt)
	}
	if d.Metrics != nil {
		statusLabel := fmt.Sprint(evt.Status)
		d.Metrics.IncRequest(evt.Model, statusLabel)
		d.Metrics.ObserveDuration(evt.Model, statusLabel, time.Since(evt.CreatedAt))
		if usage != nil && usage.Known {
			d.Metrics.AddTokens(evt.Model, usage.PromptTokens, usage.CompletionTokens)
		}
		if evt.ErrorClass != "" && evt.ErrorClass != "internal" && evt.ErrorClass != "schema_validation_failed" {
			d.Metrics.IncUpstreamError(evt.Model, evt.Provider, evt.ErrorClass)
		}
	}
}

// usageTokens extracts prompt or completion tokens only when the upstream
// reported usage; unknown usage stays nil and is never coerced to zero.
func usageTokens(u *model.Usage, prompt bool) *int {
	if u == nil || !u.Known {
		return nil
	}
	v := u.PromptTokens
	if !prompt {
		v = u.CompletionTokens
	}
	return &v
}

// usageTotal reports the upstream total token count for quota settlement,
// or nil when usage is unknown so the conservative reservation is retained.
func usageTotal(u *model.Usage) *int64 {
	if u == nil || !u.Known {
		return nil
	}
	t := int64(u.TotalTokens)
	return &t
}
