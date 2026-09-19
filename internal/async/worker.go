package async

// Worker pool for background Responses jobs. Claim discipline: every worker
// claims one job at a time under a bounded lease; heartbeats extend the lease
// while the provider call runs on a detached, job-timeout-bounded context.
// Terminal decisions are store CAS operations, so cancel/completion/failure
// and recovery races have exactly one winner, and the winner performs the one
// terminal audit/accounting handoff.
//
// Shutdown order (roadmap): stop claiming new work, give in-flight executions
// a bounded drain window to commit, then abort the remainder and hand their
// leases back to the queue. Expired leases are recovered by the sweep even
// when a process dies outright.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/tracing"
)

// CancelRegistry propagates client cancels to in-flight executions in this
// process. A cancel that wins its store transition signals the registry; when
// the job's lease belongs to a local worker the execution context is
// cancelled and the provider call observes it. Remote workers observe the
// same cancellation through heartbeat loss.
type CancelRegistry struct {
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// NewCancelRegistry builds an empty registry.
func NewCancelRegistry() *CancelRegistry {
	return &CancelRegistry{cancels: map[string]context.CancelFunc{}}
}

// Register tracks an in-flight execution. The returned func unregisters.
func (r *CancelRegistry) Register(jobID string, cancel context.CancelFunc) (unregister func()) {
	r.mu.Lock()
	r.cancels[jobID] = cancel
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.cancels, jobID)
		r.mu.Unlock()
	}
}

// Signal cancels a local execution for jobID if one is registered; unknown
// job IDs are a no-op (the cancel decision lives in the store either way).
func (r *CancelRegistry) Signal(jobID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	cancel := r.cancels[jobID]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// ResultEncoder renders terminal outcomes into the stored public envelope
// bytes. Implemented by the HTTP layer, which owns the wire types.
type ResultEncoder interface {
	// EncodeResult renders a completed domain response as the public envelope.
	EncodeResult(publicModel, id string, resp model.Response, metadata []byte) ([]byte, error)
	// EncodeFailure renders the public failure envelope for an error class.
	EncodeFailure(publicModel, id, class string) ([]byte, error)
}

// PoolDeps carries the worker collaborators.
type PoolDeps struct {
	Store      Store
	Service    *gateway.Service
	Policy     *policy.Policy
	Limiter    limiter.Gate
	Quota      quota.Gate
	Accounting *accounting.Gate
	Audit      audit.Sink
	Metrics    *metrics.Registry
	Logger     *slog.Logger
	Cancels    *CancelRegistry
	Encoder    ResultEncoder
	Now        func() time.Time
}

// PoolConfig bounds the pool and every job lifecycle knob. All values are
// process configuration (validated by internal/config).
type PoolConfig struct {
	WorkerID       string        // unique per process instance
	Count          int           // bounded worker concurrency
	PollInterval   time.Duration // queue poll cadence; also the sweep cadence
	Lease          time.Duration // heartbeat-extended claim lease
	JobTimeout     time.Duration // detached execution deadline per job
	MaxAttempts    int           // job-level executions before terminal failure
	ResultTTL      time.Duration // terminal result lifetime
	MaxResultBytes int           // stored result size cap
	Drain          time.Duration // bounded wait for in-flight jobs on stop
}

// Pool runs the workers and the recovery/expiry sweep.
type Pool struct {
	deps PoolDeps
	cfg  PoolConfig

	ctx        context.Context
	cancel     context.CancelFunc
	accepting  chan struct{}
	acceptOnce sync.Once
	wg         sync.WaitGroup
	stopped    chan struct{}
	stopOnce   sync.Once
	wake       chan struct{}

	started       atomic.Bool // set by Start, gates the readiness health signal
	inflight      atomic.Int64
	lastSweepNano atomic.Int64 // worker heartbeat: last completed sweep pass
}

// NewPool builds a pool; Start launches it.
func NewPool(deps PoolDeps, cfg PoolConfig) *Pool {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Pool{
		deps: deps, cfg: cfg,
		accepting: make(chan struct{}),
		stopped:   make(chan struct{}),
		wake:      make(chan struct{}, 1),
	}
}

// Start launches the worker goroutines and the sweep loop. Call Stop once.
func (p *Pool) Start(parent context.Context) {
	p.ctx, p.cancel = context.WithCancel(parent)
	p.started.Store(true)
	for i := 0; i < p.cfg.Count; i++ {
		p.wg.Add(1)
		go p.loop()
	}
	p.wg.Add(1)
	go p.sweep()
	go func() {
		<-p.ctx.Done()
		p.closeAccepting()
	}()
	p.setWorkerHealthy(true)
}

// Healthy is the readiness worker signal: the pool is accepting jobs and its
// sweep heartbeat is fresh within three poll intervals. A pool that has not
// started, or one past Stop, is never healthy; the startup window before the
// first sweep pass is granted (no evidence of failure yet).
func (p *Pool) Healthy() bool {
	if !p.started.Load() || !p.isAccepting() {
		return false
	}
	if nano := p.lastSweepNano.Load(); nano != 0 {
		return time.Since(time.Unix(0, nano)) <= 3*p.cfg.PollInterval
	}
	return true
}

// Wake nudges the queue poll so a freshly created job starts without waiting
// a full poll interval. Non-blocking; polling remains the correctness path.
func (p *Pool) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Stop ends intake, waits up to drain for in-flight jobs to commit, then
// aborts the remainder and hands their leases back to the queue. Safe to call
// multiple times. The three stages are logged and mirrored on the worker
// health gauge so shutdown is observable from metrics and logs alone.
func (p *Pool) Stop(drain time.Duration) {
	p.stopOnce.Do(func() {
		p.log().Info("async: shutdown: stopping job intake")
		p.closeAccepting() // 1. stop receiving new jobs
		p.setWorkerHealthy(false)
		if p.cancel != nil {
			p.cancel() // workers exit their claim loops
		}
		// 2. bounded drain: in-flight executions commit normally (their
		// contexts are detached from this pool's lifetime).
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			p.log().Info("async: shutdown: drain complete")
		case <-time.After(drain):
			// 3. reclaim worker leases: abort in-flight provider work; the
			// workers observe the abort and requeue under their own leases.
			p.log().Warn("async: shutdown: drain window elapsed; aborting in-flight executions",
				"inflight", p.inflight.Load(), "drain", drain.String())
			p.cancelInFlight()
			<-done
			p.log().Info("async: shutdown: in-flight executions aborted; leases returned to the queue")
		}
		close(p.stopped)
	})
	<-p.stopped
}

// cancelInFlight cancels every locally tracked execution context.
func (p *Pool) cancelInFlight() {
	if p.deps.Cancels == nil {
		return
	}
	p.deps.Cancels.mu.Lock()
	ids := make([]string, 0, len(p.deps.Cancels.cancels))
	for id := range p.deps.Cancels.cancels {
		ids = append(ids, id)
	}
	p.deps.Cancels.mu.Unlock()
	for _, id := range ids {
		p.deps.Cancels.Signal(id)
	}
}

func (p *Pool) closeAccepting() {
	p.acceptOnce.Do(func() { close(p.accepting) })
}

func (p *Pool) isAccepting() bool {
	select {
	case <-p.accepting:
		return false
	default:
		return true
	}
}

// loop is one worker: claim, execute, repeat. Concurrency is bounded by the
// number of loops.
func (p *Pool) loop() {
	defer p.wg.Done()
	for {
		if !p.isAccepting() {
			return
		}
		select {
		case <-p.ctx.Done():
			return
		case <-p.accepting:
			return
		default:
		}
		cl, ok, err := p.claim()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				p.log().Error("async: claim failed", "error", err)
			}
			p.sleep(p.cfg.PollInterval)
			continue
		}
		if !ok {
			p.sleepOrWake(p.cfg.PollInterval)
			continue
		}
		p.run(cl)
	}
}

// claim performs one store claim under a queue span and records the
// enqueue→claim latency (the queue-wait histogram) for the winner.
func (p *Pool) claim() (Claimed, bool, error) {
	ctx, span := tracing.Start(p.ctx, "async.claim")
	cl, ok, err := p.deps.Store.Claim(ctx, ClaimInput{
		Owner: p.cfg.WorkerID, Lease: p.cfg.Lease, Now: p.deps.Now(),
	})
	switch {
	case err != nil:
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "error"))
	case ok:
		span.SetAttributes(
			attribute.String(tracing.AttrOutcome, "claimed"),
			attribute.String(tracing.AttrJobID, cl.Job.ID),
		)
		if p.deps.Metrics != nil {
			p.deps.Metrics.ObserveQueueWait(p.deps.Now().Sub(cl.Job.CreatedAt))
		}
	default:
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "empty"))
	}
	span.End()
	return cl, ok, err
}

// sweep recovers expired leases, expires due results, and refreshes the
// pool's readiness heartbeat. Queue depth/age live on the scrape-time
// collector (internal/metrics RegisterQueueCollector), not here.
func (p *Pool) sweep() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.accepting:
			return
		case <-time.After(p.cfg.PollInterval):
		}
		p.lastSweepNano.Store(p.deps.Now().UnixNano())
		out, err := p.deps.Store.RecoverExpiredLeases(p.ctx, RecoverInput{
			MaxAttempts:   p.cfg.MaxAttempts,
			FailureResult: p.leaseLostResult,
			Now:           p.deps.Now(),
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			p.log().Error("async: lease recovery failed", "error", err)
		}
		if n := len(out.Requeued) + len(out.Failed); n > 0 {
			// Every recovered job was a lapsed worker lease (queue hand-back
			// or terminal lease-lost failure).
			if p.deps.Metrics != nil {
				p.deps.Metrics.AddLeaseExpired(n)
			}
		}
		for _, id := range out.Requeued {
			p.log().Warn("async: expired lease returned job to queue", "job_id", id)
			p.Wake()
		}
		for range out.Failed {
			p.incJob(string(StatusFailed))
		}
		for _, j := range out.Failed {
			p.auditLeaseLost(j)
		}
		if n, err := p.deps.Store.ExpireDueResults(p.ctx, p.deps.Now()); err != nil {
			if !errors.Is(err, context.Canceled) {
				p.log().Error("async: result expiry failed", "error", err)
			}
		} else {
			if n > 0 && p.deps.Metrics != nil {
				p.deps.Metrics.AddResultsExpired(n)
			}
			p.incExpired(n)
		}
		// KeyTTL reclamation: the idempotency mapping's expires_at is
		// enforced at lookup time; the sweep only reclaims the rows so the
		// mapping table cannot grow without bound.
		if n, err := p.deps.Store.SweepExpiredIdempotencyKeys(p.ctx, p.deps.Now()); err != nil {
			if !errors.Is(err, context.Canceled) {
				p.log().Error("async: idempotency key sweep failed", "error", err)
			}
		} else if n > 0 {
			p.log().Info("async: swept expired idempotency keys", "count", n)
		}
	}
}

// sleep sleeps d or until the pool context ends or intake stops.
func (p *Pool) sleep(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-p.ctx.Done():
	case <-p.accepting:
	}
}

// sleepOrWake sleeps d, waking early on a create notification.
func (p *Pool) sleepOrWake(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-p.wake:
	case <-p.ctx.Done():
	case <-p.accepting:
	}
}

func (p *Pool) log() *slog.Logger { return p.deps.Logger }

// run executes one claimed job end to end: decode, re-admit, apply the same
// limiter/quota/money-budget gates as the sync path, invoke the gateway
// service on a detached bounded context, then commit the outcome through the
// store CAS. The worker span joins the enqueue trace through the job's
// persisted W3C context (never baggage), so one trace links HTTP enqueue →
// worker → provider attempts → settlement.
func (p *Pool) run(cl Claimed) {
	p.inflight.Add(1)
	defer func() {
		p.inflight.Add(-1)
		p.setWorkerInflight()
	}()
	p.setWorkerInflight()
	start := p.deps.Now()
	job := cl.Job

	req, err := DecodeSnapshot(cl.Request)
	if err != nil {
		// An undecodable payload is a permanent condition; fail the job with
		// no provider attempt and no retry.
		p.commitFailure(job, "internal", "", start)
		return
	}

	// Re-admission against current state: a revoked grant, disabled model, or
	// dropped capability after enqueueing must not become an execution
	// bypass. Permanent failures fail the job; transient ones retry with
	// backoff and attempt accounting, so neither can cycle forever or
	// head-of-line-block the queue.
	mreq, plan, transientClass, fatalClass := p.readmit(job, req)
	if fatalClass != "" {
		p.commitFailure(job, fatalClass, "", start)
		return
	}
	if transientClass != "" {
		if job.AttemptCount+1 >= p.cfg.MaxAttempts {
			p.commitFailure(job, transientClass, "", start)
			return
		}
		p.requeue(job, true, p.cfg.PollInterval)
		return
	}

	// Rate limit: the background path never bypasses the sync limiter. A
	// genuine denial consumes an attempt and retries with backoff (a subject
	// over its limit all day eventually exhausts); a limiter OUTAGE requeues
	// free — infrastructure failure is never the job's fault and must never
	// surface as a terminal job failure.
	ok, _, release, limErr := p.deps.Limiter.Allow(p.ctx, job.SubjectID, p.deps.Now())
	if limErr != nil {
		p.requeue(job, false, p.cfg.PollInterval)
		return
	}
	if !ok {
		release()
		if job.AttemptCount+1 >= p.cfg.MaxAttempts {
			p.commitFailure(job, "rate_limit_exceeded", "", start)
			return
		}
		p.requeue(job, true, p.cfg.PollInterval)
		return
	}
	defer release()

	// Re-reservations against current state, both from the same
	// deterministic estimate the sync path uses: token quota first, then
	// monetary budgets, so the background path can never bypass either
	// gate the synchronous admission enforces.
	var limits policy.Limits
	if p.deps.Policy != nil {
		l, lErr := policy.NewResolver(p.deps.Policy).LimitsFor(p.ctx, job.SubjectID, job.PublicModel)
		if lErr != nil {
			// Resolver infrastructure failure: requeue free, never execute
			// against unknown limits.
			p.requeue(job, false, p.cfg.PollInterval)
			return
		}
		limits = l
	}

	// Token quota: reserve the deterministic estimate, settle to the
	// reported usage at the terminal handoff, release on outcomes that
	// produced no billable response.
	qres := quota.Done
	if p.deps.Quota != nil {
		ql := quota.Limits{DailyTokens: limits.DailyTokens, MonthlyTokens: limits.MonthlyTokens}
		if ql.Configured() {
			est := quota.Estimate(mreq.MaxTokens, limits.MaxOutputTokens, mreq.InputChars())
			res, qErr := p.deps.Quota.Reserve(p.ctx, job.SubjectID, ql, est, p.deps.Now())
			if qErr != nil {
				var denial *quota.Error
				if errors.As(qErr, &denial) {
					// Budget exhausted: retrying cannot help until the period
					// resets, so the job fails terminally with the stable class.
					p.commitFailure(job, "quota_exceeded", "", start)
					return
				}
				p.requeue(job, false, p.cfg.PollInterval)
				return
			}
			qres = res
		}
	}

	// Money budgets (V1.4): reserve the same estimate against the subject's
	// and tenant's monetary budgets. A configured budget with no applicable
	// price fails terminally as pricing_unavailable — the budget can never
	// be bypassed; infrastructure failures requeue free.
	money := accounting.Reservation(nil)
	if p.deps.Accounting != nil {
		in := quota.InputTokens(mreq.InputChars())
		estimate := accounting.Usage{PromptTokens: &in}
		if job.Protocol != protocolEmbeddings {
			out := quota.Estimate(mreq.MaxTokens, limits.MaxOutputTokens, mreq.InputChars()) - in
			estimate.CompletionTokens = &out
		}
		res, aErr := p.deps.Accounting.Reserve(p.ctx, accounting.ReserveInput{
			Identity:    accounting.Identity{JobID: job.ID},
			SubjectID:   job.SubjectID,
			TenantID:    job.TenantID,
			Protocol:    job.Protocol,
			PublicModel: job.PublicModel,
			Provider:    plan.Primary(),
			Estimate:    estimate,
			Now:         p.deps.Now(),
		})
		if aErr != nil {
			qres.Release()
			var denial *accounting.Error
			var class string
			switch {
			case errors.As(aErr, &denial):
				// Exhausted budget: retrying cannot help until the period
				// resets, so the job fails terminally.
				class = "budget_exceeded"
				if p.deps.Metrics != nil {
					p.deps.Metrics.IncBudgetDenial(denial.Scope)
				}
			case errors.Is(aErr, accounting.ErrPricingUnavailable),
				errors.Is(aErr, accounting.ErrBudgetConfig):
				// The budget cannot be evaluated (no price / invalid policy):
				// refusing loudly beats silently bypassing it.
				class = "pricing_unavailable"
				if errors.Is(aErr, accounting.ErrBudgetConfig) {
					class = "budget_configuration_error"
				}
			default:
				// Infrastructure outage: never the job's fault, never a
				// terminal outcome.
				p.requeue(job, false, p.cfg.PollInterval)
				return
			}
			p.commitFailure(job, class, "", start)
			return
		}
		money = res
	}
	res := reservations{quota: qres, money: money}

	// Detached bounded execution: the provider call survives HTTP shutdown
	// and pool intake stop, bounded by the job timeout, cancellable through
	// the registry when a client cancel wins its store transition. The worker
	// span is the child of the persisted enqueue span (remote parent) when
	// the job carries trace context, so execution joins the caller's trace.
	execCtx, cancelExec := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
	defer cancelExec()
	traceSource := "fresh_root"
	if tc := (tracing.TraceContext{TraceID: job.TraceID, SpanID: job.ParentSpanID, Sampled: job.TraceSampled}); !tc.Empty() {
		execCtx = tracing.WithRemote(execCtx, tc)
		traceSource = "persisted"
	}
	execCtx, workerSpan := tracing.Start(execCtx, "async.worker.execute",
		attribute.String(tracing.AttrJobID, job.ID),
		attribute.String(tracing.AttrModel, job.PublicModel),
		attribute.String(tracing.AttrProtocol, job.Protocol),
		attribute.Int(tracing.AttrAttempt, job.AttemptCount+1),
		attribute.String(tracing.AttrWorkerID, p.cfg.WorkerID),
		attribute.String(tracing.AttrTraceSource, traceSource),
	)
	defer workerSpan.End()
	unregister := p.deps.Cancels.Register(job.ID, cancelExec)
	defer unregister()

	heartbeatDone := make(chan struct{})
	go p.heartbeatLoop(job.ID, heartbeatDone, cancelExec)

	resp, providerName, err := p.deps.Service.Complete(execCtx, plan, mreq)
	close(heartbeatDone)

	if err != nil {
		workerSpan.SetAttributes(attribute.String(tracing.AttrErrorClass, providerClassOf(err)))
		p.runFailed(job, res, execCtx, err, providerName, start)
		return
	}
	workerSpan.SetAttributes(attribute.String(tracing.AttrOutcome, "completed"))
	p.runSucceeded(execCtx, job, res, mreq, resp, providerName, start)
}

// protocolEmbeddings mirrors the HTTP-layer protocol label for the one
// per-protocol accounting distinction (embeddings reserve input tokens only).
const protocolEmbeddings = "embeddings"

// reservations pairs the token-quota and money-budget reservation handles so
// every terminal path finalizes both exactly once. The zero value finalizes
// nothing.
type reservations struct {
	quota quota.Reservation
	money accounting.Reservation
}

// release refunds both reservations (no billable outcome). Idempotent.
func (r reservations) release() {
	if r.quota != nil {
		r.quota.Release()
	}
	if r.money != nil {
		_ = r.money.Release()
	}
}

// settle finalizes both reservations to the reported usage exactly once.
// Unknown usage keeps the conservative reservations; a settlement failure is
// returned so the caller makes it observable (the ledger row stays
// 'reserved' as the retryable record — never silently marked settled).
func (r reservations) settle(providerName string, usage *model.Usage) error {
	if r.quota != nil {
		r.quota.Settle(usageTotal(usage))
	}
	if r.money == nil {
		return nil
	}
	return r.money.Settle(providerName, accounting.UsageFrom(usage))
}

// runSucceeded commits a completed execution, converting output-validation
// failures and oversized results into settled terminal failures. The commit
// runs on the detached execution context: it must land during the shutdown
// drain window (the pool context is already stopping then), and a drain-timeout
// abort cancels the same context, so the commit can never outlive the job.
func (p *Pool) runSucceeded(execCtx context.Context, job Job, res reservations, mreq model.Request, resp model.Response, providerName string, start time.Time) {
	auditErr := model.ValidateOutput(mreq, resp)
	envelope, encErr := p.deps.Encoder.EncodeResult(job.PublicModel, job.ID, resp, mreq.Metadata)
	class := ""
	switch {
	case encErr != nil:
		class = "internal"
	case auditErr != nil:
		class = "schema_validation_failed"
	case len(envelope) > p.cfg.MaxResultBytes:
		class = "result_too_large"
	}
	reqID := p.requestID()
	if class == "" {
		commitCtx, commitSpan := tracing.Start(execCtx, "async.commit",
			attribute.String(tracing.AttrJobID, job.ID),
			attribute.String(tracing.AttrOutcome, "completed"))
		won, cErr := p.deps.Store.CommitSuccess(commitCtx, SuccessInput{
			JobID: job.ID, Owner: job.LeaseOwner,
			FinalRequestID:  reqID,
			Response:        envelope,
			Usage:           usageOf(resp.Usage),
			ResultBytes:     len(envelope),
			ResultExpiresAt: p.deps.Now().Add(p.cfg.ResultTTL),
			BumpAttempt:     true,
			Now:             p.deps.Now(),
		})
		commitSpan.SetAttributes(attribute.Bool("gw.won", won))
		commitSpan.End()
		// Single terminal handoff: the CAS winner settles both reservations
		// to the reported usage (unknown usage retains the conservative
		// reservation) and writes the one audit record; the loser releases —
		// except the cancel-after-output race, where the loser settles by
		// the known usage (see handoff).
		p.handoff(execCtx, won, cErr, job, reqID, res, resp.Usage, providerName, start, "")
		return
	}
	// The upstream produced a billable response the gateway cannot deliver as
	// stored: commit the failure with the real usage settled.
	failEnv, ferr := p.deps.Encoder.EncodeFailure(job.PublicModel, job.ID, class)
	if ferr != nil {
		res.release()
		p.log().Error("async: encode failure envelope", "job_id", job.ID, "error", ferr)
		return
	}
	commitCtx, commitSpan := tracing.Start(execCtx, "async.commit",
		attribute.String(tracing.AttrJobID, job.ID),
		attribute.String(tracing.AttrErrorClass, class))
	won, cErr := p.deps.Store.CommitFailure(commitCtx, FailureInput{
		JobID: job.ID, Owner: job.LeaseOwner,
		FinalRequestID:  reqID,
		ErrorClass:      class,
		Response:        failEnv,
		ResultBytes:     len(failEnv),
		ResultExpiresAt: p.deps.Now().Add(p.cfg.ResultTTL),
		BumpAttempt:     true,
		Now:             p.deps.Now(),
	})
	commitSpan.SetAttributes(attribute.Bool("gw.won", won))
	commitSpan.End()
	p.handoff(execCtx, won, cErr, job, reqID, res, resp.Usage, providerName, start, class)
}

// runFailed classifies an execution error and either requeues (retry-eligible
// pre-output failures have no partial output by construction of Complete) or
// commits the terminal failure.
func (p *Pool) runFailed(job Job, res reservations, execCtx context.Context, err error, providerName string, start time.Time) {
	// Abort hand-back (context.Canceled only): a shutdown drain, a client
	// cancel, or a lost lease stopped the execution before any outcome; the
	// lease returns to the queue without spending an attempt and without
	// backoff, so another worker (or the restarted process) takes over
	// immediately. A client-cancel abort reaches the same shape but its store
	// transition already decided the job; the requeue CAS below loses, the
	// reservations are released, and no second handoff is written. A
	// job-timeout deadline is NOT an abort: it falls through to the
	// retry/terminal classification below and consumes an attempt, so a hung
	// provider can never recycle a job forever.
	if errors.Is(execCtx.Err(), context.Canceled) {
		res.release()
		p.requeue(job, false, 0)
		return
	}
	if provider.RetryEligible(err) && job.AttemptCount+1 < p.cfg.MaxAttempts {
		res.release()
		p.requeue(job, true, p.cfg.PollInterval)
		return
	}
	class := providerClassOf(err)
	failEnv, ferr := p.deps.Encoder.EncodeFailure(job.PublicModel, job.ID, class)
	if ferr != nil {
		res.release()
		p.log().Error("async: encode failure envelope", "job_id", job.ID, "error", ferr)
		return
	}
	// The commit runs on the execution context so it lands inside the shutdown
	// drain window; a drain-timeout or client-cancel abort cancels the same
	// context and the lease falls to the recovery sweep instead.
	reqID := p.requestID()
	commitCtx, commitSpan := tracing.Start(execCtx, "async.commit",
		attribute.String(tracing.AttrJobID, job.ID),
		attribute.String(tracing.AttrErrorClass, class))
	won, cErr := p.deps.Store.CommitFailure(commitCtx, FailureInput{
		JobID: job.ID, Owner: job.LeaseOwner,
		FinalRequestID:  reqID,
		ErrorClass:      class,
		Response:        failEnv,
		ResultBytes:     len(failEnv),
		ResultExpiresAt: p.deps.Now().Add(p.cfg.ResultTTL),
		BumpAttempt:     true,
		Now:             p.deps.Now(),
	})
	commitSpan.SetAttributes(attribute.Bool("gw.won", won))
	commitSpan.End()
	p.handoff(execCtx, won, cErr, job, reqID, res, nil, providerName, start, class)
}

// readmit re-resolves model authorization, capabilities, and routing for the
// stored request. fatalClass names a terminal failure; transientClass names a
// requeue-worthy condition (disabled model / open breakers right now); an
// empty pair means the job may execute.
func (p *Pool) readmit(job Job, req model.Request) (model.Request, gateway.Plan, string, string) {
	plan, err := p.deps.Service.Resolve(job.SubjectID, req.PublicModel)
	if err != nil {
		if errors.Is(err, gateway.ErrNoRoute) {
			return req, plan, "no_route_available", ""
		}
		// Non-leaky: unknown and forbidden collapse to the same class.
		return req, plan, "", "model_not_allowed"
	}
	if p.deps.Policy != nil && !p.deps.Policy.Permitted(job.SubjectID, req.PublicModel) {
		return req, plan, "", "model_not_allowed"
	}
	caps, ok := p.deps.Service.Capabilities(req.PublicModel)
	if !ok {
		return req, plan, "no_route_available", ""
	}
	if err := model.CheckCapabilities(caps, job.Protocol, req); err != nil {
		return req, plan, "", "capability_not_supported"
	}
	req.Model = plan.Primary()
	return req, plan, "", ""
}

// heartbeatLoop extends the lease until the execution finishes; a lost lease
// cancels the execution context so the provider call stops. The heartbeat
// outlives pool shutdown on purpose: in-flight jobs must keep their leases
// through the drain window.
func (p *Pool) heartbeatLoop(jobID string, done <-chan struct{}, cancelExec context.CancelFunc) {
	ticker := time.NewTicker(p.cfg.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(context.Background(), p.cfg.Lease/3)
			err := p.deps.Store.Heartbeat(hbCtx, jobID, p.cfg.WorkerID, p.cfg.Lease, p.deps.Now())
			cancel()
			if err != nil {
				cancelExec() // lease lost or store outage: stop upstream work
				return
			}
		}
	}
}

// requeue hands a running job back to the queue under the recorded lease
// owner. A lost race (cancel or recovery already decided) is a no-op. The
// hand-back is a queue operation, not an execution step: it runs on a bounded
// background context so it still lands when the execution was aborted or the
// pool is shutting down — exactly the moment Stop's contract depends on it.
// backoff > 0 delays the job's next claimability (retry paths), so a
// repeatedly failing job retries on the poll cadence instead of in a tight
// loop and cannot starve younger queued jobs; 0 re-queues immediately
// (abort hand-backs).
func (p *Pool) requeue(job Job, bump bool, backoff time.Duration) {
	if job.LeaseOwner == "" {
		return
	}
	now := p.deps.Now()
	opts := RequeueOptions{BumpAttempt: bump}
	if backoff > 0 {
		opts.NoClaimBefore = now.Add(backoff)
	}
	ctx, cancel := context.WithTimeout(context.Background(), storeOpTimeout)
	defer cancel()
	won, err := p.deps.Store.Requeue(ctx, job.ID, job.LeaseOwner, opts, now)
	if err != nil {
		p.log().Error("async: requeue failed", "job_id", job.ID, "error", err)
		return
	}
	if won {
		p.Wake()
	}
}

// storeOpTimeout bounds worker store operations that must survive shutdown
// (lease hand-back); ordinary execution rides the job's execution context.
const storeOpTimeout = 5 * time.Second

// commitFailure commits a no-usage terminal failure (pre-execution
// classifications: re-admission, quota/budget denial, undecodable payload).
func (p *Pool) commitFailure(job Job, class, providerName string, start time.Time) {
	env, err := p.deps.Encoder.EncodeFailure(job.PublicModel, job.ID, class)
	if err != nil {
		p.log().Error("async: encode failure envelope", "job_id", job.ID, "error", err)
		return
	}
	reqID := p.requestID()
	won, cerr := p.deps.Store.CommitFailure(p.ctx, FailureInput{
		JobID: job.ID, Owner: job.LeaseOwner,
		FinalRequestID:  reqID,
		ErrorClass:      class,
		Response:        env,
		ResultBytes:     len(env),
		ResultExpiresAt: p.deps.Now().Add(p.cfg.ResultTTL),
		BumpAttempt:     true,
		Now:             p.deps.Now(),
	})
	p.handoff(p.ctx, won, cerr, job, reqID, reservations{}, nil, providerName, start, class)
}

// handoff performs the single terminal audit/accounting handoff. The store
// CAS decides: only the winner settles both reservations to the reported
// usage (unknown usage retains the conservative reservation) and writes the
// one terminal audit record; the loser releases its reservations and writes
// no audit record.
//
// One deliberate exception — the child-3 settlement handoff: when a client
// cancel wins the transition AFTER the provider produced reported usage, the
// cancelled job will never re-execute, so the losing worker records the one
// settlement from the usage it observed instead of dropping it (the roadmap's
// "jobs with existing output settle by known usage"). The cancel winner (the
// HTTP cancel handler) writes the cancellation audit record but knows no
// usage; the exactly-once ledger settlement makes the two racers safe: this
// loser writes the settled ledger row, and any repeated finalization is a
// no-op. Any other loss (lease recovery returns the job to the queue) still
// releases, because the retry re-reserves and settles.
func (p *Pool) handoff(ctx context.Context, won bool, cerr error, job Job, requestID string, res reservations, usage *model.Usage, providerName string, start time.Time, class string) {
	if cerr != nil {
		// Commit failed: never fabricate a settlement. The job is recovered
		// by lease expiry if the commit did not land.
		res.release()
		p.log().Error("async: terminal commit failed", "job_id", job.ID, "error", cerr)
		return
	}
	if !won {
		// A racing decision (cancel, recovery) owns the terminal handoff.
		if usage != nil && usage.Known && p.jobCancelled(job.ID) {
			// Cancel-after-output: settle by known usage. A settlement
			// failure stays observable and retryable via the reserved row.
			p.settleObserved(ctx, job, res, usage, providerName)
			return
		}
		res.release()
		return
	}
	p.settleObserved(ctx, job, res, usage, providerName)
	if class == "" {
		p.incJob(string(StatusCompleted))
	} else {
		p.incJob(string(StatusFailed))
	}
	if p.deps.Audit != nil {
		evt := audit.Event{
			RequestID: requestID, SubjectID: job.SubjectID,
			Model: job.PublicModel, Provider: providerName,
			ErrorClass: class, LatencyMillis: p.deps.Now().Sub(start).Milliseconds(),
			PromptTokens: usageTokens(usage, true), CompletionTokens: usageTokens(usage, false),
			CreatedAt: start, TraceID: requestID,
			RouteAttempts: 1, Protocol: job.Protocol,
		}
		if class == "" {
			evt.Status = 200
		}
		p.deps.Audit.Write(evt)
	}
}

// settleObserved finalizes the reservations under a settlement span, exactly
// once per outcome. A failure is counted and then retried once in-process:
// both settle halves are idempotent (the quota finalizer and the ledger's
// exactly-once settlement), so a bounded retry can only converge — and a
// still-failing settlement keeps its reserved ledger row as the durable,
// re-drivable evidence.
func (p *Pool) settleObserved(ctx context.Context, job Job, res reservations, usage *model.Usage, providerName string) {
	_, span := tracing.Start(ctx, "ledger.settle",
		attribute.String(tracing.AttrJobID, job.ID),
		attribute.String(tracing.AttrProvider, providerName))
	defer span.End()
	if err := res.settle(providerName, usage); err != nil {
		span.SetAttributes(attribute.String(tracing.AttrOutcome, "retrying"))
		p.settleFailed(job.ID, err)
		if p.deps.Metrics != nil {
			p.deps.Metrics.IncSettlementRetry()
		}
		if err := res.settle(providerName, usage); err != nil {
			span.SetAttributes(attribute.String(tracing.AttrOutcome, "failed"))
			p.settleFailed(job.ID, err)
			return
		}
	}
	span.SetAttributes(attribute.String(tracing.AttrOutcome, "settled"))
}

// settleFailed records a failed ledger settlement: counted, logged, and left
// retryable — the 'reserved' ledger row remains as the durable evidence and
// can be re-driven; it is never silently marked settled.
func (p *Pool) settleFailed(jobID string, err error) {
	if p.deps.Metrics != nil {
		p.deps.Metrics.IncSettlementFailure()
	}
	p.log().Error("accounting: usage settlement failed; ledger row remains reserved",
		"job_id", jobID, "error", err)
}

// jobCancelled reads the store's decision for a lost terminal race: true
// only when the job transitioned to cancelled (cancel won). Any read error
// answers false so the caller takes the safe default (release); the
// settlement would have failed on a broken store anyway.
func (p *Pool) jobCancelled(jobID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), storeOpTimeout)
	defer cancel()
	j, err := p.deps.Store.Get(ctx, jobID, p.deps.Now())
	if err != nil {
		return false
	}
	return j.Status == StatusCancelled
}

// auditLeaseLost writes the terminal audit record for a job the sweep
// terminal-failed after exhausting attempts with a dead lease. The sweep
// committed the failure result; this is that job's one handoff. Such a job
// never committed a final request ID, so the job ID (the client-visible
// response identifier) correlates the record.
func (p *Pool) auditLeaseLost(job Job) {
	if p.deps.Audit == nil {
		return
	}
	reqID := job.FinalRequestID
	if reqID == "" {
		reqID = job.ID
	}
	p.deps.Audit.Write(audit.Event{
		RequestID: reqID, SubjectID: job.SubjectID,
		Model: job.PublicModel, ErrorClass: "lease_lost",
		LatencyMillis: p.deps.Now().Sub(job.CreatedAt).Milliseconds(),
		CreatedAt:     job.CreatedAt, TraceID: reqID,
		RouteAttempts: job.AttemptCount, Protocol: job.Protocol,
	})
}

// leaseLostResult builds the stored failure for sweep-exhausted jobs.
func (p *Pool) leaseLostResult(jobID string) Result {
	env, err := p.deps.Encoder.EncodeFailure("", jobID, "lease_lost")
	if err != nil {
		return Result{ErrorClass: "lease_lost"}
	}
	return Result{Response: env, ErrorClass: "lease_lost", ResultBytes: len(env)}
}

// requestCounter names execution request IDs uniquely per process.
var requestCounter atomic.Int64

func (p *Pool) requestID() string {
	return fmt.Sprintf("req_%d_%d", p.deps.Now().UnixNano(), requestCounter.Add(1))
}

// providerClassOf names a provider error for storage without leaking content
// — the same vocabulary the sync audit path uses.
func providerClassOf(err error) string {
	return provider.ClassOf(err).String()
}

// usageOf converts known domain usage into the nullable stored shape; unknown
// usage stays nil and is never fabricated as zero.
func usageOf(u *model.Usage) *Usage {
	if u == nil || !u.Known {
		return nil
	}
	pr, co, to := int64(u.PromptTokens), int64(u.CompletionTokens), int64(u.TotalTokens)
	return &Usage{PromptTokens: &pr, CompletionTokens: &co, TotalTokens: &to}
}

// usageTotal reports the upstream total for quota settlement, or nil to keep
// the conservative reservation (mirrors the sync handler's rule).
func usageTotal(u *model.Usage) *int64 {
	if u == nil || !u.Known {
		return nil
	}
	t := int64(u.TotalTokens)
	return &t
}

// usageTokens mirrors the sync audit projection.
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

// metric hooks (no-ops when the registry is absent).

func (p *Pool) incJob(status string) {
	if p.deps.Metrics != nil {
		p.deps.Metrics.IncAsyncJob(status)
	}
}

func (p *Pool) setWorkerHealthy(v bool) {
	if p.deps.Metrics != nil {
		p.deps.Metrics.SetWorkerHealthy(v)
	}
}

func (p *Pool) setWorkerInflight() {
	if p.deps.Metrics != nil {
		p.deps.Metrics.SetWorkerInflight(int(p.inflight.Load()))
	}
}

func (p *Pool) incExpired(n int) {
	if p.deps.Metrics != nil {
		for i := 0; i < n; i++ {
			p.deps.Metrics.IncAsyncJob(string(StatusExpired))
		}
	}
}
