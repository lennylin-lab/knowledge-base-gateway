// Command gateway runs the LLM gateway process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/adminauth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/dberr"
	"github.com/knowledge-base/knowledge-base-gateway/internal/envfile"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
	"github.com/knowledge-base/knowledge-base-gateway/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := envfile.Load(".env"); err != nil {
		logger.Error("failed to load .env", "error", err)
		os.Exit(1)
	}

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("configuration invalid", "error", err)
		os.Exit(1)
	}

	// OTLP trace export (independent kill switch, default off): with the
	// switch off the no-op provider stays installed and nothing else in the
	// process changes. A shutdown flush is bounded by the exit deadline.
	shutdownTracing, err := telemetry.Setup(context.Background(), telemetry.Options{
		Enabled:  cfg.OTLPEnabled,
		Endpoint: cfg.OTLPEndpoint,
		Insecure: cfg.OTLPInsecure,
		Ratio:    cfg.OTLPRatio,
		Timeout:  cfg.OTLPTimeout,
	}, logger)
	if err != nil {
		logger.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			logger.Warn("telemetry shutdown: unflushed spans dropped", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}
}

// run assembles the gateway from the validated configuration and serves until
// ctx is cancelled. Every wiring failure — database connect, provider
// registry validation, catalog and route loading — is returned before the
// HTTP listener starts, so an invalid provider registry can never reach a
// state where /readyz could report ready.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	// Optional PostgreSQL persistence. When configured, catalog, routes,
	// policies, keys, and audit records live in the database.
	var (
		dbw            *pgstore.DB
		authenticator  httpapi.Authenticator
		lifecycleStore auth.MutationStore
		auditSink      audit.Sink
		accountingGate *accounting.Gate
		ledgerStore    *pgstore.LedgerStore
	)
	if cfg.DatabaseURL != "" {
		var err error
		dbw, err = pgstore.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			// pgx parse/connect errors embed parts of the connection string
			// (host, user, database, password-redacted URL). Classify the
			// failure instead of wrapping it so nothing DSN-shaped reaches
			// the logs.
			return fmt.Errorf("database connect: %s", dberr.DescribeConnectFailure(err))
		}
		defer dbw.Close()
		authenticator = &pgstore.Authenticator{DB: dbw, Now: time.Now}
		lifecycleStore = dbw
		auditSink = pgAudit{db: dbw}
		// V1.4 cost governance: the ledger is authoritative in database mode
		// and budget enforcement follows GATEWAY_BUDGETS_ENABLED (the
		// rollback point — enforcement off never disables ledger capture).
		ledgerStore = &pgstore.LedgerStore{DB: dbw}
	}

	// Provider registry. Secrets come only from the environment; base URLs
	// are validated against SSRF rules and every enabled provider's
	// credential is checked before the server can start.
	providers := map[string]provider.Provider{}
	var catalog *policy.Catalog
	if dbw != nil {
		// Database mode: the persisted registry is authoritative for kind
		// and base URL; the legacy GATEWAY_PROVIDER selector is ignored.
		pcfgs, err := dbw.LoadProviders(ctx)
		if err != nil {
			return fmt.Errorf("load providers: %w", err)
		}
		for _, pc := range pcfgs {
			p, err := newProviderFromRegistry(cfg, pc.Kind, pc.Name, pc.BaseURL)
			if err != nil {
				return err
			}
			providers[pc.Name] = p
		}
		entries, err := dbw.LoadCatalog(ctx)
		if err != nil {
			return fmt.Errorf("load catalog: %w", err)
		}
		catalog = policy.NewCatalog(entries)
	} else {
		switch cfg.Provider {
		case "openai":
			p, err := newProviderFromRegistry(cfg, "openai", "openai", cfg.OpenAIURL)
			if err != nil {
				return err
			}
			providers["openai"] = p
		case "anthropic":
			p, err := newProviderFromRegistry(cfg, "anthropic", "anthropic", cfg.AnthropicURL)
			if err != nil {
				return err
			}
			providers["anthropic"] = p
		default:
			providers["fake"] = provider.Fake{}
		}
		var entries []policy.ModelInfo
		for _, m := range cfg.Models {
			entries = append(entries, policy.ModelInfo{
				PublicName: m.PublicName, Provider: m.Provider, UpstreamModel: m.UpstreamModel, Enabled: m.Enabled,
			})
		}
		catalog = policy.NewCatalog(entries)
	}

	svc := gateway.New(catalog, providers, cfg.RequestTimeout, cfg.MaxRetries)

	// V1.4 background jobs execute on their own Service: the synchronous
	// total deadline (cfg.RequestTimeout, unchanged since V1.3) must never cap
	// a queued job, so the async service's total deadline is the job timeout.
	// The live route table is shared, so breaker state and route/model
	// administration affect both paths identically; only the deadline differs.
	asyncSvc := gateway.New(catalog, providers, cfg.AsyncJobTimeout, cfg.MaxRetries)
	asyncSvc.Routes = svc.Routes

	// Persisted primary/backup routes replace the single-candidate defaults.
	if dbw != nil {
		routeCfgs, err := dbw.LoadRoutes(ctx)
		if err != nil {
			return fmt.Errorf("load routes: %w", err)
		}
		seen := map[string]bool{}
		for _, rc := range routeCfgs {
			if seen[rc.PublicModel] {
				continue
			}
			seen[rc.PublicModel] = true
			rs, err := routeSet(rc.PublicModel, routeCfgs, providers)
			if err != nil {
				return err
			}
			svc.Routes.SetRoutes(rc.PublicModel, rs)
		}
	}

	// Policy: dev mode allows each configured subject every model; database
	// mode loads explicit model grants and limits. A subject without a grant
	// row stays denied (the HTTP layer collapses that to the same non-leaky
	// 403 as an unknown model).
	pol := policy.New()
	if dbw != nil {
		limits, err := dbw.LoadLimits(ctx)
		if err != nil {
			return fmt.Errorf("load policies: %w", err)
		}
		for subject, l := range limits {
			pol.SetLimits(subject, l)
		}
		grants, err := dbw.LoadGrants(ctx)
		if err != nil {
			return fmt.Errorf("load policy grants: %w", err)
		}
		for _, g := range grants {
			pol.Allow(g.Subject, g.Model)
		}
	} else {
		subjects := map[string]struct{}{}
		for _, k := range cfg.Keys {
			subjects[k.Subject] = struct{}{}
		}
		for s := range subjects {
			pol.AllowAll(s)
		}
		// Development-mode default models (GATEWAY_DEFAULT_MODELS). Database
		// mode ignores this list; access_policies carries the slots there.
		for _, d := range cfg.DefaultModels {
			pol.SetDefault(d.Subject, d.ChatModel, d.EmbeddingModel)
		}
	}

	// API keys: database mode reads them from PostgreSQL; otherwise dev keys
	// from the environment are hashed at load.
	var keyAuth httpapi.Authenticator = authenticator
	var keyManager *auth.Manager
	if keyAuth == nil {
		store := auth.NewStore()
		for _, k := range cfg.Keys {
			salt, err := auth.NewSalt()
			if err != nil {
				return fmt.Errorf("generate salt: %w", err)
			}
			store.Put(auth.KeyRecord{
				ID: k.ID, Subject: k.Subject, Salt: salt,
				Hash: auth.HashAPIKey(salt, k.PlaintextKey), Status: auth.StatusActive,
			})
		}
		keyAuth = store
		lifecycleStore = store
	}
	keyManager = auth.NewManager(lifecycleStore)

	// Limits: in-memory is development-only; Redis mode must be verified by
	// readiness before traffic is served. The token-quota gate shares the
	// mode: in-memory for single-process development, Redis for
	// multi-instance atomic daily/monthly budgets.
	var rateLimiter limiter.Gate
	var quotaGate quota.Gate
	var rdb *redis.Client
	if cfg.RedisEnabled {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		rl := limiter.NewRedis(rdb, "gw", cfg.RatePerMinute, cfg.MaxConcurrent)
		if dbw != nil {
			if limits, err := dbw.LoadLimits(ctx); err == nil {
				rl.SetLookup(func(subject string) (rate, conc int) {
					if l, ok := limits[subject]; ok {
						return l.RatePerMinute, l.MaxConcurrent
					}
					return 0, 0
				})
			}
		}
		rateLimiter = rl
		quotaGate = quota.NewRedis(rdb, "gw")
		defer rdb.Close()
	} else {
		rateLimiter = limiter.New(cfg.RatePerMinute, cfg.MaxConcurrent)
		quotaGate = quota.NewMemory()
	}

	// V1.4 cost governance gate: prices, budget policies, and the settlement
	// ledger live in PostgreSQL; the money counters share the limiter's mode
	// (Redis for multi-instance atomic subject+tenant enforcement, in-memory
	// for development). Budget enforcement follows GATEWAY_BUDGETS_ENABLED;
	// ledger capture is unconditional in database mode (the rollback point).
	if ledgerStore != nil {
		var budgets accounting.BudgetGate
		if rdb != nil {
			budgets = accounting.NewRedisBudget(rdb, "gw")
		} else {
			budgets = accounting.NewMemoryBudget()
		}
		accountingGate = &accounting.Gate{
			Store: ledgerStore, Budgets: budgets,
			Enforcement: cfg.BudgetsEnabled, Now: time.Now,
		}
	}

	reg := metrics.New()
	if auditSink == nil {
		auditSink = audit.NewMemorySink(logger)
	}
	// The unknown-cost settlement counter rides the accounting gate.
	if accountingGate != nil {
		accountingGate.Metrics = reg
	}

	// V1.4 data lifecycle: retention sweeps, archives, and exports behind the
	// admin API. GATEWAY_LIFECYCLE_ENABLED=false is the documented rollback
	// point (endpoints 404, maintenance command refuses to run). Retention is
	// inert until retention_policies rows exist, and scheduled sweeps belong
	// to cmd/maintain — the gateway only serves operator-triggered runs.
	var lifecycleAdmin *lifecycle.Admin
	if cfg.LifecycleEnabled && dbw != nil {
		lcStore := &pgstore.LifecycleStore{DB: dbw}
		admin, err := lifecycle.NewAdmin(lifecycle.AdminDeps{
			Store: lcStore, Export: lcStore,
			Sink:    lifecycle.NewFSArchiveSink(cfg.LifecycleArchiveDir),
			Metrics: reg, Logger: logger, Now: time.Now,
		})
		if err != nil {
			return fmt.Errorf("lifecycle wiring: %w", err)
		}
		lifecycleAdmin = admin
		logger.Info("lifecycle admin enabled", "archive_dir", cfg.LifecycleArchiveDir)
	}

	// Management service: database mode uses the PostgreSQL queries (the DB
	// satisfies mgmt.Service); development mode uses the in-memory service
	// over the catalog, route table, and audit sink.
	var mgmtSvc mgmt.Service
	if dbw != nil {
		mgmtSvc = dbw
	} else {
		mgmtSvc = mgmt.NewMemoryService(catalog, pol, svc.Routes, auditSink.(*audit.MemorySink), providerViews(providers))
	}

	// Runtime refresh boundary for the audited model enable/disable switch.
	// The mutation and its audit record already committed atomically by the
	// time this runs; it swaps the persisted row and route bindings into the
	// live catalog and route table so the decision affects subsequent model
	// resolution without a restart. Development mode needs no boundary: the
	// in-memory service flips the shared catalog directly.
	var applyModelChange func(ctx context.Context, publicModel string, enabled bool) error
	if dbw != nil {
		applyModelChange = func(ctx context.Context, publicModel string, enabled bool) error {
			info, found, err := dbw.ModelEntry(ctx, publicModel)
			if err != nil {
				return err
			}
			if found {
				catalog.SetEntry(info)
			} else {
				catalog.Remove(publicModel) // row vanished underneath us; converge
			}
			if !found {
				return nil
			}
			routeCfgs, err := dbw.LoadRoutes(ctx)
			if err != nil {
				return err
			}
			rs, err := routeSet(publicModel, routeCfgs, providers)
			if err != nil {
				return err
			}
			if rs == nil && info.Enabled {
				// No persisted bindings: mirror the single-candidate default
				// gateway.New derives for catalog entries.
				if p, ok := providers[info.Provider]; ok {
					rs = []router.Route{{
						ProviderName: info.Provider, Provider: p,
						UpstreamModel: info.UpstreamModel, Priority: 10, Enabled: true,
						Breaker: router.NewBreaker(5, 30*time.Second),
					}}
				}
			}
			if rs != nil {
				svc.Routes.SetRoutes(publicModel, rs)
			}
			return nil
		}
	}

	// Live provider breaker state for /admin/providers: stores cannot see
	// route state, so the process overlays it on the registry view. Enabled
	// providers without any live route in this process report breaker "none".
	providerRuntime := func(name string) (mgmt.ProviderRuntime, bool) {
		if s, ok := svc.Routes.ProviderBreakers()[name]; ok {
			return mgmt.ProviderRuntime{
				BreakerState: s.State, TotalRoutes: s.TotalRoutes, OpenRoutes: s.OpenRoutes,
			}, true
		}
		return mgmt.ProviderRuntime{}, true
	}

	// Runtime refresh boundary for the audited default-model switch: reload
	// the subject's persisted policy (limits and default-model slots) into
	// the live process after the mutation committed. Development mode needs
	// no boundary: the in-memory service mutates the shared policy directly.
	var applyPolicyChange func(ctx context.Context, subject string) error
	if dbw != nil {
		applyPolicyChange = func(ctx context.Context, subject string) error {
			limits, err := dbw.LoadLimits(ctx)
			if err != nil {
				return err
			}
			if l, ok := limits[subject]; ok {
				pol.SetLimits(subject, l)
			}
			return nil
		}
	}

	chat := &httpapi.ChatHandler{
		Auth: keyAuth, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Accounting: accountingGate, Audit: auditSink, Metrics: reg,
		MaxBody: cfg.MaxBodyBytes, MaxMsgs: cfg.MaxMessages, MaxChars: cfg.MaxMessageChars,
	}

	// V1.4 scoped admin identities: stored credentials in database mode, an
	// in-memory store in development mode. The legacy GATEWAY_ADMIN_TOKEN
	// rides the same authenticator as the explicit platform-admin bootstrap
	// identity; failed authentications are rate limited independently.
	adminAuth := &adminauth.Authenticator{
		Store:       adminCredStore(dbw),
		LegacyToken: cfg.AdminToken,
		Limiter:     adminauth.NewAuthLimiter(),
		Now:         time.Now,
	}

	// V1.4 background Responses jobs: opt-in (GATEWAY_ASYNC_ENABLED) and
	// database-mode only — PostgreSQL owns the job state machine, so the
	// in-memory development mode has no queue and background acceptance
	// answers with the stable 503 job_queue_unavailable (the documented
	// rollback posture: stop accepting, drain workers, retain rows/results).
	var asyncBundle *httpapi.Async
	var asyncJobsHandler http.Handler
	var asyncPool *async.Pool
	if cfg.AsyncEnabled && dbw != nil {
		jobs := &pgstore.AsyncStore{DB: dbw}
		// Queue depth/oldest age come from the scrape-time collector (one
		// sample per /metrics request), never per-job series.
		reg.RegisterQueueCollector(func(ctx context.Context) (int, time.Duration, error) {
			depth, err := jobs.QueueDepth(ctx)
			if err != nil {
				return 0, 0, err
			}
			age, err := jobs.QueueOldestAge(ctx)
			if err != nil {
				return 0, 0, err
			}
			return depth, age, nil
		})
		cancels := async.NewCancelRegistry()
		asyncPool = async.NewPool(async.PoolDeps{
			Store: jobs, Service: asyncSvc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
			Accounting: accountingGate,
			Audit:      auditSink, Metrics: reg, Logger: logger, Cancels: cancels,
			Encoder: httpapi.NewAsyncEncoder(), Now: time.Now,
		}, async.PoolConfig{
			WorkerID: workerID(), Count: cfg.AsyncWorkers, PollInterval: cfg.AsyncPollInterval,
			Lease: cfg.AsyncLease, JobTimeout: cfg.AsyncJobTimeout,
			MaxAttempts: cfg.AsyncMaxAttempts, ResultTTL: cfg.AsyncResultTTL,
			MaxResultBytes: cfg.AsyncMaxResultBytes, Drain: cfg.AsyncDrainTimeout,
		})
		asyncPool.Start(ctx)
		// Stop runs after the HTTP listeners shut down (defers run LIFO and
		// this is registered before dbw.Close): HTTP intake stops first, then
		// workers drain and hand back their leases.
		defer asyncPool.Stop(cfg.AsyncDrainTimeout)
		asyncBundle = &httpapi.Async{
			Jobs: jobs, Cancels: cancels, Wake: asyncPool.Wake,
			ResultTTL: cfg.AsyncResultTTL, KeyTTL: cfg.AsyncIdempotencyTTL,
			MaxResultBytes: cfg.AsyncMaxResultBytes, MaxKeyBytes: cfg.AsyncMaxKeyBytes,
		}
		asyncJobsHandler = &httpapi.AsyncJobsHandler{
			Auth: keyAuth, Jobs: jobs, Cancels: cancels, Audit: auditSink, Metrics: reg,
			AdminAuth: adminAuth,
			PollHint:  cfg.AsyncPollInterval, Now: time.Now,
		}
		logger.Info("async responses enabled", "workers", cfg.AsyncWorkers,
			"lease", cfg.AsyncLease.String(), "job_timeout", cfg.AsyncJobTimeout.String())
	}

	responses := &httpapi.ResponsesHandler{
		Auth: keyAuth, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Accounting: accountingGate, Audit: auditSink, Metrics: reg,
		MaxBody: cfg.MaxBodyBytes, MaxItems: cfg.MaxMessages * 2, MaxChars: cfg.MaxMessageChars,
		Async: asyncBundle,
	}
	embeddings := &httpapi.EmbeddingsHandler{
		Auth: keyAuth, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Accounting: accountingGate, Audit: auditSink, Metrics: reg,
		MaxBody: cfg.MaxBodyBytes, MaxItems: cfg.MaxMessages * 2, MaxChars: cfg.MaxMessageChars,
	}
	models := &httpapi.ModelsHandler{Auth: keyAuth, Service: svc, Policy: pol}

	// /readyz: named dependency checks with short independent deadlines. Each
	// check exists only while its feature is enabled (rollback: disabling a
	// feature removes its check), required failures withdraw readiness, and
	// the body names what degraded. Liveness (/healthz) stays process-only.
	readiness := httpapi.NewReadiness(logger)
	if dbw != nil {
		readiness.Register("database", true, dbw.Ready)
	}
	if cfg.RedisEnabled {
		// A configured multi-instance limiter must be reachable.
		readiness.Register("redis", true, func(ctx context.Context) error {
			return redisReady(ctx, cfg.RedisAddr)
		})
	}
	readiness.Register("catalog", true, func(context.Context) error {
		if catalog == nil || len(catalog.All()) == 0 {
			return errors.New("model catalog is empty")
		}
		return nil
	})
	if asyncBundle != nil {
		// Queue health: a real claim/query against the job table (the same
		// table the claimer scans), then the live worker heartbeat.
		readiness.Register("queue", true, func(ctx context.Context) error {
			if _, err := asyncBundle.Jobs.QueueDepth(ctx); err != nil {
				return err
			}
			return nil
		})
		readiness.Register("worker", true, func(context.Context) error {
			if asyncPool != nil && asyncPool.Healthy() {
				return nil
			}
			return errors.New("worker pool not accepting jobs or sweep heartbeat stale")
		})
	}
	if ledgerStore != nil {
		// Settlement health: reserved ledger rows older than the window above
		// the explicit threshold flip readiness (backlog, not a momentary
		// in-flight row). An unmeasurable backlog fails closed.
		readiness.Register("settlement", true, func(ctx context.Context) error {
			n, err := ledgerStore.ReservedBacklog(ctx, settlementBacklogWindow)
			if err != nil {
				return err
			}
			if n > cfg.SettlementBacklogMax {
				return fmt.Errorf("reserved ledger backlog %d exceeds threshold %d", n, cfg.SettlementBacklogMax)
			}
			return nil
		})
	}

	var responsesHandler http.Handler = responses
	if !cfg.ResponsesEnabled {
		responsesHandler = nil // documented rollback switch
	}
	var embeddingsHandler http.Handler = embeddings
	if !cfg.EmbeddingsEnabled {
		embeddingsHandler = nil // documented rollback switch
	}
	mux := httpapi.NewMux(chat, httpapi.Deps{
		Logger:          logger,
		Ready:           readiness,
		Metrics:         reg.Handler(),
		Responses:       responsesHandler,
		ResponsesGet:    asyncJobsHandler,
		ResponsesCancel: asyncJobsHandler,
		Embeddings:      embeddingsHandler,
		Models:          models,
	})

	// Bind both listeners synchronously so a port conflict is an ordinary
	// run() failure returned to main; listener goroutines never exit the
	// process. Serve receives the pre-bound listener, so a graceful Shutdown
	// can never race a not-yet-opened socket.
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", cfg.Addr, err)
	}
	defer ln.Close()

	var adminSrv *http.Server
	var adminLn net.Listener
	// Admin API: separate listener, internal network only. It starts when
	// either authentication path is available: the legacy bootstrap token or
	// stored admin credentials (database mode). During the migration both
	// paths stay live; removing GATEWAY_ADMIN_TOKEN once scoped credentials
	// are production-verified is the documented deprecation step.
	if cfg.AdminToken != "" || dbw != nil {
		adminLn, err = net.Listen("tcp", cfg.AdminAddr)
		if err != nil {
			return fmt.Errorf("admin listen %s: %w", cfg.AdminAddr, err)
		}
		defer func() { _ = adminLn.Close() }()
		adminSrv = &http.Server{
			Addr: cfg.AdminAddr, ReadHeaderTimeout: 10 * time.Second,
			Handler: httpapi.NewAdminMux(httpapi.AdminDeps{
				Manager: keyManager, Logger: logger, Token: cfg.AdminToken, Mgmt: mgmtSvc,
				AdminAuth:        adminAuth,
				AdminManager:     adminauth.NewManager(adminAuth.Store),
				ApplyModelChange: applyModelChange, ProviderRuntime: providerRuntime,
				ApplyPolicyChange: applyPolicyChange,
				Accounting:        ledgerStore, // nil in development mode
				Lifecycle:         lifecycleAdmin,
			}),
		}
	}

	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// runCtx covers the serving lifetime: it cancels when the process is
	// signalled (parent ctx) or when either listener fails, so both exit
	// paths converge on the same graceful-shutdown epilogue. serveErr carries
	// the first listener failure out to the return value.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	serveErr := make(chan error, 2)

	go func() {
		logger.Info("gateway listening", "addr", cfg.Addr, "provider", cfg.Provider,
			"persistence", cfg.DatabaseURL != "", "limits_mode", cfg.LimitsMode)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			serveErr <- err
			cancelRun()
		}
	}()

	// Admin API: separate listener, token-gated, internal network only.
	if adminSrv != nil {
		go func() {
			logger.Info("admin api listening", "addr", cfg.AdminAddr)
			if err := adminSrv.Serve(adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin server failed", "error", err)
				serveErr <- err
				cancelRun()
			}
		}()
	}

	<-runCtx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	if adminSrv != nil {
		if err := adminSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful admin shutdown failed", "error", err)
		}
	}
	// A listener failure cancelled runCtx; surface it so main exits non-zero.
	// The goroutine sends to the buffered channel before cancelling, so the
	// value is visible here.
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	default:
	}
	return nil
}

// adminCredStore selects the admin credential persistence: PostgreSQL in
// database mode, in-memory in development mode (development issues nothing
// unless an operator calls /admin/admins with the bootstrap token).
func adminCredStore(dbw *pgstore.DB) adminauth.Store {
	if dbw != nil {
		return pgstore.AdminCredentialStore{DB: dbw}
	}
	return adminauth.NewMemoryStore()
}

// routeSet builds the router routes for one public model from the persisted
// route bindings, and returns nil when the model has none (the caller then
// keeps or rebuilds the single-candidate default). Both startup and the
// management runtime-refresh use it so the two paths cannot drift.
func routeSet(publicModel string, cfgs []pgstore.RouteConfig, providers map[string]provider.Provider) ([]router.Route, error) {
	var rs []router.Route
	for _, rc := range cfgs {
		if rc.PublicModel != publicModel {
			continue
		}
		p, ok := providers[rc.ProviderName]
		if !ok {
			return nil, fmt.Errorf("model %s: route references unknown provider %q", publicModel, rc.ProviderName)
		}
		rs = append(rs, router.Route{
			ProviderName: rc.ProviderName, Provider: p,
			UpstreamModel: rc.UpstreamModel, Priority: rc.Priority,
			Timeout: time.Duration(rc.TimeoutMillis) * time.Millisecond,
			Enabled: rc.Enabled, Breaker: router.NewBreaker(5, 30*time.Second),
		})
	}
	return rs, nil
}

// newProviderFromRegistry builds one provider from a registry entry: kind and
// base URL come from the database (or, in local mode, the legacy
// GATEWAY_PROVIDER settings), while the credential comes from the process
// environment. Every enabled provider is validated before the gateway serves
// traffic; errors name the missing configuration kind and never include the
// secret value or the database DSN.
func newProviderFromRegistry(cfg config.Config, kind, name, baseURL string) (provider.Provider, error) {
	// The fake provider never dials its base URL — seeded registries use
	// internal:// pseudo URLs for it — so SSRF validation applies only to
	// network-backed kinds.
	if kind != "fake" {
		if err := provider.ValidateBaseURL(baseURL, cfg.AllowInsecure); err != nil {
			return nil, fmt.Errorf("provider %s: %w", name, err)
		}
	}
	switch kind {
	case "openai":
		key, err := resolveCredential(kind, name, cfg.OpenAIKey)
		if err != nil {
			return nil, err
		}
		return provider.NewOpenAI(baseURL, key), nil
	case "anthropic":
		key, err := resolveCredential(kind, name, cfg.AnthropicKey)
		if err != nil {
			return nil, err
		}
		return provider.NewAnthropic(baseURL, key), nil
	case "fake":
		return provider.Fake{}, nil
	default:
		return nil, fmt.Errorf("provider %s: unsupported kind %q", name, kind)
	}
}

// credentialEnvName derives the per-provider credential environment variable
// for one registry row: <KIND>_API_KEY__<PROVIDER_NAME>. The provider name is
// uppercased with every non-alphanumeric rune mapped to '_', so registry
// names like "openai-embed" or "eu.chat" become valid variable names
// (OPENAI_API_KEY__OPENAI_EMBED, OPENAI_API_KEY__EU_CHAT). The mapping is
// ASCII-only on purpose: environment variable names are ASCII on every
// supported platform.
func credentialEnvName(kind, name string) string {
	sanitize := func(s string) string {
		var b strings.Builder
		b.Grow(len(s))
		for _, r := range s {
			switch {
			case r >= 'a' && r <= 'z':
				b.WriteRune(r - 'a' + 'A')
			case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
				b.WriteRune(r)
			default:
				b.WriteByte('_')
			}
		}
		return b.String()
	}
	return sanitize(kind) + "_API_KEY__" + sanitize(name)
}

// resolveCredential returns the API key for one provider registry row. The
// per-provider variable <KIND>_API_KEY__<PROVIDER_NAME> wins when set;
// otherwise the kind-level credential loaded by internal/config
// (<KIND>_API_KEY) applies, which keeps development mode zero-config
// (GATEWAY_PROVIDER=openai uses the literal name "openai" and resolves
// through the fallback). When neither is present the error names both
// checked variables — never any value.
func resolveCredential(kind, name, kindKey string) (string, error) {
	perProvider := credentialEnvName(kind, name)
	if v := os.Getenv(perProvider); v != "" {
		return v, nil
	}
	if kindKey != "" {
		return kindKey, nil
	}
	return "", fmt.Errorf("provider %s: %s or %s_API_KEY must be set for %s kind",
		name, perProvider, strings.ToUpper(kind), kind)
}

// providerViews snapshots the provider registry for the dev-mode management
// service. Endpoints and credentials are never included.
func providerViews(providers map[string]provider.Provider) []mgmt.ProviderView {
	out := make([]mgmt.ProviderView, 0, len(providers))
	for name := range providers {
		out = append(out, mgmt.ProviderView{Name: name, Kind: providers[name].Name(), Enabled: true})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// redisReady pings Redis for readiness checks.
func redisReady(ctx context.Context, addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	return rdb.Ping(ctx).Err()
}

// workerID names this process's async worker instance: host, pid, and start
// time make it unique across restarts, which is what lease ownership keys on.
func workerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("worker-%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}

// pgAudit adapts the store writer to the audit.Sink signature.
type pgAudit struct{ db *pgstore.DB }

// settlementBacklogWindow: only reserved ledger rows older than this count
// toward the readiness settlement backlog — fresh reserved rows are normal
// in-flight settlements (the detached finalize path can take seconds).
const settlementBacklogWindow = 10 * time.Minute

func (a pgAudit) Write(e audit.Event) {
	_ = a.db.WriteAudit(context.Background(), e)
}
