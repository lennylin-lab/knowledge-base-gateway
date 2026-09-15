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
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/dberr"
	"github.com/knowledge-base/knowledge-base-gateway/internal/envfile"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
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

	// Persisted primary/backup routes replace the single-candidate defaults.
	if dbw != nil {
		routeCfgs, err := dbw.LoadRoutes(ctx)
		if err != nil {
			return fmt.Errorf("load routes: %w", err)
		}
		byModel := map[string][]router.Route{}
		for _, rc := range routeCfgs {
			p, ok := providers[rc.ProviderName]
			if !ok {
				return fmt.Errorf("model %s: route references unknown provider %q", rc.PublicModel, rc.ProviderName)
			}
			byModel[rc.PublicModel] = append(byModel[rc.PublicModel], router.Route{
				ProviderName: rc.ProviderName, Provider: p,
				UpstreamModel: rc.UpstreamModel, Priority: rc.Priority,
				Timeout: time.Duration(rc.TimeoutMillis) * time.Millisecond,
				Enabled: rc.Enabled, Breaker: router.NewBreaker(5, 30*time.Second),
			})
		}
		for m, rs := range byModel {
			svc.Routes.SetRoutes(m, rs)
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
	if cfg.RedisEnabled {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
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

	reg := metrics.New()
	if auditSink == nil {
		auditSink = audit.NewMemorySink(logger)
	}

	chat := &httpapi.ChatHandler{
		Auth: keyAuth, Service: svc, Policy: pol, Limiter: rateLimiter, Quota: quotaGate,
		Audit: auditSink, Metrics: reg,
		MaxBody: cfg.MaxBodyBytes, MaxMsgs: cfg.MaxMessages, MaxChars: cfg.MaxMessageChars,
	}

	ready := func() bool {
		if dbw != nil {
			if err := dbw.Ready(ctx); err != nil {
				logger.Warn("readiness: database unavailable", "error", err)
				return false
			}
		}
		if cfg.RedisEnabled {
			// A configured multi-instance limiter must be reachable.
			c, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := redisReady(c, cfg.RedisAddr); err != nil {
				logger.Warn("readiness: redis unavailable", "error", err)
				return false
			}
		}
		return catalog != nil && len(catalog.All()) > 0
	}

	mux := httpapi.NewMux(chat, httpapi.Deps{
		Logger:  logger,
		ReadyFn: ready,
		Metrics: reg.Handler(),
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
	if cfg.AdminToken != "" {
		adminLn, err = net.Listen("tcp", cfg.AdminAddr)
		if err != nil {
			return fmt.Errorf("admin listen %s: %w", cfg.AdminAddr, err)
		}
		defer func() { _ = adminLn.Close() }()
		adminSrv = &http.Server{
			Addr: cfg.AdminAddr, ReadHeaderTimeout: 10 * time.Second,
			Handler: httpapi.NewAdminMux(httpapi.AdminDeps{Manager: keyManager, Logger: logger, Token: cfg.AdminToken}),
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
		if cfg.OpenAIKey == "" {
			return nil, fmt.Errorf("provider %s: OPENAI_API_KEY must be set for openai kind", name)
		}
		return provider.NewOpenAI(baseURL, cfg.OpenAIKey), nil
	case "anthropic":
		if cfg.AnthropicKey == "" {
			return nil, fmt.Errorf("provider %s: ANTHROPIC_API_KEY must be set for anthropic kind", name)
		}
		return provider.NewAnthropic(baseURL, cfg.AnthropicKey), nil
	case "fake":
		return provider.Fake{}, nil
	default:
		return nil, fmt.Errorf("provider %s: unsupported kind %q", name, kind)
	}
}

// redisReady pings Redis for readiness checks.
func redisReady(ctx context.Context, addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	return rdb.Ping(ctx).Err()
}

// pgAudit adapts the store writer to the audit.Sink signature.
type pgAudit struct{ db *pgstore.DB }

func (a pgAudit) Write(e audit.Event) {
	_ = a.db.WriteAudit(context.Background(), e)
}
