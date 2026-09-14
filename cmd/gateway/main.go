// Command gateway runs the LLM gateway process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("configuration invalid", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Optional PostgreSQL persistence. When configured, catalog, routes,
	// policies, keys, and audit records live in the database.
	var (
		dbw            *pgstore.DB
		authenticator  httpapi.Authenticator
		lifecycleStore auth.MutationStore
		auditSink      audit.Sink
	)
	if cfg.DatabaseURL != "" {
		dbw, err = pgstore.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			logger.Error("database connect failed", "error", err)
			os.Exit(1)
		}
		defer dbw.Close()
		authenticator = &pgstore.Authenticator{DB: dbw, Now: time.Now}
		lifecycleStore = dbw
		auditSink = pgAudit{db: dbw}
	}

	// Provider registry. Secrets come only from the environment; base URLs
	// are validated against SSRF rules at startup.
	newProvider := func(kind, name, baseURL string) (provider.Provider, error) {
		if err := provider.ValidateBaseURL(baseURL, cfg.AllowInsecure); err != nil {
			return nil, fmt.Errorf("provider %s: %w", name, err)
		}
		switch kind {
		case "openai":
			return provider.NewOpenAI(baseURL, cfg.OpenAIKey), nil
		case "anthropic":
			return provider.NewAnthropic(baseURL, cfg.AnthropicKey), nil
		case "fake":
			return provider.Fake{}, nil
		default:
			return nil, fmt.Errorf("provider %s: unsupported kind %q", name, kind)
		}
	}

	providers := map[string]provider.Provider{}
	var catalog *policy.Catalog
	if dbw != nil {
		pcfgs, err := dbw.LoadProviders(ctx)
		if err != nil {
			logger.Error("load providers", "error", err)
			os.Exit(1)
		}
		for _, pc := range pcfgs {
			p, err := newProvider(pc.Kind, pc.Name, pc.BaseURL)
			if err != nil {
				logger.Error("provider configuration invalid", "error", err)
				os.Exit(1)
			}
			providers[pc.Name] = p
		}
		entries, err := dbw.LoadCatalog(ctx)
		if err != nil {
			logger.Error("load catalog", "error", err)
			os.Exit(1)
		}
		catalog = policy.NewCatalog(entries)
	} else {
		switch cfg.Provider {
		case "openai":
			providers["openai"] = provider.NewOpenAI(cfg.OpenAIURL, cfg.OpenAIKey)
		case "anthropic":
			providers["anthropic"] = provider.NewAnthropic(cfg.AnthropicURL, cfg.AnthropicKey)
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
			logger.Error("load routes", "error", err)
			os.Exit(1)
		}
		byModel := map[string][]router.Route{}
		for _, rc := range routeCfgs {
			p, ok := providers[rc.ProviderName]
			if !ok {
				logger.Error("route references unknown provider", "provider", rc.ProviderName)
				os.Exit(1)
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
	// mode loads explicit model grants and limits.
	pol := policy.New()
	if dbw != nil {
		limits, err := dbw.LoadLimits(ctx)
		if err != nil {
			logger.Error("load policies", "error", err)
			os.Exit(1)
		}
		for subject, l := range limits {
			pol.SetLimits(subject, l)
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
				logger.Error("generate salt", "error", err)
				os.Exit(1)
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
	// readiness before traffic is served.
	var rateLimiter limiter.Gate
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
		defer rdb.Close()
	} else {
		rateLimiter = limiter.New(cfg.RatePerMinute, cfg.MaxConcurrent)
	}

	reg := metrics.New()
	if auditSink == nil {
		auditSink = audit.NewMemorySink(logger)
	}

	chat := &httpapi.ChatHandler{
		Auth: keyAuth, Service: svc, Policy: pol, Limiter: rateLimiter, Audit: auditSink, Metrics: reg,
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

	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		logger.Info("gateway listening", "addr", cfg.Addr, "provider", cfg.Provider,
			"persistence", cfg.DatabaseURL != "", "limits_mode", cfg.LimitsMode)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Admin API: separate listener, token-gated, internal network only.
	if cfg.AdminToken != "" {
		adminSrv := &http.Server{
			Addr: cfg.AdminAddr, ReadHeaderTimeout: 10 * time.Second,
			Handler: httpapi.NewAdminMux(httpapi.AdminDeps{Manager: keyManager, Logger: logger, Token: cfg.AdminToken}),
		}
		go func() {
			logger.Info("admin api listening", "addr", cfg.AdminAddr)
			if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin server failed", "error", err)
			}
		}()
		defer adminSrv.Close()
	}

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
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
