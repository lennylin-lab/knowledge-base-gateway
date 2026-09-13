// Command gateway runs the LLM gateway process.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("configuration invalid", "error", err)
		os.Exit(1)
	}

	// Wire auth store from configured dev keys (plaintext hashed at load).
	keyStore := auth.NewStore()
	for _, k := range cfg.Keys {
		salt, err := auth.NewSalt()
		if err != nil {
			logger.Error("generate salt", "error", err)
			os.Exit(1)
		}
		keyStore.Put(auth.KeyRecord{
			ID: k.ID, Subject: k.Subject, Salt: salt,
			Hash: auth.HashAPIKey(salt, k.PlaintextKey), Status: auth.StatusActive,
		})
	}

	var entries []policy.ModelInfo
	subjects := map[string]struct{}{}
	for _, k := range cfg.Keys {
		subjects[k.Subject] = struct{}{}
	}
	for _, m := range cfg.Models {
		entries = append(entries, policy.ModelInfo{
			PublicName: m.PublicName, Provider: m.Provider, UpstreamModel: m.UpstreamModel, Enabled: m.Enabled,
		})
	}
	catalog := policy.NewCatalog(entries)
	pol := policy.New()
	for s := range subjects {
		pol.AllowAll(s)
	}

	providers := map[string]provider.Provider{}
	switch cfg.Provider {
	case "openai":
		providers["openai"] = provider.NewOpenAI(cfg.OpenAIURL, cfg.OpenAIKey)
	default:
		providers["fake"] = provider.Fake{}
	}

	svc := gateway.New(catalog, providers, cfg.RequestTimeout, cfg.MaxRetries)
	lim := limiter.New(cfg.RatePerMinute, cfg.MaxConcurrent)
	reg := metrics.New()
	sink := audit.NewMemorySink(logger)

	chat := &httpapi.ChatHandler{
		Auth: keyStore, Service: svc, Policy: pol, Limiter: lim, Audit: sink, Metrics: reg,
		MaxBody: cfg.MaxBodyBytes, MaxMsgs: cfg.MaxMessages, MaxChars: cfg.MaxMessageChars,
	}
	mux := httpapi.NewMux(chat, httpapi.Deps{
		Logger:  logger,
		ReadyFn: func() bool { return len(cfg.Models) > 0 && len(cfg.Keys) > 0 },
		Metrics: reg.Handler(),
	})

	srv := &http.Server{
		Addr: cfg.Addr, Handler: mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("gateway listening", "addr", cfg.Addr, "provider", cfg.Provider)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}
