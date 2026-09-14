// Package config loads and validates gateway configuration from the
// environment. Provider secrets are read only here and never persisted.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// KeyEntry describes one configured API key for development mode.
// Format: "<key-id>:<subject>:<plaintext-key>" (comma separated in GATEWAY_API_KEYS).
type KeyEntry struct {
	ID           string
	Subject      string
	PlaintextKey string
}

// ModelEntry maps a public model name to a provider and upstream model.
type ModelEntry struct {
	PublicName    string
	Provider      string
	UpstreamModel string
	Enabled       bool
}

// Config is the validated process configuration.
type Config struct {
	Addr            string
	RequestTimeout  time.Duration
	MaxBodyBytes    int64
	MaxMessages     int
	MaxMessageChars int
	MaxRetries      int
	RatePerMinute   int
	MaxConcurrent   int

	Provider  string // "openai", "anthropic", or "fake"
	OpenAIKey string
	OpenAIURL string

	Keys   []KeyEntry
	Models []ModelEntry

	// V1.1 production settings. All optional; the gateway runs in
	// development mode (in-memory stores, local limiter) when unset.
	DatabaseURL   string // GATEWAY_DATABASE_URL enables PostgreSQL persistence
	RedisAddr     string // GATEWAY_REDIS_ADDR enables the distributed limiter
	RedisEnabled  bool
	LimitsMode    string // "local" (default, dev-only) or "redis"
	AdminToken    string // GATEWAY_ADMIN_TOKEN; enables the admin API
	AdminAddr     string // defaults :8081
	AllowInsecure bool   // GATEWAY_ALLOW_INSECURE_BASE_URLS (dev only)
	AnthropicKey  string
	AnthropicURL  string
}

// FromEnv builds a Config from environment variables and validates it.
//
// GATEWAY_DATABASE_URL is the configuration-mode boundary. When it is set,
// the database is authoritative for keys, catalog, routes, policies, and the
// provider registry, so the development-only GATEWAY_API_KEYS and
// GATEWAY_MODELS lists are optional (a supplied value is still parsed so a
// typo fails startup instead of being silently ignored). When it is unset,
// local development mode requires both. Provider secrets are loaded before
// any validation so every provider kind is checked against its own
// credential.
func FromEnv() (Config, error) {
	c := Config{
		Addr:            env("GATEWAY_ADDR", ":8080"),
		RequestTimeout:  60 * time.Second,
		MaxBodyBytes:    1 << 20,
		MaxMessages:     64,
		MaxMessageChars: 32_000,
		MaxRetries:      2,
		RatePerMinute:   120,
		MaxConcurrent:   8,
		Provider:        env("GATEWAY_PROVIDER", "fake"),

		// Provider secrets and endpoints are loaded before any validation
		// runs, so the switch below always sees the credential of the kind
		// it is checking.
		OpenAIKey:    os.Getenv("OPENAI_API_KEY"),
		OpenAIURL:    env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		AnthropicKey: os.Getenv("ANTHROPIC_API_KEY"),
		AnthropicURL: env("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),

		// Configuration-mode boundary and admin/SSRF settings.
		DatabaseURL:   os.Getenv("GATEWAY_DATABASE_URL"),
		AdminToken:    os.Getenv("GATEWAY_ADMIN_TOKEN"),
		AdminAddr:     env("GATEWAY_ADMIN_ADDR", ":8081"),
		AllowInsecure: os.Getenv("GATEWAY_ALLOW_INSECURE_BASE_URLS") == "true",
	}
	if v := os.Getenv("GATEWAY_MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 10 {
			return c, fmt.Errorf("GATEWAY_MAX_RETRIES: want integer 0..10, got %q", v)
		}
		c.MaxRetries = n
	}
	if v := os.Getenv("GATEWAY_RATE_PER_MINUTE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("GATEWAY_RATE_PER_MINUTE: want positive integer, got %q", v)
		}
		c.RatePerMinute = n
	}

	// Keys: GATEWAY_API_KEYS="id1:subject1:secret1,id2:subject2:secret2"
	// Dev-only convenience; in database mode the PostgreSQL store is
	// authoritative, but an explicitly supplied value is still parsed and
	// validated.
	if raw := os.Getenv("GATEWAY_API_KEYS"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 3)
			if len(fields) != 3 {
				return c, fmt.Errorf("GATEWAY_API_KEYS entry %q must be <id>:<subject>:<key>", part)
			}
			c.Keys = append(c.Keys, KeyEntry{ID: fields[0], Subject: fields[1], PlaintextKey: fields[2]})
		}
		if len(c.Keys) == 0 {
			return c, fmt.Errorf("GATEWAY_API_KEYS: at least one key is required")
		}
	}
	if c.DatabaseURL == "" && len(c.Keys) == 0 {
		return c, fmt.Errorf("GATEWAY_API_KEYS: at least one key is required")
	}

	// Models: GATEWAY_MODELS="<public>:<provider>:<upstream>[,<public>:...]"
	if raw := os.Getenv("GATEWAY_MODELS"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 3)
			if len(fields) != 3 {
				return c, fmt.Errorf("GATEWAY_MODELS entry %q must be <public-name>:<provider>:<upstream-model>", part)
			}
			c.Models = append(c.Models, ModelEntry{PublicName: fields[0], Provider: fields[1], UpstreamModel: fields[2], Enabled: true})
		}
		if len(c.Models) == 0 {
			return c, fmt.Errorf("GATEWAY_MODELS: at least one model is required")
		}
	}
	if c.DatabaseURL == "" && len(c.Models) == 0 {
		return c, fmt.Errorf("GATEWAY_MODELS: at least one model is required")
	}

	// GATEWAY_PROVIDER stays authoritative for local development mode; its
	// value is validated in both modes so a typo fails fast. The credential
	// requirement is local-mode-only: in database mode the persisted
	// provider registry is authoritative and every enabled row is
	// credential-checked at startup (cmd/gateway), so the legacy selector
	// must not demand secrets the registry does not need.
	switch c.Provider {
	case "openai":
		if c.DatabaseURL == "" && c.OpenAIKey == "" {
			return c, fmt.Errorf("OPENAI_API_KEY must be set when GATEWAY_PROVIDER=openai")
		}
	case "anthropic":
		if c.DatabaseURL == "" && c.AnthropicKey == "" {
			return c, fmt.Errorf("ANTHROPIC_API_KEY must be set when GATEWAY_PROVIDER=anthropic")
		}
	case "fake":
	default:
		return c, fmt.Errorf("GATEWAY_PROVIDER: unsupported provider %q", c.Provider)
	}

	c.LimitsMode = env("GATEWAY_LIMITS_MODE", "local")
	switch c.LimitsMode {
	case "redis":
		c.RedisAddr = env("GATEWAY_REDIS_ADDR", "127.0.0.1:6379")
		c.RedisEnabled = true
	case "local":
	default:
		return c, fmt.Errorf("GATEWAY_LIMITS_MODE: want \"local\" or \"redis\", got %q", c.LimitsMode)
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
