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

	Provider  string // "openai" or "fake"
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
		OpenAIKey:       os.Getenv("OPENAI_API_KEY"),
		OpenAIURL:       env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
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
	// Dev-only convenience; production must load keys from the database store.
	for _, part := range strings.Split(os.Getenv("GATEWAY_API_KEYS"), ",") {
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

	// Models: GATEWAY_MODELS="<public>:<provider>:<upstream>[,<public>:...]"
	for _, part := range strings.Split(os.Getenv("GATEWAY_MODELS"), ",") {
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

	if c.Provider == "openai" && c.OpenAIKey == "" {
		return c, fmt.Errorf("OPENAI_API_KEY must be set when GATEWAY_PROVIDER=openai")
	}
	switch c.Provider {
	case "openai":
		if c.OpenAIKey == "" {
			return c, fmt.Errorf("OPENAI_API_KEY must be set when GATEWAY_PROVIDER=openai")
		}
	case "anthropic":
		if c.AnthropicKey == "" {
			return c, fmt.Errorf("ANTHROPIC_API_KEY must be set when GATEWAY_PROVIDER=anthropic")
		}
	case "fake":
	default:
		return c, fmt.Errorf("GATEWAY_PROVIDER: unsupported provider %q", c.Provider)
	}

	if v := os.Getenv("GATEWAY_DATABASE_URL"); v != "" {
		c.DatabaseURL = v
	}
	c.AdminToken = os.Getenv("GATEWAY_ADMIN_TOKEN")
	c.AdminAddr = env("GATEWAY_ADMIN_ADDR", ":8081")
	c.AnthropicKey = os.Getenv("ANTHROPIC_API_KEY")
	c.AnthropicURL = env("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
	c.AllowInsecure = os.Getenv("GATEWAY_ALLOW_INSECURE_BASE_URLS") == "true"

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
