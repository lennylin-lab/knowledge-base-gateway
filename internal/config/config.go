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

// DefaultModelEntry names a subject's default models for local development
// mode. Format in GATEWAY_DEFAULT_MODELS (comma separated):
// "<subject>:<chat-model>[:<embedding-model>]". The embedding segment is
// optional; a single-segment entry sets only the chat default.
type DefaultModelEntry struct {
	Subject        string
	ChatModel      string
	EmbeddingModel string
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

	// V1.2 developer platform. ResponsesEnabled is the independent endpoint
	// rollback switch; per-model gates live in the catalog capability matrix.
	// V1.3 adds the same switch for the embeddings endpoint.
	ResponsesEnabled  bool
	EmbeddingsEnabled bool

	// V1.4 background Responses jobs. AsyncEnabled gates acceptance of
	// background:true (default off: the documented gradual rollout); jobs
	// additionally require database mode (PostgreSQL owns the state
	// machine). The remaining knobs bound the worker pool, leases, retries,
	// and result retention; all values are validated positive.
	AsyncEnabled        bool
	AsyncWorkers        int
	AsyncPollInterval   time.Duration
	AsyncLease          time.Duration
	AsyncJobTimeout     time.Duration
	AsyncMaxAttempts    int
	AsyncResultTTL      time.Duration
	AsyncMaxResultBytes int
	AsyncIdempotencyTTL time.Duration
	AsyncDrainTimeout   time.Duration
	AsyncMaxKeyBytes    int

	// DefaultModels carries the local-development default-model assignments
	// (GATEWAY_DEFAULT_MODELS). In database mode the access_policies columns
	// are authoritative and this list is ignored.
	DefaultModels []DefaultModelEntry
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

	// V1.2 Responses endpoint: enabled by default; set to "false" to disable
	// the endpoint independently of Chat Completions (documented rollback
	// switch). Per-model gates live in the catalog capability matrix.
	c.ResponsesEnabled = os.Getenv("GATEWAY_RESPONSES_ENABLED") != "false"

	// V1.3 Embeddings endpoint: same default-enabled, independently togglable
	// rollback switch. Per-model gates live in the catalog capability matrix
	// (embeddings + embedding_dim).
	c.EmbeddingsEnabled = os.Getenv("GATEWAY_EMBEDDINGS_ENABLED") != "false"

	// V1.4 background Responses jobs: opt-in (gradual rollout by model and
	// tenant), database-mode only. Every lifecycle knob has a bounded,
	// validated default; a supplied value must be positive.
	c.AsyncEnabled = os.Getenv("GATEWAY_ASYNC_ENABLED") == "true"
	c.AsyncWorkers = 2
	c.AsyncPollInterval = time.Second
	c.AsyncLease = 60 * time.Second
	c.AsyncJobTimeout = 10 * time.Minute
	c.AsyncMaxAttempts = 3
	c.AsyncResultTTL = 24 * time.Hour
	c.AsyncMaxResultBytes = 1 << 20
	c.AsyncIdempotencyTTL = 24 * time.Hour
	c.AsyncDrainTimeout = 10 * time.Second
	c.AsyncMaxKeyBytes = 256
	if v := os.Getenv("GATEWAY_ASYNC_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 64 {
			return c, fmt.Errorf("GATEWAY_ASYNC_WORKERS: want integer 1..64, got %q", v)
		}
		c.AsyncWorkers = n
	}
	for _, spec := range []struct {
		name string
		set  func(d time.Duration)
	}{
		{"GATEWAY_ASYNC_POLL_INTERVAL", func(d time.Duration) { c.AsyncPollInterval = d }},
		{"GATEWAY_ASYNC_LEASE", func(d time.Duration) { c.AsyncLease = d }},
		{"GATEWAY_ASYNC_JOB_TIMEOUT", func(d time.Duration) { c.AsyncJobTimeout = d }},
		{"GATEWAY_ASYNC_RESULT_TTL", func(d time.Duration) { c.AsyncResultTTL = d }},
		{"GATEWAY_ASYNC_IDEMPOTENCY_TTL", func(d time.Duration) { c.AsyncIdempotencyTTL = d }},
		{"GATEWAY_ASYNC_DRAIN_TIMEOUT", func(d time.Duration) { c.AsyncDrainTimeout = d }},
	} {
		if v := os.Getenv(spec.name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("%s: want positive duration, got %q", spec.name, v)
			}
			spec.set(d)
		}
	}
	if v := os.Getenv("GATEWAY_ASYNC_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 100 {
			return c, fmt.Errorf("GATEWAY_ASYNC_MAX_ATTEMPTS: want integer 1..100, got %q", v)
		}
		c.AsyncMaxAttempts = n
	}
	if v := os.Getenv("GATEWAY_ASYNC_MAX_RESULT_BYTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1024 {
			return c, fmt.Errorf("GATEWAY_ASYNC_MAX_RESULT_BYTES: want integer >= 1024, got %q", v)
		}
		c.AsyncMaxResultBytes = n
	}

	// GATEWAY_DEFAULT_MODELS="subject:chat-model[:embedding-model],..."
	// Development-mode default-model assignments; in database mode the
	// access_policies default columns are authoritative. A supplied value is
	// parsed and validated in both modes so a typo fails startup.
	if raw := os.Getenv("GATEWAY_DEFAULT_MODELS"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 3)
			switch len(fields) {
			case 2:
				c.DefaultModels = append(c.DefaultModels, DefaultModelEntry{Subject: fields[0], ChatModel: fields[1]})
			case 3:
				c.DefaultModels = append(c.DefaultModels, DefaultModelEntry{Subject: fields[0], ChatModel: fields[1], EmbeddingModel: fields[2]})
			default:
				return c, fmt.Errorf("GATEWAY_DEFAULT_MODELS entry %q must be <subject>:<chat-model>[:<embedding-model>]", part)
			}
			e := c.DefaultModels[len(c.DefaultModels)-1]
			if e.Subject == "" || e.ChatModel == "" {
				return c, fmt.Errorf("GATEWAY_DEFAULT_MODELS entry %q must name a subject and a chat model", part)
			}
		}
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
