package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// configEnvVars lists every environment variable FromEnv reads so tests stay
// hermetic even when the developer shell exports production settings.
var configEnvVars = []string{
	"GATEWAY_ADDR", "GATEWAY_PROVIDER",
	"OPENAI_API_KEY", "OPENAI_BASE_URL",
	"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL",
	"GATEWAY_API_KEYS", "GATEWAY_MODELS",
	"GATEWAY_MAX_RETRIES", "GATEWAY_RATE_PER_MINUTE",
	"GATEWAY_STREAM_STALL_TIMEOUT", "GATEWAY_STREAM_TOTAL_TIMEOUT", "GATEWAY_STREAM_MAX_RETRIES",
	"GATEWAY_DATABASE_URL", "GATEWAY_LIMITS_MODE", "GATEWAY_REDIS_ADDR",
	"GATEWAY_ADMIN_TOKEN", "GATEWAY_ADMIN_ADDR",
	"GATEWAY_ALLOW_INSECURE_BASE_URLS", "GATEWAY_RESPONSES_ENABLED",
	"GATEWAY_EMBEDDINGS_ENABLED", "GATEWAY_DEFAULT_MODELS",
	"GATEWAY_ASYNC_ENABLED", "GATEWAY_ASYNC_WORKERS", "GATEWAY_ASYNC_POLL_INTERVAL",
	"GATEWAY_ASYNC_LEASE", "GATEWAY_ASYNC_JOB_TIMEOUT", "GATEWAY_ASYNC_MAX_ATTEMPTS",
	"GATEWAY_ASYNC_RESULT_TTL", "GATEWAY_ASYNC_IDEMPOTENCY_TTL",
	"GATEWAY_ASYNC_MAX_RESULT_BYTES", "GATEWAY_ASYNC_DRAIN_TIMEOUT",
	"GATEWAY_BUDGETS_ENABLED", "GATEWAY_LIFECYCLE_ENABLED", "GATEWAY_LIFECYCLE_ARCHIVE_DIR",
	"GATEWAY_OTLP_ENABLED", "GATEWAY_OTLP_ENDPOINT", "GATEWAY_OTLP_INSECURE",
	"GATEWAY_OTLP_SAMPLING_RATIO", "GATEWAY_OTLP_TIMEOUT", "GATEWAY_SETTLEMENT_BACKLOG_MAX",
}

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for _, k := range configEnvVars {
		os.Unsetenv(k)
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestFromEnvValid(t *testing.T) {
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS": "key-1:tenant-a:sk-abc,key-2:tenant-b:sk-def",
		"GATEWAY_MODELS":   "gpt-a:openai:gpt-a-upstream,gpt-b:fake:gpt-b",
		"GATEWAY_PROVIDER": "fake",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Keys) != 2 || cfg.Keys[0].Subject != "tenant-a" {
		t.Errorf("keys = %+v", cfg.Keys)
	}
	if len(cfg.Models) != 2 || cfg.Models[0].UpstreamModel != "gpt-a-upstream" {
		t.Errorf("models = %+v", cfg.Models)
	}
	if cfg.RequestTimeout != 60*time.Second {
		t.Errorf("default timeout = %v", cfg.RequestTimeout)
	}
	if cfg.StreamStallTimeout != 30*time.Second {
		t.Errorf("default StreamStallTimeout = %v, want 30s", cfg.StreamStallTimeout)
	}
	if cfg.StreamTotalTimeout != 10*time.Minute {
		t.Errorf("default StreamTotalTimeout = %v, want 10m", cfg.StreamTotalTimeout)
	}
	if cfg.StreamMaxRetries != 10 {
		t.Errorf("default StreamMaxRetries = %d, want 10", cfg.StreamMaxRetries)
	}
	if cfg.MaxConcurrent != 8 {
		t.Errorf("default MaxConcurrent = %d, want 8", cfg.MaxConcurrent)
	}
	if !cfg.ResponsesEnabled {
		t.Error("ResponsesEnabled must default to true; GATEWAY_RESPONSES_ENABLED=false is the rollback switch")
	}
	if !cfg.EmbeddingsEnabled {
		t.Error("EmbeddingsEnabled must default to true; GATEWAY_EMBEDDINGS_ENABLED=false is the rollback switch")
	}
	// V1.4 observability: OTLP export defaults off (the independent kill
	// switch) but every knob that must not be zero when enabled has a real
	// default; the settlement backlog threshold must never deny-by-default.
	if cfg.OTLPEnabled {
		t.Error("OTLPEnabled must default to false; GATEWAY_OTLP_ENABLED=true is the opt-in")
	}
	if cfg.OTLPEndpoint == "" {
		t.Error("OTLPEndpoint must have a non-empty default")
	}
	if cfg.OTLPRatio <= 0 || cfg.OTLPRatio > 1 {
		t.Errorf("OTLPRatio = %v, want in (0,1]", cfg.OTLPRatio)
	}
	if cfg.OTLPTimeout <= 0 {
		t.Errorf("OTLPTimeout = %v, want positive", cfg.OTLPTimeout)
	}
	if cfg.SettlementBacklogMax <= 0 {
		t.Errorf("SettlementBacklogMax = %d, want positive", cfg.SettlementBacklogMax)
	}
}

func TestFromEnvStreamTimeouts(t *testing.T) {
	// An explicit 0 is meaningful for the two duration knobs: it disables
	// the stall check or the stream total cap (documented rollback switch).
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":             "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":               "gpt-a:fake:gpt-a",
		"GATEWAY_PROVIDER":             "fake",
		"GATEWAY_STREAM_STALL_TIMEOUT": "0s",
		"GATEWAY_STREAM_TOTAL_TIMEOUT": "90s",
		"GATEWAY_STREAM_MAX_RETRIES":   "4",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StreamStallTimeout != 0 {
		t.Errorf("StreamStallTimeout = %v, want 0 (detection disabled)", cfg.StreamStallTimeout)
	}
	if cfg.StreamTotalTimeout != 90*time.Second {
		t.Errorf("StreamTotalTimeout = %v, want 90s", cfg.StreamTotalTimeout)
	}
	if cfg.StreamMaxRetries != 4 {
		t.Errorf("StreamMaxRetries = %d, want 4", cfg.StreamMaxRetries)
	}

	for _, tt := range []struct {
		name string
		env  string
		want string
	}{
		{"negative stall", "GATEWAY_STREAM_STALL_TIMEOUT=-1s", "GATEWAY_STREAM_STALL_TIMEOUT"},
		{"garbage stall", "GATEWAY_STREAM_STALL_TIMEOUT=soon", "GATEWAY_STREAM_STALL_TIMEOUT"},
		{"negative total", "GATEWAY_STREAM_TOTAL_TIMEOUT=-5m", "GATEWAY_STREAM_TOTAL_TIMEOUT"},
		{"above range retries", "GATEWAY_STREAM_MAX_RETRIES=11", "GATEWAY_STREAM_MAX_RETRIES"},
		{"negative retries", "GATEWAY_STREAM_MAX_RETRIES=-1", "GATEWAY_STREAM_MAX_RETRIES"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			name, value, _ := strings.Cut(tt.env, "=")
			setEnv(t, map[string]string{
				"GATEWAY_API_KEYS": "key-1:tenant-a:sk-abc",
				"GATEWAY_MODELS":   "gpt-a:fake:gpt-a",
				"GATEWAY_PROVIDER": "fake",
				name:               value,
			})
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

func TestEmbeddingsEnabledFlag(t *testing.T) {
	cases := map[string]bool{
		"false": false,
		"true":  true,
		"FALSE": true, // only the exact string "false" disables
		"":      true,
	}
	for raw, want := range cases {
		setEnv(t, map[string]string{
			"GATEWAY_API_KEYS":           "key-1:tenant-a:sk-abc",
			"GATEWAY_MODELS":             "gpt-a:fake:gpt-a",
			"GATEWAY_EMBEDDINGS_ENABLED": raw,
		})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if cfg.EmbeddingsEnabled != want {
			t.Errorf("GATEWAY_EMBEDDINGS_ENABLED=%q: EmbeddingsEnabled = %v, want %v", raw, cfg.EmbeddingsEnabled, want)
		}
	}
}

func TestDefaultModelsParsing(t *testing.T) {
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":       "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":         "gpt-a:fake:gpt-a,e:fake:e",
		"GATEWAY_DEFAULT_MODELS": "tenant-a:gpt-a:e, tenant-b:gpt-a",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.DefaultModels) != 2 {
		t.Fatalf("defaults = %+v", cfg.DefaultModels)
	}
	first := cfg.DefaultModels[0]
	if first.Subject != "tenant-a" || first.ChatModel != "gpt-a" || first.EmbeddingModel != "e" {
		t.Errorf("first entry = %+v", first)
	}
	second := cfg.DefaultModels[1]
	if second.Subject != "tenant-b" || second.ChatModel != "gpt-a" || second.EmbeddingModel != "" {
		t.Errorf("second entry = %+v (embedding slot optional)", second)
	}

	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":       "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":         "gpt-a:fake:gpt-a",
		"GATEWAY_DEFAULT_MODELS": "tenant-a",
	})
	if _, err := FromEnv(); err == nil {
		t.Error("a default-model entry without a chat model must fail startup")
	}
}

func TestResponsesEnabledFlag(t *testing.T) {
	cases := map[string]bool{
		"false": false,
		"true":  true,
		"FALSE": true, // only the exact string "false" disables
		"":      true,
	}
	for raw, want := range cases {
		setEnv(t, map[string]string{
			"GATEWAY_API_KEYS":          "key-1:tenant-a:sk-abc",
			"GATEWAY_MODELS":            "gpt-a:fake:gpt-a",
			"GATEWAY_RESPONSES_ENABLED": raw,
		})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if cfg.ResponsesEnabled != want {
			t.Errorf("GATEWAY_RESPONSES_ENABLED=%q: ResponsesEnabled = %v, want %v", raw, cfg.ResponsesEnabled, want)
		}
	}
}

func TestFromEnvInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"no keys":   {"GATEWAY_MODELS": "a:b:c"},
		"no models": {"GATEWAY_API_KEYS": "k:s:v"},
		"bad key format": {
			"GATEWAY_API_KEYS": "only-two-parts",
			"GATEWAY_MODELS":   "a:b:c",
		},
		"bad model format": {
			"GATEWAY_API_KEYS": "k:s:v",
			"GATEWAY_MODELS":   "only-two-parts",
		},
		"bad provider": {
			"GATEWAY_API_KEYS": "k:s:v",
			"GATEWAY_MODELS":   "a:b:c",
			"GATEWAY_PROVIDER": "anthropic",
		},
		"openai without key": {
			"GATEWAY_API_KEYS": "k:s:v",
			"GATEWAY_MODELS":   "a:openai:c",
			"GATEWAY_PROVIDER": "openai",
		},
		"anthropic without key": {
			"GATEWAY_API_KEYS": "k:s:v",
			"GATEWAY_MODELS":   "a:anthropic:c",
			"GATEWAY_PROVIDER": "anthropic",
		},
		"bad retries": {
			"GATEWAY_API_KEYS":    "k:s:v",
			"GATEWAY_MODELS":      "a:b:c",
			"GATEWAY_MAX_RETRIES": "99",
		},
	}
	for name, kv := range cases {
		setEnv(t, kv)
		if _, err := FromEnv(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// TestFromEnvProviderSecretsLoadedBeforeValidation pins the ordering fix: a
// GATEWAY_PROVIDER=anthropic configuration is accepted when its secret is
// present, because every provider secret is loaded before validation runs.
func TestFromEnvProviderSecretsLoadedBeforeValidation(t *testing.T) {
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":  "k:s:v",
		"GATEWAY_MODELS":    "a:anthropic:c",
		"GATEWAY_PROVIDER":  "anthropic",
		"ANTHROPIC_API_KEY": "sk-ant-test",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("valid anthropic configuration rejected: %v", err)
	}
	if cfg.AnthropicKey != "sk-ant-test" {
		t.Errorf("AnthropicKey = %q", cfg.AnthropicKey)
	}

	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS": "k:s:v",
		"GATEWAY_MODELS":   "a:openai:c",
		"GATEWAY_PROVIDER": "openai",
		"OPENAI_API_KEY":   "sk-oai-test",
	})
	cfg, err = FromEnv()
	if err != nil {
		t.Fatalf("valid openai configuration rejected: %v", err)
	}
	if cfg.OpenAIKey != "sk-oai-test" {
		t.Errorf("OpenAIKey = %q", cfg.OpenAIKey)
	}
}

// TestFromEnvDatabaseModeSkipsDevLists covers the configuration-mode boundary:
// GATEWAY_DATABASE_URL set means keys, catalog, routes, and policies come from
// PostgreSQL, so the development-only GATEWAY_API_KEYS/GATEWAY_MODELS lists
// are optional. A supplied value is still validated, and the legacy
// GATEWAY_PROVIDER selector must not demand credentials in database mode (the
// persisted provider registry is credential-checked at startup instead).
func TestFromEnvDatabaseModeSkipsDevLists(t *testing.T) {
	// Database mode with only database settings succeeds without dev lists.
	setEnv(t, map[string]string{"GATEWAY_DATABASE_URL": "postgres://db.example/gw"})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("database mode without dev lists rejected: %v", err)
	}
	if cfg.DatabaseURL == "" || len(cfg.Keys) != 0 || len(cfg.Models) != 0 {
		t.Errorf("cfg = %+v", cfg)
	}

	// Redis limiter settings compose with database mode.
	setEnv(t, map[string]string{
		"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
		"GATEWAY_LIMITS_MODE":  "redis",
		"GATEWAY_REDIS_ADDR":   "127.0.0.1:6379",
	})
	if _, err := FromEnv(); err != nil {
		t.Fatalf("database mode with redis rejected: %v", err)
	}

	// The legacy provider selector does not require credentials in database
	// mode: registry rows are validated at startup instead.
	setEnv(t, map[string]string{
		"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
		"GATEWAY_PROVIDER":     "openai",
	})
	if _, err := FromEnv(); err != nil {
		t.Fatalf("database mode must not require the legacy selector credential: %v", err)
	}

	// Malformed development values stay rejected: a supplied typo must fail
	// startup instead of being silently ignored.
	for name, kv := range map[string]map[string]string{
		"malformed keys": {
			"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
			"GATEWAY_API_KEYS":     "only-two-parts",
		},
		"malformed models": {
			"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
			"GATEWAY_MODELS":       "only-two-parts",
		},
		"unsupported provider": {
			"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
			"GATEWAY_PROVIDER":     "opeenai",
		},
	} {
		setEnv(t, kv)
		if _, err := FromEnv(); err == nil {
			t.Errorf("%s in database mode: expected error", name)
		}
	}

	// Well-formed supplied lists still parse in database mode.
	setEnv(t, map[string]string{
		"GATEWAY_DATABASE_URL": "postgres://db.example/gw",
		"GATEWAY_API_KEYS":     "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":       "gpt-a:fake:gpt-a",
	})
	cfg, err = FromEnv()
	if err != nil {
		t.Fatalf("database mode with supplied dev lists rejected: %v", err)
	}
	if len(cfg.Keys) != 1 || len(cfg.Models) != 1 {
		t.Errorf("supplied lists must still parse, got keys=%d models=%d", len(cfg.Keys), len(cfg.Models))
	}
}

// TestAsyncEnabledFlagAndValidation pins the V1.4 background-job rollout
// contract: acceptance is opt-in (the flag defaults off), the lifecycle knobs
// carry bounded defaults, and a malformed supplied value fails startup rather
// than being silently ignored.
func TestAsyncEnabledFlagAndValidation(t *testing.T) {
	base := map[string]string{
		"GATEWAY_API_KEYS": "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":   "gpt-a:fake:gpt-a",
	}
	// Default: off, with bounded defaults for every knob.
	setEnv(t, base)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AsyncEnabled {
		t.Error("async acceptance must default to disabled (gradual rollout)")
	}
	if cfg.AsyncWorkers != 2 || cfg.AsyncMaxAttempts != 3 || cfg.AsyncPollInterval <= 0 ||
		cfg.AsyncLease <= 0 || cfg.AsyncJobTimeout <= 0 || cfg.AsyncResultTTL <= 0 ||
		cfg.AsyncIdempotencyTTL <= 0 || cfg.AsyncDrainTimeout <= 0 || cfg.AsyncMaxResultBytes < 1024 {
		t.Errorf("async defaults missing: %+v", cfg)
	}
	// Exact-on opt-in, mirroring the other rollout switches. Every variation
	// merges the base vars: setEnv replaces the whole environment, and a bare
	// flag override would otherwise fail startup on missing keys.
	withBase := func(extra map[string]string) map[string]string {
		merged := map[string]string{}
		for k, v := range base {
			merged[k] = v
		}
		for k, v := range extra {
			merged[k] = v
		}
		return merged
	}
	setEnv(t, withBase(map[string]string{"GATEWAY_ASYNC_ENABLED": "true"}))
	if cfg, err = FromEnv(); err != nil || !cfg.AsyncEnabled {
		t.Errorf("GATEWAY_ASYNC_ENABLED=true: enabled=%v err=%v", cfg.AsyncEnabled, err)
	}
	setEnv(t, withBase(map[string]string{"GATEWAY_ASYNC_ENABLED": "TRUE"}))
	if cfg, err = FromEnv(); err != nil || cfg.AsyncEnabled {
		t.Errorf("only the exact string \"true\" enables: enabled=%v err=%v", cfg.AsyncEnabled, err)
	}
	// Well-formed overrides apply.
	setEnv(t, withBase(map[string]string{
		"GATEWAY_ASYNC_ENABLED":          "true",
		"GATEWAY_ASYNC_WORKERS":          "4",
		"GATEWAY_ASYNC_MAX_ATTEMPTS":     "5",
		"GATEWAY_ASYNC_LEASE":            "30s",
		"GATEWAY_ASYNC_RESULT_TTL":       "1h",
		"GATEWAY_ASYNC_MAX_RESULT_BYTES": "4096",
	}))
	if cfg, err = FromEnv(); err != nil {
		t.Fatal(err)
	}
	if cfg.AsyncWorkers != 4 || cfg.AsyncMaxAttempts != 5 || cfg.AsyncLease != 30*time.Second ||
		cfg.AsyncResultTTL != time.Hour || cfg.AsyncMaxResultBytes != 4096 {
		t.Errorf("async overrides not applied: %+v", cfg)
	}
	// Malformed values fail startup.
	for name, kv := range map[string]map[string]string{
		"zero workers":         {"GATEWAY_ASYNC_WORKERS": "0"},
		"huge workers":         {"GATEWAY_ASYNC_WORKERS": "65"},
		"malformed workers":    {"GATEWAY_ASYNC_WORKERS": "two"},
		"zero attempts":        {"GATEWAY_ASYNC_MAX_ATTEMPTS": "0"},
		"negative lease":       {"GATEWAY_ASYNC_LEASE": "-1s"},
		"malformed lease":      {"GATEWAY_ASYNC_LEASE": "soon"},
		"tiny result bytes":    {"GATEWAY_ASYNC_MAX_RESULT_BYTES": "16"},
		"malformed poll":       {"GATEWAY_ASYNC_POLL_INTERVAL": "5"},
		"malformed drain":      {"GATEWAY_ASYNC_DRAIN_TIMEOUT": "later"},
		"malformed idem ttl":   {"GATEWAY_ASYNC_IDEMPOTENCY_TTL": "-24h"},
		"malformed job t/o":    {"GATEWAY_ASYNC_JOB_TIMEOUT": "0s"},
		"malformed result ttl": {"GATEWAY_ASYNC_RESULT_TTL": "in a bit"},
	} {
		merged := map[string]string{"GATEWAY_ASYNC_ENABLED": "true"}
		for k, v := range base {
			merged[k] = v
		}
		for k, v := range kv {
			merged[k] = v
		}
		setEnv(t, merged)
		if _, err := FromEnv(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// TestBudgetsEnabledFlag mirrors the rollout-switch convention: opt-in,
// exact-on "true", default off — enforcement only, never ledger capture.
func TestBudgetsEnabledFlag(t *testing.T) {
	base := map[string]string{
		"GATEWAY_API_KEYS": "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":   "gpt-a:fake:gpt-a",
	}
	setEnv(t, base)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BudgetsEnabled {
		t.Error("budget enforcement must default to disabled (gradual rollout)")
	}
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":        base["GATEWAY_API_KEYS"],
		"GATEWAY_MODELS":          base["GATEWAY_MODELS"],
		"GATEWAY_BUDGETS_ENABLED": "true",
	})
	if cfg, err = FromEnv(); err != nil || !cfg.BudgetsEnabled {
		t.Errorf("GATEWAY_BUDGETS_ENABLED=true: enabled=%v err=%v", cfg.BudgetsEnabled, err)
	}
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":        base["GATEWAY_API_KEYS"],
		"GATEWAY_MODELS":          base["GATEWAY_MODELS"],
		"GATEWAY_BUDGETS_ENABLED": "TRUE",
	})
	if cfg, err = FromEnv(); err != nil || cfg.BudgetsEnabled {
		t.Errorf("only the exact string \"true\" enables: enabled=%v err=%v", cfg.BudgetsEnabled, err)
	}
}

// TestLifecycleFlags mirrors the rollback-switch convention for the V1.4
// data lifecycle: default on but inert until retention_policies rows exist
// (absence of a policy is a hold), with the exact string "false" as the
// documented opt-out, and a non-empty archive-sink root always configured.
func TestLifecycleFlags(t *testing.T) {
	base := map[string]string{
		"GATEWAY_API_KEYS": "key-1:tenant-a:sk-abc",
		"GATEWAY_MODELS":   "gpt-a:fake:gpt-a",
	}
	setEnv(t, base)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.LifecycleEnabled {
		t.Error("lifecycle must default to enabled (inert without retention policies)")
	}
	if cfg.LifecycleArchiveDir == "" {
		t.Error("archive sink root must have a non-empty default")
	}
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS":              base["GATEWAY_API_KEYS"],
		"GATEWAY_MODELS":                base["GATEWAY_MODELS"],
		"GATEWAY_LIFECYCLE_ENABLED":     "false",
		"GATEWAY_LIFECYCLE_ARCHIVE_DIR": "/var/lib/kbgw/archives",
	})
	if cfg, err = FromEnv(); err != nil || cfg.LifecycleEnabled {
		t.Errorf("GATEWAY_LIFECYCLE_ENABLED=false must disable: enabled=%v err=%v", cfg.LifecycleEnabled, err)
	}
	if cfg.LifecycleArchiveDir != "/var/lib/kbgw/archives" {
		t.Errorf("archive dir override = %q", cfg.LifecycleArchiveDir)
	}
}

// TestFromEnvZeroConfigInjection pins the issue-#13 zero-config contract:
// with the default fake provider, no database, and neither dev list
// supplied, FromEnv injects the documented development defaults so
// `go run ./cmd/gateway` boots with zero configuration. Database mode and
// non-fake providers never take the injection branch.
func TestFromEnvZeroConfigInjection(t *testing.T) {
	t.Run("zero environment injects dev defaults", func(t *testing.T) {
		setEnv(t, nil) // fake provider is the default; nothing else set
		cfg, err := FromEnv()
		if err != nil {
			t.Fatalf("zero-config startup rejected: %v", err)
		}
		if len(cfg.Keys) != 1 {
			t.Fatalf("keys = %+v, want exactly the injected dev key", cfg.Keys)
		}
		if k := cfg.Keys[0]; k.ID != DevSubject || k.Subject != DevSubject || k.PlaintextKey != DevAPIKey {
			t.Errorf("injected key = %+v, want id/subject %q and key %q", k, DevSubject, DevAPIKey)
		}
		if len(cfg.Models) != 1 {
			t.Fatalf("models = %+v, want exactly the injected dev model", cfg.Models)
		}
		if m := cfg.Models[0]; m.PublicName != DevModel || m.Provider != "fake" || m.UpstreamModel != DevModel || !m.Enabled {
			t.Errorf("injected model = %+v, want fake %q", m, DevModel)
		}
	})

	t.Run("database mode is never injected", func(t *testing.T) {
		setEnv(t, map[string]string{"GATEWAY_DATABASE_URL": "postgres://db.example/gw"})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatalf("database mode without dev lists rejected: %v", err)
		}
		if len(cfg.Keys) != 0 || len(cfg.Models) != 0 {
			t.Errorf("database mode must not inject dev defaults: keys=%d models=%d", len(cfg.Keys), len(cfg.Models))
		}
	})

	t.Run("non-fake provider is never injected", func(t *testing.T) {
		setEnv(t, map[string]string{"GATEWAY_PROVIDER": "openai"})
		cfg, err := FromEnv()
		if err == nil || !strings.Contains(err.Error(), "GATEWAY_API_KEYS: at least one key is required") {
			t.Fatalf("err = %v, want the at-least-one-key validation error", err)
		}
		if len(cfg.Keys) != 0 {
			t.Errorf("no key may be injected for a non-fake provider: %+v", cfg.Keys)
		}
	})
}

// TestFromEnvPartialConfigOptsOutOfInjection pins the explicit-configuration
// boundary: setting either dev list opts out of the zero-config injection,
// so a half-configured gateway keeps failing with the original
// "at least one key/model is required" errors instead of silently gaining
// injected entries.
func TestFromEnvPartialConfigOptsOutOfInjection(t *testing.T) {
	for name, tt := range map[string]struct {
		env  map[string]string
		want string
	}{
		"only keys": {
			env:  map[string]string{"GATEWAY_API_KEYS": "k1:dev:sk-x"},
			want: "GATEWAY_MODELS: at least one model is required",
		},
		"only models": {
			// A name distinct from DevModel so the no-injection check below
			// can tell an explicit entry apart from an injected one.
			env:  map[string]string{"GATEWAY_MODELS": "my-model:fake:my-model"},
			want: "GATEWAY_API_KEYS: at least one key is required",
		},
		"keys set but effectively empty": {
			env:  map[string]string{"GATEWAY_API_KEYS": " "},
			want: "GATEWAY_API_KEYS: at least one key is required",
		},
		"models set but effectively empty": {
			env:  map[string]string{"GATEWAY_MODELS": " "},
			want: "GATEWAY_MODELS: at least one model is required",
		},
	} {
		t.Run(name, func(t *testing.T) {
			setEnv(t, tt.env)
			cfg, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			for _, k := range cfg.Keys {
				if k.PlaintextKey == DevAPIKey {
					t.Errorf("injected dev key present despite explicit configuration: %+v", k)
				}
			}
			for _, m := range cfg.Models {
				if m.PublicName == DevModel && m.Provider == "fake" && m.UpstreamModel == DevModel {
					t.Errorf("injected dev model present despite explicit configuration: %+v", m)
				}
			}
		})
	}
}

// TestFromEnvProviderCredentialEnforcementUnchanged: real providers keep
// their credential enforcement; the zero-config injection is fake-only.
func TestFromEnvProviderCredentialEnforcementUnchanged(t *testing.T) {
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS": "k1:dev:sk-x",
		"GATEWAY_MODELS":   "gpt-4o-mini:openai:gpt-4o-mini",
		"GATEWAY_PROVIDER": "openai",
	})
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY must be set when GATEWAY_PROVIDER=openai") {
		t.Fatalf("err = %v, want the openai credential error", err)
	}
}

// TestSummary pins the startup summary line for both modes with exact
// full-string expectations. The summary is whitelist-only: a DSN or token
// value set in the environment must never appear in it.
func TestSummary(t *testing.T) {
	// Database mode with defaults: admin off without GATEWAY_ADMIN_TOKEN.
	setEnv(t, map[string]string{"GATEWAY_DATABASE_URL": "postgres://db.example/gw"})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	want := "mode=database limits=local responses=on embeddings=on async=off budgets=off lifecycle=on otlp=off admin=off"
	if got := cfg.Summary(); got != want {
		t.Errorf("database summary = %q, want %q", got, want)
	}

	// Database mode with every switch flipped and the admin token set.
	setEnv(t, map[string]string{
		"GATEWAY_DATABASE_URL":       "postgres://secret-user:secret-pass@db.example/gw",
		"GATEWAY_LIMITS_MODE":        "redis",
		"GATEWAY_ADMIN_TOKEN":        "super-secret-admin-token",
		"GATEWAY_ADMIN_ADDR":         ":9090",
		"GATEWAY_RESPONSES_ENABLED":  "false",
		"GATEWAY_EMBEDDINGS_ENABLED": "false",
		"GATEWAY_ASYNC_ENABLED":      "true",
		"GATEWAY_BUDGETS_ENABLED":    "true",
		"GATEWAY_LIFECYCLE_ENABLED":  "false",
		"GATEWAY_OTLP_ENABLED":       "true",
	})
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	want = "mode=database limits=redis responses=off embeddings=off async=on budgets=on lifecycle=off otlp=on admin=:9090"
	if got := cfg.Summary(); got != want {
		t.Errorf("database summary = %q, want %q", got, want)
	}
	if s := cfg.Summary(); strings.Contains(s, "secret") {
		t.Errorf("summary must not contain DSN or token values: %q", s)
	}

	// Dev mode with the injected zero-config defaults.
	setEnv(t, nil)
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Summary(), "mode=dev provider=fake keys=1 models=1"; got != want {
		t.Errorf("dev summary = %q, want %q", got, want)
	}

	// Dev mode with explicit lists counts them.
	setEnv(t, map[string]string{
		"GATEWAY_API_KEYS": "k1:tenant-a:sk-x,k2:tenant-b:sk-y",
		"GATEWAY_MODELS":   "gpt-a:fake:gpt-a",
	})
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.Summary(), "mode=dev provider=fake keys=2 models=1"; got != want {
		t.Errorf("dev summary = %q, want %q", got, want)
	}
}
