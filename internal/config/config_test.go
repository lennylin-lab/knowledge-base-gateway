package config

import (
	"os"
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
	"GATEWAY_DATABASE_URL", "GATEWAY_LIMITS_MODE", "GATEWAY_REDIS_ADDR",
	"GATEWAY_ADMIN_TOKEN", "GATEWAY_ADMIN_ADDR",
	"GATEWAY_ALLOW_INSECURE_BASE_URLS", "GATEWAY_RESPONSES_ENABLED",
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
	if cfg.MaxConcurrent != 8 {
		t.Errorf("default MaxConcurrent = %d, want 8", cfg.MaxConcurrent)
	}
	if !cfg.ResponsesEnabled {
		t.Error("ResponsesEnabled must default to true; GATEWAY_RESPONSES_ENABLED=false is the rollback switch")
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
