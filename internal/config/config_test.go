package config

import (
	"os"
	"testing"
	"time"
)

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for _, k := range []string{"GATEWAY_ADDR", "GATEWAY_PROVIDER", "OPENAI_API_KEY", "OPENAI_BASE_URL", "GATEWAY_API_KEYS", "GATEWAY_MODELS", "GATEWAY_MAX_RETRIES", "GATEWAY_RATE_PER_MINUTE"} {
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
}

func TestFromEnvInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"no keys":   {"GATEWAY_MODELS": "a:b:c"},
		"no models": {"GATEWAY_API_KEYS": "k:s:v"},
		"bad key format": {
			"GATEWAY_API_KEYS": "only-two-parts",
			"GATEWAY_MODELS":   "a:b:c",
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
