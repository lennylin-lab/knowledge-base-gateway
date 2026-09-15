package envfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingFileIsOptional(t *testing.T) {
	if err := Load(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("Load missing file: %v", err)
	}
}

func TestLoadPreservesProcessEnvironment(t *testing.T) {
	const loadedKey = "KNOWLEDGE_BASE_GATEWAY_DOTENV_LOADED"
	const existingKey = "KNOWLEDGE_BASE_GATEWAY_DOTENV_EXISTING"
	t.Setenv(existingKey, "from-process")
	t.Cleanup(func() { _ = os.Unsetenv(loadedKey) })

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(loadedKey+"=from-file\n"+existingKey+"=from-file\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	if err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := os.Getenv(loadedKey); got != "from-file" {
		t.Fatalf("loaded value = %q, want from-file", got)
	}
	if got := os.Getenv(existingKey); got != "from-process" {
		t.Fatalf("existing value = %q, want from-process", got)
	}
}
