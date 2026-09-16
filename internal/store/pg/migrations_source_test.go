package pg

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// migrationsDirURL resolves the repository migrations directory to an
// absolute file:// URL, which is how golang-migrate's file source parses it.
func migrationsDirURL(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("../../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("migrations dir missing at %s: %v", abs, err)
	}
	return "file://" + abs
}

// TestMigrationFilesParse verifies, without a database, that every file under
// migrations/ follows the golang-migrate naming convention and forms complete
// up/down pairs, so cmd/migrate can apply and roll the schema back.
func TestMigrationFilesParse(t *testing.T) {
	d, err := source.Open(migrationsDirURL(t))
	if err != nil {
		t.Fatalf("open migrations dir: %v", err)
	}
	defer d.Close()

	// Walk the version chain and require an up and a down body for each.
	version, err := d.First()
	if err != nil {
		t.Fatalf("no first migration: %v", err)
	}
	versions := []uint{version}
	for {
		next, err := d.Next(version)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatalf("next after %d: %v", version, err)
		}
		versions = append(versions, next)
		version = next
	}
	if len(versions) != 5 {
		t.Fatalf("expected exactly 5 migration versions, got %v", versions)
	}

	for _, v := range versions {
		up, id, err := d.ReadUp(v)
		if err != nil {
			t.Fatalf("version %d: missing up migration: %v", v, err)
		}
		_, _ = io.Copy(io.Discard, up)
		up.Close()
		if id == "" {
			t.Fatalf("version %d: up migration has no identifier", v)
		}
		down, _, err := d.ReadDown(v)
		if err != nil {
			t.Fatalf("version %d: missing down migration: %v", v, err)
		}
		_, _ = io.Copy(io.Discard, down)
		down.Close()
	}
}
