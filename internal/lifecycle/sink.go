package lifecycle

// Filesystem ArchiveSink. The durability contract: the data file is written
// through a temp file + fsync + rename, then read back and verified (row
// count and SHA-256 over the exact bytes) before the manifest is written the
// same way. A manifest therefore only ever exists for a fully persisted,
// verified artifact — the completion marker consumers (and the sweeper's
// delete phase) rely on. Any failure removes the partial artifact, so a
// partial archive is never eligible for source deletion.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FSArchiveSink writes archives under Root (created on demand).
type FSArchiveSink struct {
	Root string
}

// NewFSArchiveSink builds the filesystem sink rooted at dir.
func NewFSArchiveSink(dir string) *FSArchiveSink {
	return &FSArchiveSink{Root: dir}
}

// Write implements ArchiveSink.
func (s *FSArchiveSink) Write(_ context.Context, m Manifest, data []byte) (Manifest, error) {
	if err := os.MkdirAll(s.Root, 0o750); err != nil {
		return Manifest{}, fmt.Errorf("lifecycle: create archive dir: %w", err)
	}
	if m.ArchiveID == "" {
		m.ArchiveID = newArchiveID(string(m.Table))
	}
	m.DataFile = m.ArchiveID + ".ndjson"
	sum := sha256.Sum256(data)
	m.SHA256 = hex.EncodeToString(sum[:])
	m.RowCount = countLines(data)
	m.CreatedAt = time.Now().UTC()

	dataPath := filepath.Join(s.Root, m.DataFile)
	if err := writeFileSync(dataPath, data); err != nil {
		s.removeData(m)
		return Manifest{}, err
	}
	// Verification before the completion marker: re-read from storage and
	// compare against what this Write was asked to persist.
	stored, err := os.ReadFile(dataPath)
	if err != nil {
		s.removeData(m)
		return Manifest{}, fmt.Errorf("lifecycle: archive read-back failed: %w", err)
	}
	storedSum := sha256.Sum256(stored)
	if storedSum != sum || countLines(stored) != m.RowCount {
		s.removeData(m)
		return Manifest{}, fmt.Errorf("lifecycle: archive verification failed for %s", m.DataFile)
	}

	m.Complete = true
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		s.removeData(m)
		return Manifest{}, fmt.Errorf("lifecycle: encode manifest: %w", err)
	}
	if err := writeFileSync(filepath.Join(s.Root, m.ArchiveID+".manifest.json"), raw); err != nil {
		s.removeData(m)
		return Manifest{}, err
	}
	return m, nil
}

// removeData removes a failed artifact's data file. The manifest is only
// ever written after verification, so there is nothing else to clean up.
func (s *FSArchiveSink) removeData(m Manifest) {
	if m.DataFile == "" {
		return
	}
	_ = os.Remove(filepath.Join(s.Root, m.DataFile))
}

// newArchiveID names one artifact: table, UTC timestamp, random suffix.
func newArchiveID(table string) string {
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return fmt.Sprintf("%s_%s_%d", table, time.Now().UTC().Format("20060102T150405Z"), os.Getpid())
	}
	return fmt.Sprintf("%s_%s_%s", table, time.Now().UTC().Format("20060102T150405Z"), hex.EncodeToString(rnd[:]))
}

// writeFileSync persists via temp file + fsync + rename so readers never see
// a partial file and a crash never leaves an unmarked artifact behind.
func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("lifecycle: create %s: %w", filepath.Base(tmp), err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("lifecycle: write %s: %w", filepath.Base(tmp), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("lifecycle: sync %s: %w", filepath.Base(tmp), err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("lifecycle: close %s: %w", filepath.Base(tmp), err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("lifecycle: publish %s: %w", filepath.Base(path), err)
	}
	return nil
}

// countLines counts complete NDJSON lines (trailing-newline terminated).
func countLines(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	n := 0
	for _, b := range data {
		if b == '\n' {
			n++
		}
	}
	return n
}
