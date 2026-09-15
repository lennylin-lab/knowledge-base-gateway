// Command replay runs deterministic provider fixtures through the offline
// provider adapters and asserts the normalized domain responses and stream
// events. It is fully offline: no network I/O, no API keys, no real
// providers. With no arguments it replays the fixtures embedded from
// internal/replay/testdata; -dir replays an external fixture directory
// instead. Exit code 1 means at least one fixture failed.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/knowledge-base/knowledge-base-gateway/internal/replay"
)

func main() {
	dir := flag.String("dir", "", "replay fixtures from this directory instead of the bundled set")
	flag.Parse()

	fixtures, err := load(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}

	failed := 0
	for _, f := range fixtures {
		if err := replay.Run(context.Background(), f); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", f.Name, err)
			continue
		}
		fmt.Printf("ok   %s\n", f.Name)
	}
	fmt.Printf("%d/%d fixtures passed\n", len(fixtures)-failed, len(fixtures))
	if failed > 0 {
		os.Exit(1)
	}
}

func load(dir string) ([]replay.Fixture, error) {
	if dir == "" {
		return replay.Bundled()
	}
	return replay.LoadDir(dir)
}
