// Command migrate applies and rolls back the gateway's PostgreSQL schema
// with version tracking and advisory locking. It is an operational command:
// the gateway process never mutates the schema at startup.
//
// Usage:
//
//	migrate -dsn "$GATEWAY_DATABASE_URL" -dir migrations <command>
//
// Commands:
//
//	up       apply all pending migrations
//	down     roll back every migration (destructive)
//	steps N  apply N migrations (negative N rolls back)
//	version  print the current schema version
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang-migrate/migrate/v4"
	// The pgx5 driver registers itself for file:// -> PostgreSQL migrations
	// and imports pgx/v5/stdlib, which registers the "pgx" database/sql
	// driver used below.
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/dberr"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("GATEWAY_DATABASE_URL"), "PostgreSQL DSN (defaults to GATEWAY_DATABASE_URL)")
	dir := flag.String("dir", "migrations", "migrations directory")
	flag.Parse()

	if *dsn == "" {
		fatal("a PostgreSQL DSN is required (-dsn or GATEWAY_DATABASE_URL)")
	}
	cmd := flag.Arg(0)
	if cmd == "" {
		usage()
		os.Exit(2)
	}

	m, closeDB, err := newMigrator(*dsn, *dir)
	if err != nil {
		// pgx embeds DSN components (host, user, database, password-redacted
		// URL) in its connect errors; classify instead of echoing.
		fatal("connect: %s", dberr.DescribeConnectFailure(err))
	}
	defer closeDB()

	switch cmd {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			fatal("up: %v", err)
		}
	case "down":
		if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			fatal("down: %v", err)
		}
	case "steps":
		n, perr := strconv.Atoi(flag.Arg(1))
		if perr != nil || n == 0 {
			fatal("steps requires a non-zero integer, e.g. `steps -1`")
		}
		if err := m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			fatal("steps %d: %v", n, err)
		}
	case "version":
	default:
		usage()
		os.Exit(2)
	}

	version, dirty, verr := m.Version()
	if errors.Is(verr, migrate.ErrNilVersion) {
		fmt.Println("no migrations applied (version 0)")
		return
	}
	if verr != nil {
		fatal("read version: %v", verr)
	}
	fmt.Printf("version %d (dirty=%v)\n", version, dirty)
}

func newMigrator(dsn, dir string) (*migrate.Migrate, func(), error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, err
	}
	// database/sql is lazy: without an eager ping, bad-host and bad-credential
	// failures surface from Up/Version with DSN-bearing text instead of here.
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, nil, err
	}
	driver, err := pgx5.WithInstance(db, &pgx5.Config{})
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+absDir, "pgx5", driver)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return m, func() { _ = db.Close() }, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: migrate -dsn <postgres-dsn> [-dir migrations] up|down|steps N|version")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "migrate: "+format+"\n", args...)
	os.Exit(1)
}
