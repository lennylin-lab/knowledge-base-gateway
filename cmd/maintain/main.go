// Command maintain runs the data-lifecycle maintenance process: retention
// sweeps (archive → verify → delete) and idempotency-key reclamation. It is
// deliberately separate from request serving (see docs/data-lifecycle.md):
// stop this process first when rolling lifecycle behavior back. With no
// retention_policies rows configured it deletes nothing, and a disabled or
// absent policy is a hold. Verified archives are never destroyed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/config"
	"github.com/knowledge-base/knowledge-base-gateway/internal/dberr"
	"github.com/knowledge-base/knowledge-base-gateway/internal/envfile"
	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	pgstore "github.com/knowledge-base/knowledge-base-gateway/internal/store/pg"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var (
		dry        bool
		table      string
		batchSize  int
		maxBatches int
		interval   time.Duration
	)
	flag.BoolVar(&dry, "dry", false, "dry run: report eligible rows, archive and delete nothing")
	flag.StringVar(&table, "table", "", "limit the cycle to one governed table (llm_requests, async_jobs, async_job_results, usage_ledger)")
	flag.IntVar(&batchSize, "batch", lifecycle.DefaultBatchSize, "rows per archive/delete batch")
	flag.IntVar(&maxBatches, "max-batches", lifecycle.DefaultMaxBatches, "batches per table per cycle")
	flag.DurationVar(&interval, "interval", 0, "loop cadence; 0 runs one cycle and exits")
	flag.Parse()

	if v := os.Getenv("GATEWAY_LIFECYCLE_ENABLED"); v == "false" {
		logger.Error("lifecycle operations are disabled by configuration (GATEWAY_LIFECYCLE_ENABLED=false); refusing to run")
		os.Exit(1)
	}
	dsn := os.Getenv("GATEWAY_DATABASE_URL")
	if dsn == "" {
		// The .env loader keeps the command aligned with the gateway's
		// development configuration path.
		if err := envfile.Load(".env"); err != nil {
			logger.Error("failed to load .env", "error", err)
			os.Exit(1)
		}
		dsn = os.Getenv("GATEWAY_DATABASE_URL")
	}
	if dsn == "" {
		logger.Error("GATEWAY_DATABASE_URL is required")
		os.Exit(1)
	}

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("configuration invalid", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, dry, table, batchSize, maxBatches, interval, logger); err != nil {
		logger.Error("maintenance failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, dry bool, table string, batchSize, maxBatches int, interval time.Duration, logger *slog.Logger) error {
	dbw, err := pgstore.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		// pgx errors embed DSN fragments; classify instead of wrapping.
		return fmt.Errorf("database connect: %s", dberr.DescribeConnectFailure(err))
	}
	defer dbw.Close()

	store := &pgstore.LifecycleStore{DB: dbw}
	admin, err := lifecycle.NewAdmin(lifecycle.AdminDeps{
		Store: store, Export: store,
		Sink:    lifecycle.NewFSArchiveSink(cfg.LifecycleArchiveDir),
		Metrics: metrics.New(), Logger: logger,
		BatchSize: batchSize, MaxBatches: maxBatches, Now: time.Now,
	})
	if err != nil {
		return err
	}

	// The maintenance process also owns KeyTTL reclamation outside the
	// retention cycle (the async worker sweeps on its own cadence; this path
	// covers deployments without async enabled).
	defer func() {
		if n, err := admin.Store().SweepExpiredIdempotencyKeys(context.Background(), time.Now()); err != nil {
			logger.Warn("idempotency key sweep failed", "error", err)
		} else if n > 0 {
			logger.Info("swept expired idempotency keys", "count", n)
		}
	}()

	for {
		cycleCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		results, err := admin.Sweep().Run(cycleCtx, lifecycle.Options{DryRun: dry, Only: lifecycle.TableName(table)})
		cancel()
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(results)
		logger.Info("maintenance cycle complete", "dry_run", dry, "results", json.RawMessage(raw))
		if interval <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}
