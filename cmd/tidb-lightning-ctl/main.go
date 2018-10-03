package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/restore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, errors.ErrorStack(err))
		os.Exit(1)
	}
}

func run() error {
	cfg := config.NewConfig()
	cfg.FlagSet = flag.NewFlagSet("lightning-ctl", flag.ExitOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "tidb-lightning.toml", "tidb-lightning configuration file")

	compact := fs.Bool("compact", false, "do manual compaction on the target cluster")
	mode := fs.String("switch-mode", "", "switch tikv into import mode or normal mode, values can be ['import', 'normal']")

	cpRemove := fs.String("checkpoint-remove", "", "remove the checkpoint associated with the given table")
	cpErrIgnore := fs.String("checkpoint-error-ignore", "", "ignore errors encoutered previously on this table; may corrupt this table if used incorrectly")
	cpErrDestroy := fs.String("checkpoint-error-destroy", "", "deletes imported data with table which has an error before")
	cpDump := fs.String("checkpoint-dump", "", "dump the checkpoint information as two CSV files in this folder")

	err := fs.Parse(os.Args[1:])
	if err == nil {
		err = cfg.Load()
	}
	if err != nil {
		return errors.Trace(err)
	}

	ctx := context.Background()

	if *compact {
		return errors.Trace(compactCluster(ctx, cfg))
	}
	if len(*mode) != 0 {
		return errors.Trace(switchMode(ctx, cfg, *mode))
	}
	if len(*cpRemove) != 0 {
		return errors.Trace(checkpointRemove(ctx, cfg, *cpRemove))
	}
	if len(*cpErrIgnore) != 0 {
		return errors.Trace(checkpointErrorIgnore(ctx, cfg, *cpErrIgnore))
	}
	if len(*cpErrDestroy) != 0 {
		return errors.Trace(checkpointErrorDestroy(ctx, cfg, *cpErrDestroy))
	}
	if len(*cpDump) != 0 {
		return errors.Trace(checkpointDump(ctx, cfg, *cpDump))
	}

	fs.Usage()
	return nil
}

func compactCluster(ctx context.Context, cfg *config.Config) error {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer importer.Close()

	if err := importer.Compact(ctx, restore.FullLevelCompact); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func switchMode(ctx context.Context, cfg *config.Config, mode string) error {
	var m import_sstpb.SwitchMode
	switch mode {
	case config.ImportMode:
		m = import_sstpb.SwitchMode_Import
	case config.NormalMode:
		m = import_sstpb.SwitchMode_Normal
	default:
		return errors.NotValidf("invalid mode %s, must use %s or %s", mode, config.ImportMode, config.NormalMode)
	}

	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer importer.Close()

	if err := importer.SwitchMode(ctx, m); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkpointRemove(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := restore.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	return errors.Trace(cpdb.RemoveCheckpoint(ctx, tableName))
}

func checkpointErrorIgnore(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := restore.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	return errors.Trace(cpdb.IgnoreErrorCheckpoint(ctx, tableName))
}

func checkpointErrorDestroy(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := restore.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	target, err := restore.NewTiDBManager(cfg.TiDB)
	if err != nil {
		return errors.Trace(err)
	}
	defer target.Close()

	return errors.Trace(cpdb.DestroyErrorCheckpoint(ctx, tableName, target))
}

func checkpointDump(ctx context.Context, cfg *config.Config, dumpFolder string) error {
	cpdb, err := restore.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	if err := os.MkdirAll(dumpFolder, 0755); err != nil {
		return errors.Trace(err)
	}

	tablesFileName := path.Join(dumpFolder, "tables.csv")
	tablesFile, err := os.Create(tablesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", tablesFileName)
	}
	defer tablesFile.Close()

	chunksFileName := path.Join(dumpFolder, "chunks.csv")
	chunksFile, err := os.Create(chunksFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", chunksFileName)
	}
	defer chunksFile.Close()

	if err := cpdb.DumpTables(ctx, tablesFile); err != nil {
		return errors.Trace(err)
	}
	if err := cpdb.DumpChunks(ctx, chunksFile); err != nil {
		return errors.Trace(err)
	}
	return nil
}
