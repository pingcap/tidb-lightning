// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/satori/go.uuid"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, errors.ErrorStack(err))
		os.Exit(1)
	}
}

func run() error {
	var (
		compact                                     *bool
		mode, flagImportEngine, flagCleanupEngine   *string
		cpRemove, cpErrIgnore, cpErrDestroy, cpDump *string

		fsUsage func()
	)

	globalCfg := config.Must(config.LoadGlobalConfig(os.Args[1:], func(fs *flag.FlagSet) {
		compact = fs.Bool("compact", false, "do manual compaction on the target cluster")
		mode = fs.String("switch-mode", "", "switch tikv into import mode or normal mode, values can be ['import', 'normal']")

		flagImportEngine = fs.String("import-engine", "", "manually import a closed engine (value can be '`db`.`table`:123' or a UUID")
		flagCleanupEngine = fs.String("cleanup-engine", "", "manually delete a closed engine")

		cpRemove = fs.String("checkpoint-remove", "", "remove the checkpoint associated with the given table (value can be 'all' or '`db`.`table`')")
		cpErrIgnore = fs.String("checkpoint-error-ignore", "", "ignore errors encoutered previously on the given table (value can be 'all' or '`db`.`table`'); may corrupt this table if used incorrectly")
		cpErrDestroy = fs.String("checkpoint-error-destroy", "", "deletes imported data with table which has an error before (value can be 'all' or '`db`.`table`')")
		cpDump = fs.String("checkpoint-dump", "", "dump the checkpoint information as two CSV files in the given folder")

		fsUsage = fs.Usage
	}))

	cfg := config.NewConfig()
	if err := cfg.LoadFromGlobal(globalCfg); err != nil {
		return err
	}
	if err := cfg.Adjust(); err != nil {
		return err
	}

	ctx := context.Background()

	if *compact {
		return errors.Trace(compactCluster(ctx, cfg))
	}
	if len(*mode) != 0 {
		return errors.Trace(switchMode(ctx, cfg, *mode))
	}
	if len(*flagImportEngine) != 0 {
		return errors.Trace(importEngine(ctx, cfg, *flagImportEngine))
	}
	if len(*flagCleanupEngine) != 0 {
		return errors.Trace(cleanupEngine(ctx, cfg, *flagCleanupEngine))
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

	fsUsage()
	return nil
}

func compactCluster(ctx context.Context, cfg *config.Config) error {
	return kv.ForAllStores(
		ctx,
		&http.Client{},
		cfg.TiDB.PdAddr,
		kv.StoreStateOffline,
		func(c context.Context, store *kv.Store) error {
			return kv.Compact(c, store.Address, restore.FullLevelCompact)
		},
	)
}

func switchMode(ctx context.Context, cfg *config.Config, mode string) error {
	var m import_sstpb.SwitchMode
	switch mode {
	case config.ImportMode:
		m = import_sstpb.SwitchMode_Import
	case config.NormalMode:
		m = import_sstpb.SwitchMode_Normal
	default:
		return errors.Errorf("invalid mode %s, must use %s or %s", mode, config.ImportMode, config.NormalMode)
	}

	return kv.ForAllStores(
		ctx,
		&http.Client{},
		cfg.TiDB.PdAddr,
		kv.StoreStateOffline,
		func(c context.Context, store *kv.Store) error {
			return kv.SwitchMode(c, store.Address, m)
		},
	)
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

	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer importer.Close()

	targetTables, err := cpdb.DestroyErrorCheckpoint(ctx, tableName)
	if err != nil {
		return errors.Trace(err)
	}

	var lastErr error

	for _, table := range targetTables {
		fmt.Fprintln(os.Stderr, "Dropping table:", table.TableName)
		err := target.DropTable(ctx, table.TableName)
		if err != nil {
			fmt.Fprintln(os.Stderr, "* Encountered error while dropping table:", err)
			lastErr = err
		}
	}

	for _, table := range targetTables {
		for engineID := table.MinEngineID; engineID <= table.MaxEngineID; engineID++ {
			fmt.Fprintln(os.Stderr, "Closing and cleaning up engine:", table.TableName, engineID)
			closedEngine, err := importer.UnsafeCloseEngine(ctx, table.TableName, engineID)
			if err != nil {
				fmt.Fprintln(os.Stderr, "* Encountered error while closing engine:", err)
				lastErr = err
			} else {
				closedEngine.Cleanup(ctx)
			}
		}
	}

	return errors.Trace(lastErr)
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

	enginesFileName := path.Join(dumpFolder, "engines.csv")
	enginesFile, err := os.Create(tablesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", enginesFileName)
	}
	defer enginesFile.Close()

	chunksFileName := path.Join(dumpFolder, "chunks.csv")
	chunksFile, err := os.Create(chunksFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", chunksFileName)
	}
	defer chunksFile.Close()

	if err := cpdb.DumpTables(ctx, tablesFile); err != nil {
		return errors.Trace(err)
	}
	if err := cpdb.DumpEngines(ctx, enginesFile); err != nil {
		return errors.Trace(err)
	}
	if err := cpdb.DumpChunks(ctx, chunksFile); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func unsafeCloseEngine(ctx context.Context, importer *kv.Importer, engine string) (*kv.ClosedEngine, error) {
	if index := strings.LastIndexByte(engine, ':'); index >= 0 {
		tableName := engine[:index]
		engineID, err := strconv.Atoi(engine[index+1:])
		if err != nil {
			return nil, errors.Trace(err)
		}
		ce, err := importer.UnsafeCloseEngine(ctx, tableName, int32(engineID))
		return ce, errors.Trace(err)
	}

	engineUUID, err := uuid.FromString(engine)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ce, err := importer.UnsafeCloseEngineWithUUID(ctx, "<tidb-lightning-ctl>", engineUUID)
	return ce, errors.Trace(err)
}

func importEngine(ctx context.Context, cfg *config.Config, engine string) error {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}

	ce, err := unsafeCloseEngine(ctx, importer, engine)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(ce.Import(ctx))
}

func cleanupEngine(ctx context.Context, cfg *config.Config, engine string) error {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}

	ce, err := unsafeCloseEngine(ctx, importer, engine)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(ce.Cleanup(ctx))
}
