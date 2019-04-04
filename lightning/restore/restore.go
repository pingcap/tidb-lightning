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

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/parser/model"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/kvencoder"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

const (
	defaultGCLifeTime = 100 * time.Hour
)

const (
	indexEngineID      = -1
	wholeTableEngineID = math.MaxInt32
)

const (
	compactStateIdle int32 = iota
	compactStateDoing
)

var (
	requiredTiDBVersion = *semver.New("2.1.0")
	requiredPDVersion   = *semver.New("2.1.0")
	requiredTiKVVersion = *semver.New("2.1.0")
)

func init() {
	cfg := tidbcfg.GetGlobalConfig()
	cfg.Log.SlowThreshold = 3000
}

type saveCp struct {
	tableName string
	merger    TableCheckpointMerger
}

type errorSummary struct {
	status CheckpointStatus
	err    error
}
type errorSummaries struct {
	sync.Mutex
	summary map[string]errorSummary
}

func (es *errorSummaries) emitLog() {
	es.Lock()
	defer es.Unlock()
	if errorCount := len(es.summary); errorCount > 0 {
		var msg strings.Builder
		fmt.Fprintf(&msg, "Totally **%d** tables failed to be imported.\n", errorCount)
		for tableName, errorSummary := range es.summary {
			fmt.Fprintf(&msg, "- [%s] [%s] %s\n", tableName, errorSummary.status.MetricName(), errorSummary.err.Error())
		}
		common.AppLogger.Error(msg.String())
	}
}

func (es *errorSummaries) record(tableName string, err error, status CheckpointStatus) {
	es.Lock()
	defer es.Unlock()
	es.summary[tableName] = errorSummary{status: status, err: err}
}

type RestoreController struct {
	cfg             *config.Config
	dbMetas         []*mydump.MDDatabaseMeta
	dbInfos         map[string]*TidbDBInfo
	tableWorkers    *worker.Pool
	indexWorkers    *worker.Pool
	regionWorkers   *worker.Pool
	ioWorkers       *worker.Pool
	importer        *kv.Importer
	tidbMgr         *TiDBManager
	postProcessLock sync.Mutex // a simple way to ensure post-processing is not concurrent without using complicated goroutines
	alterTableLock  sync.Mutex
	compactState    int32

	errorSummaries errorSummaries

	checkpointsDB CheckpointsDB
	saveCpCh      chan saveCp
	checkpointsWg sync.WaitGroup

	closedEngineLimit *worker.Pool
}

func NewRestoreController(ctx context.Context, dbMetas []*mydump.MDDatabaseMeta, cfg *config.Config) (*RestoreController, error) {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cpdb, err := OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tidbMgr, err := NewTiDBManager(cfg.TiDB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rc := &RestoreController{
		cfg:           cfg,
		dbMetas:       dbMetas,
		tableWorkers:  worker.NewPool(ctx, cfg.App.TableConcurrency, "table"),
		indexWorkers:  worker.NewPool(ctx, cfg.App.IndexConcurrency, "index"),
		regionWorkers: worker.NewPool(ctx, cfg.App.RegionConcurrency, "region"),
		ioWorkers:     worker.NewPool(ctx, cfg.App.IOConcurrency, "io"),
		importer:      importer,
		tidbMgr:       tidbMgr,

		errorSummaries: errorSummaries{
			summary: make(map[string]errorSummary),
		},

		checkpointsDB:     cpdb,
		saveCpCh:          make(chan saveCp),
		closedEngineLimit: worker.NewPool(ctx, cfg.App.TableConcurrency*2, "closed-engine"),
	}

	return rc, nil
}

func OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (CheckpointsDB, error) {
	if !cfg.Checkpoint.Enable {
		return NewNullCheckpointsDB(), nil
	}

	switch cfg.Checkpoint.Driver {
	case "mysql":
		db, err := sql.Open("mysql", cfg.Checkpoint.DSN)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cpdb, err := NewMySQLCheckpointsDB(ctx, db, cfg.Checkpoint.Schema)
		if err != nil {
			db.Close()
			return nil, errors.Trace(err)
		}
		return cpdb, nil

	case "file":
		return NewFileCheckpointsDB(cfg.Checkpoint.DSN), nil

	default:
		return nil, errors.Errorf("Unknown checkpoint driver %s", cfg.Checkpoint.Driver)
	}
}

func (rc *RestoreController) Wait() {
	close(rc.saveCpCh)
	rc.checkpointsWg.Wait()
}

func (rc *RestoreController) Close() {
	rc.importer.Close()
	rc.tidbMgr.Close()
}

func (rc *RestoreController) Run(ctx context.Context) error {
	timer := time.Now()
	opts := []func(context.Context) error{
		rc.checkRequirements,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.switchToNormalMode,
		rc.cleanCheckpoints,
	}

	var err error
outside:
	for i, process := range opts {
		err = process(ctx)
		switch {
		case err == nil:
		case !common.ShouldLogError(err):
			common.AppLogger.Infof("[step %d] user terminated : %v", i, err)
			err = nil
			break outside
		default:
			common.AppLogger.Errorf("[step %d] run cause error : %v", i, err)
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			break outside // ps : not continue
		}
	}

	common.AppLogger.Infof("the whole procedure takes %v", time.Since(timer))

	rc.errorSummaries.emitLog()

	return errors.Trace(err)
}

func (rc *RestoreController) restoreSchema(ctx context.Context) error {
	tidbMgr, err := NewTiDBManager(rc.cfg.TiDB)
	if err != nil {
		return errors.Trace(err)
	}
	defer tidbMgr.Close()

	if !rc.cfg.Mydumper.NoSchema {
		tidbMgr.db.ExecContext(ctx, "SET SQL_MODE = ?", rc.cfg.TiDB.StrSQLMode)

		for _, dbMeta := range rc.dbMetas {
			timer := time.Now()
			common.AppLogger.Infof("restore table schema for `%s`", dbMeta.Name)
			tablesSchema := make(map[string]string)
			for _, tblMeta := range dbMeta.Tables {
				tablesSchema[tblMeta.Name] = tblMeta.GetSchema()
			}
			err = tidbMgr.InitSchema(ctx, dbMeta.Name, tablesSchema)
			if err != nil {
				return errors.Errorf("db schema failed to init : %v", err)
			}
			common.AppLogger.Infof("restore table schema for `%s` takes %v", dbMeta.Name, time.Since(timer))
		}
	}
	dbInfos, err := tidbMgr.LoadSchemaInfo(ctx, rc.dbMetas)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dbInfos = dbInfos

	// Load new checkpoints
	err = rc.checkpointsDB.Initialize(ctx, dbInfos)
	if err != nil {
		return errors.Trace(err)
	}

	go rc.listenCheckpointUpdates()

	// Estimate the number of chunks for progress reporting
	rc.estimateChunkCountIntoMetrics()
	return nil
}

func (rc *RestoreController) estimateChunkCountIntoMetrics() {
	estimatedChunkCount := 0
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			estimatedChunkCount += len(tableMeta.DataFiles)
		}
	}
	metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated).Add(float64(estimatedChunkCount))
}

func (rc *RestoreController) saveStatusCheckpoint(tableName string, engineID int32, err error, statusIfSucceed CheckpointStatus) {
	merger := &StatusCheckpointMerger{Status: statusIfSucceed, EngineID: engineID}

	switch {
	case err == nil:
		break
	case !common.IsContextCanceledError(err):
		merger.SetInvalid()
		rc.errorSummaries.record(tableName, err, statusIfSucceed)
	default:
		return
	}

	if engineID == wholeTableEngineID {
		metric.RecordTableCount(statusIfSucceed.MetricName(), err)
	} else {
		metric.RecordEngineCount(statusIfSucceed.MetricName(), err)
	}

	rc.saveCpCh <- saveCp{tableName: tableName, merger: merger}
}

// listenCheckpointUpdates will combine several checkpoints together to reduce database load.
func (rc *RestoreController) listenCheckpointUpdates() {
	rc.checkpointsWg.Add(1)

	var lock sync.Mutex
	coalesed := make(map[string]*TableCheckpointDiff)

	hasCheckpoint := make(chan struct{}, 1)

	go func() {
		for range hasCheckpoint {
			lock.Lock()
			cpd := coalesed
			coalesed = make(map[string]*TableCheckpointDiff)
			lock.Unlock()

			if len(cpd) > 0 {
				rc.checkpointsDB.Update(cpd)
			}
			rc.checkpointsWg.Done()
		}
	}()

	for scp := range rc.saveCpCh {
		lock.Lock()
		cpd, ok := coalesed[scp.tableName]
		if !ok {
			cpd = NewTableCheckpointDiff()
			coalesed[scp.tableName] = cpd
		}
		scp.merger.MergeInto(cpd)

		if len(hasCheckpoint) == 0 {
			rc.checkpointsWg.Add(1)
			hasCheckpoint <- struct{}{}
		}

		lock.Unlock()

		// gofail: var FailIfImportedChunk int
		// if merger, ok := scp.merger.(*ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(FailIfImportedChunk) {
		// 	rc.checkpointsWg.Done()
		// 	rc.checkpointsWg.Wait()
		// 	panic("forcing failure due to FailIfImportedChunk")
		// }
		// goto RETURN1

		// gofail: RETURN1:

		// gofail: var FailIfStatusBecomes int
		// if merger, ok := scp.merger.(*StatusCheckpointMerger); ok && merger.EngineID >= 0 && int(merger.Status) == FailIfStatusBecomes {
		// 	rc.checkpointsWg.Done()
		// 	rc.checkpointsWg.Wait()
		// 	panic("forcing failure due to FailIfStatusBecomes")
		// }
		// goto RETURN2

		// gofail: RETURN2:

		// gofail: var FailIfIndexEngineImported int
		// if merger, ok := scp.merger.(*StatusCheckpointMerger); ok && merger.EngineID == wholeTableEngineID && merger.Status == CheckpointStatusIndexImported && FailIfIndexEngineImported > 0 {
		// 	rc.checkpointsWg.Done()
		// 	rc.checkpointsWg.Wait()
		// 	panic("forcing failure due to FailIfIndexEngineImported")
		// }
		// goto RETURN3

		// gofail: RETURN3:

		// gofail: var KillIfImportedChunk int
		// if merger, ok := scp.merger.(*ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(KillIfImportedChunk) {
		// 	common.KillMySelf()
		// }
		// goto RETURN4

		// gofail: RETURN4:
	}
	rc.checkpointsWg.Done()
}

func (rc *RestoreController) runPeriodicActions(ctx context.Context, stop <-chan struct{}) {
	switchModeTicker := time.NewTicker(rc.cfg.Cron.SwitchMode.Duration)
	logProgressTicker := time.NewTicker(rc.cfg.Cron.LogProgress.Duration)
	defer func() {
		switchModeTicker.Stop()
		logProgressTicker.Stop()
	}()

	rc.switchToImportMode(ctx)

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			common.AppLogger.Warnf("Stopping periodic actions due to %v", ctx.Err())
			return
		case <-stop:
			common.AppLogger.Info("Everything imported, stopping periodic actions")
			return

		case <-switchModeTicker.C:
			// periodically switch to import mode, as requested by TiKV 3.0
			rc.switchToImportMode(ctx)

		case <-logProgressTicker.C:
			// log the current progress periodically, so OPS will know that we're still working
			nanoseconds := float64(time.Since(start).Nanoseconds())
			estimated := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated))
			finished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
			totalTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))
			completedTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStateCompleted, metric.TableResultSuccess))
			bytesRead := metric.ReadHistogramSum(metric.RowReadBytesHistogram)

			var remaining string
			if finished >= estimated {
				remaining = ", post-processing"
			} else if finished > 0 {
				remainNanoseconds := (estimated/finished - 1) * nanoseconds
				remaining = fmt.Sprintf(", remaining %s", time.Duration(remainNanoseconds).Round(time.Second))
			}

			// Note: a speed of 28 MiB/s roughly corresponds to 100 GiB/hour.
			common.AppLogger.Infof(
				"progress: %.0f/%.0f chunks (%.1f%%), %.0f/%.0f tables (%.1f%%), speed %.2f MiB/s%s",
				finished, estimated, finished/estimated*100,
				completedTables, totalTables, completedTables/totalTables*100,
				bytesRead/(1048576e-9*nanoseconds),
				remaining,
			)
		}
	}
}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	timer := time.Now()
	var wg sync.WaitGroup

	var restoreErr common.OnceError

	stopPeriodicActions := make(chan struct{}, 1)
	go rc.runPeriodicActions(ctx, stopPeriodicActions)

	for _, dbMeta := range rc.dbMetas {
		dbInfo, ok := rc.dbInfos[dbMeta.Name]
		if !ok {
			common.AppLogger.Errorf("database %s not found in rc.dbInfos", dbMeta.Name)
			continue
		}
		for _, tableMeta := range dbMeta.Tables {
			tableInfo, ok := dbInfo.Tables[tableMeta.Name]
			if !ok {
				return errors.Errorf("table info %s not found", tableMeta.Name)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if cp.Status <= CheckpointStatusMaxInvalid {
				return errors.Errorf("Checkpoint for %s has invalid status: %d", tableName, cp.Status)
			}
			if err != nil {
				return errors.Trace(err)
			}
			tr, err := NewTableRestore(tableName, tableMeta, dbInfo, tableInfo, cp)
			if err != nil {
				return errors.Trace(err)
			}

			wg.Add(1)
			go func(t *TableRestore, cp *TableCheckpoint) {
				defer wg.Done()
				err := t.restoreTable(ctx, rc, cp)
				metric.RecordTableCount("completed", err)
				restoreErr.Set(t.tableName, err)
			}(tr, cp)
		}
	}

	wg.Wait()
	stopPeriodicActions <- struct{}{}
	common.AppLogger.Infof("restore all tables data takes %v", time.Since(timer))

	return errors.Trace(restoreErr.Get())
}

func (t *TableRestore) restoreTable(
	ctx context.Context,
	rc *RestoreController,
	cp *TableCheckpoint,
) error {
	// 1. Load the table info.

	// no need to do anything if the chunks are already populated
	if len(cp.Engines) > 0 {
		common.AppLogger.Infof("[%s] reusing %d engines and %d chunks from checkpoint", t.tableName, len(cp.Engines), cp.CountChunks())
	} else if cp.Status < CheckpointStatusAllWritten {
		if err := t.populateChunks(rc.cfg, cp); err != nil {
			return errors.Trace(err)
		}
		if err := rc.checkpointsDB.InsertEngineCheckpoints(ctx, t.tableName, cp.Engines); err != nil {
			return errors.Trace(err)
		}

		// rebase the allocator so it exceeds the number of rows.
		cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, t.tableInfo.core.AutoIncID)
		for _, engine := range cp.Engines {
			for _, chunk := range engine.Chunks {
				cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, chunk.Chunk.RowIDMax)
			}
		}
		t.alloc.Rebase(t.tableInfo.ID, cp.AllocBase, false)
		rc.saveCpCh <- saveCp{
			tableName: t.tableName,
			merger: &RebaseCheckpointMerger{
				AllocBase: cp.AllocBase,
			},
		}
	}

	// 2. Restore engines (if still needed)
	err := t.restoreEngines(ctx, rc, cp)
	if err != nil {
		return errors.Trace(err)
	}

	// 3. Post-process
	return errors.Trace(t.postProcess(ctx, rc, cp))
}

func (t *TableRestore) restoreEngines(ctx context.Context, rc *RestoreController, cp *TableCheckpoint) error {
	indexEngineCp := cp.Engines[indexEngineID]
	if indexEngineCp == nil {
		return errors.Errorf("table %v index engine checkpoint not found", t.tableName)
	}

	// The table checkpoint status set to `CheckpointStatusIndexImported` only if
	// both all data engines and the index engine had been imported to TiKV.
	// But persist index engine checkpoint status and table checkpoint status are
	// not an atomic operation, so `cp.Status < CheckpointStatusIndexImported`
	// but `indexEngineCp.Status == CheckpointStatusImported` could happen
	// when kill lightning after saving index engine checkpoint status before saving
	// table checkpoint status.
	var closedIndexEngine *kv.ClosedEngine
	if indexEngineCp.Status < CheckpointStatusImported && cp.Status < CheckpointStatusIndexImported {
		indexWorker := rc.indexWorkers.Apply()
		defer rc.indexWorkers.Recycle(indexWorker)
		indexEngine, err := rc.importer.OpenEngine(ctx, t.tableName, indexEngineID)
		if err != nil {
			return errors.Trace(err)
		}

		// The table checkpoint status less than `CheckpointStatusIndexImported` implies
		// that index engine checkpoint status less than `CheckpointStatusImported`.
		// So the index engine must be found in above process
		if indexEngine == nil {
			return errors.Errorf("table checkpoint status %v incompitable with index engine checkpoint status %v",
				cp.Status, indexEngineCp.Status)
		}

		timer := time.Now()
		var wg sync.WaitGroup
		var engineErr common.OnceError

		for engineID, engine := range cp.Engines {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if engineErr.Get() != nil {
				break
			}

			// Should skip index engine
			if engineID < 0 {
				continue
			}

			wg.Add(1)

			// Note: We still need tableWorkers to control the concurrency of tables.
			// In the future, we will investigate more about
			// the difference between restoring tables concurrently and restoring tables one by one.
			restoreWorker := rc.tableWorkers.Apply()

			go func(w *worker.Worker, eid int32, ecp *EngineCheckpoint) {
				defer wg.Done()
				tag := fmt.Sprintf("%s:%d", t.tableName, eid)

				dataClosedEngine, dataWorker, err := t.restoreEngine(ctx, rc, indexEngine, eid, ecp)
				rc.tableWorkers.Recycle(w)
				if err != nil {
					engineErr.Set(tag, err)
					return
				}

				defer rc.closedEngineLimit.Recycle(dataWorker)
				if err := t.importEngine(ctx, dataClosedEngine, rc, eid, ecp); err != nil {
					engineErr.Set(tag, err)
				}
			}(restoreWorker, engineID, engine)
		}

		wg.Wait()

		common.AppLogger.Infof("[%s] import whole table takes %v", t.tableName, time.Since(timer))
		err = engineErr.Get()
		if err != nil {
			return errors.Trace(err)
		}

		// If index engine file has been closed but not imported only if context cancel occurred
		// when `importKV()` execution, so `UnsafeCloseEngine` and continue import it.
		if indexEngineCp.Status == CheckpointStatusClosed {
			closedIndexEngine, err = rc.importer.UnsafeCloseEngine(ctx, t.tableName, indexEngineID)
		} else {
			closedIndexEngine, err = indexEngine.Close(ctx)
			rc.saveStatusCheckpoint(t.tableName, indexEngineID, err, CheckpointStatusClosed)
		}
		if err != nil {
			common.AppLogger.Errorf("[%s] [kv-deliver] index engine closed error: %s", t.tableName, errors.ErrorStack(err))
			return errors.Trace(err)
		}
	}

	if cp.Status < CheckpointStatusIndexImported {
		var err error
		if indexEngineCp.Status < CheckpointStatusImported {
			// the lock ensures the import() step will not be concurrent.
			rc.postProcessLock.Lock()
			err = t.importKV(ctx, closedIndexEngine)
			rc.postProcessLock.Unlock()
			rc.saveStatusCheckpoint(t.tableName, indexEngineID, err, CheckpointStatusImported)
		}

		// gofail: var FailBeforeIndexEngineImported struct{}
		//  panic("forcing failure due to FailBeforeIndexEngineImported")

		rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusIndexImported)
		if err != nil {
			common.AppLogger.Errorf("[%[1]s] failed to import index engine: %v", t.tableName, err.Error())
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TableRestore) restoreEngine(
	ctx context.Context,
	rc *RestoreController,
	indexEngine *kv.OpenedEngine,
	engineID int32,
	cp *EngineCheckpoint,
) (*kv.ClosedEngine, *worker.Worker, error) {
	if cp.Status >= CheckpointStatusClosed {
		w := rc.closedEngineLimit.Apply()
		closedEngine, err := rc.importer.UnsafeCloseEngine(ctx, t.tableName, engineID)
		// If any error occurred, recycle worker immediately
		if err != nil {
			rc.closedEngineLimit.Recycle(w)
			return closedEngine, nil, errors.Trace(err)
		}
		return closedEngine, w, nil
	}

	timer := time.Now()

	dataEngine, err := rc.importer.OpenEngine(ctx, t.tableName, engineID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var wg sync.WaitGroup
	var chunkErr common.OnceError

	// Restore table data
	for chunkIndex, chunk := range cp.Chunks {
		if chunk.Chunk.Offset >= chunk.Chunk.EndOffset {
			continue
		}

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		if chunkErr.Get() != nil {
			break
		}

		// Flows :
		// 	1. read mydump file
		// 	2. sql -> kvs
		// 	3. load kvs data (into kv deliver server)
		// 	4. flush kvs data (into tikv node)

		cr, err := newChunkRestore(chunkIndex, rc.cfg, chunk, rc.ioWorkers)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending).Inc()

		restoreWorker := rc.regionWorkers.Apply()
		wg.Add(1)
		go func(w *worker.Worker, cr *chunkRestore) {
			// Restore a chunk.
			defer func() {
				cr.close()
				wg.Done()
				rc.regionWorkers.Recycle(w)
			}()
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateRunning).Inc()
			err := cr.restore(ctx, t, engineID, dataEngine, indexEngine, rc)
			if err == nil {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()
				return
			}
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Inc()
			tag := fmt.Sprintf("%s:%d] [%s", t.tableName, engineID, &cr.chunk.Key)
			chunkErr.Set(tag, err)
		}(restoreWorker, cr)
	}

	wg.Wait()
	dur := time.Since(timer)

	// Report some statistics into the log for debugging.
	totalKVSize := uint64(0)
	totalSQLSize := int64(0)
	for _, chunk := range cp.Chunks {
		totalKVSize += chunk.Checksum.SumSize()
		totalSQLSize += chunk.Chunk.EndOffset
	}

	common.AppLogger.Infof("[%s:%d] encode kv data and write takes %v (read %d, written %d)", t.tableName, engineID, dur, totalSQLSize, totalKVSize)
	err = chunkErr.Get()
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusAllWritten)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	dataWorker := rc.closedEngineLimit.Apply()
	closedDataEngine, err := dataEngine.Close(ctx)
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusClosed)
	if err != nil {
		common.AppLogger.Errorf("[kv-deliver] flush stage with error (step = close) : %s", errors.ErrorStack(err))
		// If any error occurred, recycle worker immediately
		rc.closedEngineLimit.Recycle(dataWorker)
		return nil, nil, errors.Trace(err)
	}
	return closedDataEngine, dataWorker, nil
}

func (t *TableRestore) importEngine(
	ctx context.Context,
	closedEngine *kv.ClosedEngine,
	rc *RestoreController,
	engineID int32,
	cp *EngineCheckpoint,
) error {
	if cp.Status >= CheckpointStatusImported {
		return nil
	}

	// 1. close engine, then calling import
	// FIXME: flush is an asynchronous operation, what if flush failed?

	// the lock ensures the import() step will not be concurrent.
	rc.postProcessLock.Lock()
	err := t.importKV(ctx, closedEngine)
	rc.postProcessLock.Unlock()
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusImported)
	if err != nil {
		return errors.Trace(err)
	}

	// 2. perform a level-1 compact if idling.
	if rc.cfg.PostRestore.Level1Compact &&
		atomic.CompareAndSwapInt32(&rc.compactState, compactStateIdle, compactStateDoing) {
		go func() {
			err := rc.doCompact(ctx, Level1Compact)
			if err != nil {
				// log it and continue
				common.AppLogger.Warnf("compact %d failed %v", Level1Compact, err)
			}
			atomic.StoreInt32(&rc.compactState, compactStateIdle)
		}()
	}

	return nil
}

func (t *TableRestore) postProcess(ctx context.Context, rc *RestoreController, cp *TableCheckpoint) error {
	setSessionConcurrencyVars(ctx, rc.tidbMgr.db, rc.cfg.TiDB)

	// 3. alter table set auto_increment
	if cp.Status < CheckpointStatusAlteredAutoInc {
		rc.alterTableLock.Lock()
		err := t.restoreTableMeta(ctx, rc.tidbMgr.db)
		rc.alterTableLock.Unlock()
		rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusAlteredAutoInc)
		if err != nil {
			common.AppLogger.Errorf(
				"[%[1]s] failed to AUTO TABLE %[1]s SET AUTO_INCREMENT=%[2]d : %[3]v",
				t.tableName, t.alloc.Base()+1, err.Error(),
			)
			return errors.Trace(err)
		}
	}

	// 4. do table checksum
	var localChecksum verify.KVChecksum
	for _, engine := range cp.Engines {
		for _, chunk := range engine.Chunks {
			localChecksum.Add(&chunk.Checksum)
		}
	}
	common.AppLogger.Infof("[%s] local checksum [sum:%d, kvs:%d, size:%v]",
		t.tableName, localChecksum.Sum(), localChecksum.SumKVS(), localChecksum.SumSize())
	if cp.Status < CheckpointStatusChecksummed {
		if !rc.cfg.PostRestore.Checksum {
			common.AppLogger.Infof("[%s] Skip checksum.", t.tableName)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, nil, CheckpointStatusChecksumSkipped)
		} else {
			err := t.compareChecksum(ctx, rc.tidbMgr.db, localChecksum)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusChecksummed)
			if err != nil {
				common.AppLogger.Errorf("[%s] checksum failed: %v", t.tableName, err.Error())
				return errors.Trace(err)
			}
		}
	}

	// 5. do table analyze
	if cp.Status < CheckpointStatusAnalyzed {
		if !rc.cfg.PostRestore.Analyze {
			common.AppLogger.Infof("[%s] Skip analyze.", t.tableName)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, nil, CheckpointStatusAnalyzeSkipped)
		} else {
			err := t.analyzeTable(ctx, rc.tidbMgr.db)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusAnalyzed)
			if err != nil {
				common.AppLogger.Errorf("[%s] analyze failed: %v", t.tableName, err.Error())
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// do full compaction for the whole data.
func (rc *RestoreController) fullCompact(ctx context.Context) error {
	if !rc.cfg.PostRestore.Compact {
		common.AppLogger.Info("Skip full compaction.")
		return nil
	}

	// wait until any existing level-1 compact to complete first.
	common.AppLogger.Info("Wait for existing level 1 compaction to finish")
	start := time.Now()
	for !atomic.CompareAndSwapInt32(&rc.compactState, compactStateIdle, compactStateDoing) {
		time.Sleep(100 * time.Millisecond)
	}
	common.AppLogger.Infof("Wait for existing level 1 compaction to finish takes %v", time.Since(start))

	return errors.Trace(rc.doCompact(ctx, FullLevelCompact))
}

func (rc *RestoreController) doCompact(ctx context.Context, level int32) error {
	return errors.Trace(rc.importer.Compact(ctx, level))
}

func (rc *RestoreController) switchToImportMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import)
}

func (rc *RestoreController) switchToNormalMode(ctx context.Context) error {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal)
	return nil
}

func (rc *RestoreController) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) {
	if err := rc.importer.SwitchMode(ctx, mode); err != nil {
		common.AppLogger.Warnf("cannot switch to %s mode: %v", mode.String(), err)
	}
}

func (rc *RestoreController) checkRequirements(_ context.Context) error {
	// skip requirement check if explicitly turned off
	if !rc.cfg.App.CheckRequirements {
		return nil
	}

	client := &http.Client{}
	if err := rc.checkTiDBVersion(client); err != nil {
		return errors.Trace(err)
	}
	if err := rc.checkPDVersion(client); err != nil {
		return errors.Trace(err)
	}
	if err := rc.checkTiKVVersion(client); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func extractTiDBVersion(version string) (*semver.Version, error) {
	// version format: "5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f"
	//                               ^~~~~~~~~^ we only want this part
	// version format: "5.7.10-TiDB-v2.0.4-1-g06a0bf5"
	//                               ^~~~^
	// version format: "5.7.10-TiDB-v2.0.7"
	//                               ^~~~^
	// version format: "5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty"
	//                               ^~~~~~~~~^
	// The version is generated by `git describe --tags` on the TiDB repository.
	versions := strings.Split(strings.TrimSuffix(version, "-dirty"), "-")
	end := len(versions)
	switch end {
	case 3, 4:
	case 5, 6:
		end -= 2
	default:
		return nil, errors.Errorf("not a valid TiDB version: %s", version)
	}
	rawVersion := strings.Join(versions[2:end], "-")
	rawVersion = strings.TrimPrefix(rawVersion, "v")
	return semver.NewVersion(rawVersion)
}

func (rc *RestoreController) checkTiDBVersion(client *http.Client) error {
	url := fmt.Sprintf("http://%s:%d/status", rc.cfg.TiDB.Host, rc.cfg.TiDB.StatusPort)
	var status struct{ Version string }
	err := common.GetJSON(client, url, &status)
	if err != nil {
		return errors.Trace(err)
	}

	version, err := extractTiDBVersion(status.Version)
	if err != nil {
		return errors.Trace(err)
	}
	return checkVersion("TiDB", requiredTiDBVersion, *version)
}

func (rc *RestoreController) checkPDVersion(client *http.Client) error {
	url := fmt.Sprintf("http://%s/pd/api/v1/config/cluster-version", rc.cfg.TiDB.PdAddr)
	var rawVersion string
	err := common.GetJSON(client, url, &rawVersion)
	if err != nil {
		return errors.Trace(err)
	}

	version, err := semver.NewVersion(rawVersion)
	if err != nil {
		return errors.Trace(err)
	}

	return checkVersion("PD", requiredPDVersion, *version)
}

func (rc *RestoreController) checkTiKVVersion(client *http.Client) error {
	url := fmt.Sprintf("http://%s/pd/api/v1/stores", rc.cfg.TiDB.PdAddr)

	var stores struct {
		Stores []struct {
			Store struct {
				Address string
				Version string
			}
		}
	}
	err := common.GetJSON(client, url, &stores)
	if err != nil {
		return errors.Trace(err)
	}

	for _, store := range stores.Stores {
		version, err := semver.NewVersion(store.Store.Version)
		if err != nil {
			return errors.Annotate(err, store.Store.Address)
		}
		component := fmt.Sprintf("TiKV (at %s)", store.Store.Address)
		err = checkVersion(component, requiredTiKVVersion, *version)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkVersion(component string, expected, actual semver.Version) error {
	if actual.Compare(expected) >= 0 {
		return nil
	}
	return errors.Errorf(
		"%s version too old, expected '>=%s', found '%s'",
		component,
		expected,
		actual,
	)
}

func (rc *RestoreController) cleanCheckpoints(ctx context.Context) error {
	if !rc.cfg.Checkpoint.Enable || rc.cfg.Checkpoint.KeepAfterSuccess {
		common.AppLogger.Info("Skip clean checkpoints.")

		return nil
	}
	timer := time.Now()
	err := rc.checkpointsDB.RemoveCheckpoint(ctx, "all")
	common.AppLogger.Infof("clean checkpoints takes %v", time.Since(timer))
	return errors.Trace(err)
}

type chunkRestore struct {
	parser mydump.Parser
	index  int
	chunk  *ChunkCheckpoint
}

func newChunkRestore(
	index int,
	cfg *config.Config,
	chunk *ChunkCheckpoint,
	ioWorkers *worker.Pool,
) (*chunkRestore, error) {
	blockBufSize := cfg.Mydumper.ReadBlockSize

	reader, err := os.Open(chunk.Key.Path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var parser mydump.Parser
	switch path.Ext(strings.ToLower(chunk.Key.Path)) {
	case ".csv":
		parser = mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers)
	default:
		parser = mydump.NewChunkParser(cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	}

	reader.Seek(chunk.Chunk.Offset, io.SeekStart)
	parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax)

	return &chunkRestore{
		parser: parser,
		index:  index,
		chunk:  chunk,
	}, nil
}

func (cr *chunkRestore) close() {
	cr.parser.Close()
}

type TableRestore struct {
	// The unique table name in the form "`db`.`tbl`".
	tableName string
	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encTable  table.Table
	alloc     autoid.Allocator
}

func NewTableRestore(
	tableName string,
	tableMeta *mydump.MDTableMeta,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	cp *TableCheckpoint,
) (*TableRestore, error) {
	idAlloc := kv.NewPanickingAllocator(cp.AllocBase)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo.core)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", tableName)
	}

	return &TableRestore{
		tableName: tableName,
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
		encTable:  tbl,
		alloc:     idAlloc,
	}, nil
}

func (tr *TableRestore) Close() {
	tr.encTable = nil
	common.AppLogger.Infof("[%s] restore done", tr.tableName)
}

func (t *TableRestore) populateChunks(cfg *config.Config, cp *TableCheckpoint) error {
	common.AppLogger.Infof("[%s] load chunks", t.tableName)
	timer := time.Now()

	chunks, err := mydump.MakeTableRegions(t.tableMeta, t.tableInfo.Columns, cfg.Mydumper.BatchSize, cfg.Mydumper.BatchImportRatio, cfg.App.TableConcurrency)
	if err != nil {
		return errors.Trace(err)
	}

	for _, chunk := range chunks {
		engine, found := cp.Engines[chunk.EngineID]
		if !found {
			engine = &EngineCheckpoint{
				Status: CheckpointStatusLoaded,
			}
			cp.Engines[chunk.EngineID] = engine
		}
		engine.Chunks = append(engine.Chunks, &ChunkCheckpoint{
			Key: ChunkCheckpointKey{
				Path:   chunk.File,
				Offset: chunk.Chunk.Offset,
			},
			ColumnPermutation: nil,
			Chunk:             chunk.Chunk,
		})
	}

	// Add index engine checkpoint
	cp.Engines[indexEngineID] = &EngineCheckpoint{Status: CheckpointStatusLoaded}

	common.AppLogger.Infof("[%s] load %d engines and %d chunks takes %v", t.tableName, len(cp.Engines), len(chunks), time.Since(timer))
	return nil
}

// initializeColumns computes the "column permutation" for an INSERT INTO
// statement. Suppose a table has columns (a, b, c, d) in canonical order, and
// we execute `INSERT INTO (d, b, a) VALUES ...`, we will need to remap the
// columns as:
//
// - column `a` is at position 2
// - column `b` is at position 1
// - column `c` is missing
// - column `d` is at position 0
//
// The column permutation of (d, b, a) is set to be [2, 1, -1, 0].
func (t *TableRestore) initializeColumns(columns []string, ccp *ChunkCheckpoint) {
	colPerm := make([]int, 0, len(t.tableInfo.core.Columns)+1)
	shouldIncludeRowID := !t.tableInfo.core.PKIsHandle

	if len(columns) == 0 {
		// no provided columns, so use identity permutation.
		for i := range t.tableInfo.core.Columns {
			colPerm = append(colPerm, i)
		}
		if shouldIncludeRowID {
			colPerm = append(colPerm, -1)
		}
	} else {
		columnMap := make(map[string]int)
		for i, column := range columns {
			columnMap[column] = i
		}
		for _, colInfo := range t.tableInfo.core.Columns {
			if i, ok := columnMap[colInfo.Name.L]; ok {
				colPerm = append(colPerm, i)
			} else {
				colPerm = append(colPerm, -1)
			}
		}
		if i, ok := columnMap[model.ExtraHandleName.L]; ok {
			colPerm = append(colPerm, i)
		} else if shouldIncludeRowID {
			colPerm = append(colPerm, -1)
		}
	}

	ccp.ColumnPermutation = colPerm
}

func (tr *TableRestore) restoreTableMeta(ctx context.Context, db *sql.DB) error {
	timer := time.Now()

	err := AlterAutoIncrement(ctx, db, tr.tableMeta.DB, tr.tableMeta.Name, tr.alloc.Base()+1)
	if err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] alter table set auto_id takes %v", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name), time.Since(timer))
	return nil
}

func (tr *TableRestore) importKV(ctx context.Context, closedEngine *kv.ClosedEngine) error {
	common.AppLogger.Infof("[%s] flush kv deliver ...", closedEngine.Tag())
	start := time.Now()

	if err := closedEngine.Import(ctx); err != nil {
		if common.ShouldLogError(err) {
			common.AppLogger.Errorf("[%s] failed to flush kvs : %s", closedEngine.Tag(), err.Error())
		}
		return errors.Trace(err)
	}
	closedEngine.Cleanup(ctx)

	dur := time.Since(start)
	metric.ImportSecondsHistogram.Observe(dur.Seconds())
	common.AppLogger.Infof("[%s] kv deliver all flushed, takes %v", closedEngine.Tag(), dur)

	// gofail: var SlowDownImport struct{}

	return nil
}

// do checksum for each table.
func (tr *TableRestore) compareChecksum(ctx context.Context, db *sql.DB, localChecksum verify.KVChecksum) error {
	start := time.Now()
	remoteChecksum, err := DoChecksum(ctx, db, tr.tableName)
	dur := time.Since(start)
	metric.ChecksumSecondsHistogram.Observe(dur.Seconds())
	if err != nil {
		return errors.Trace(err)
	}

	if remoteChecksum.Checksum != localChecksum.Sum() ||
		remoteChecksum.TotalKVs != localChecksum.SumKVS() ||
		remoteChecksum.TotalBytes != localChecksum.SumSize() {
		return errors.Errorf("checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
	}

	common.AppLogger.Infof("[%s] checksum pass, %+v takes %v", tr.tableName, localChecksum, dur)
	return nil
}

func (tr *TableRestore) analyzeTable(ctx context.Context, db *sql.DB) error {
	timer := time.Now()
	common.AppLogger.Infof("[%s] analyze", tr.tableName)
	query := fmt.Sprintf("ANALYZE TABLE %s", tr.tableName)
	err := common.ExecWithRetry(ctx, db, query, query)
	if err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] analyze takes %v", tr.tableName, time.Since(timer))
	return nil
}

// RemoteChecksum represents a checksum result got from tidb.
type RemoteChecksum struct {
	Schema     string
	Table      string
	Checksum   uint64
	TotalKVs   uint64
	TotalBytes uint64
}

func (c *RemoteChecksum) String() string {
	return fmt.Sprintf("[%s] remote_checksum=%d, total_kvs=%d, total_bytes=%d", common.UniqueTable(c.Schema, c.Table), c.Checksum, c.TotalKVs, c.TotalBytes)
}

func setSessionConcurrencyVars(ctx context.Context, db *sql.DB, dsn config.DBStore) {
	err := common.ExecWithRetry(ctx, db, "(set session concurrency variables)", `SET
		SESSION tidb_build_stats_concurrency = ?,
		SESSION tidb_distsql_scan_concurrency = ?,
		SESSION tidb_index_serial_scan_concurrency = ?,
		SESSION tidb_checksum_table_concurrency = ?;
	`, dsn.BuildStatsConcurrency, dsn.DistSQLScanConcurrency, dsn.IndexSerialScanConcurrency, dsn.ChecksumTableConcurrency)
	if err != nil {
		common.AppLogger.Warnf("failed to set session concurrency variables: %s", err.Error())
	}
}

// DoChecksum do checksum for tables.
// table should be in <db>.<table>, format.  e.g. foo.bar
func DoChecksum(ctx context.Context, db *sql.DB, table string) (*RemoteChecksum, error) {
	timer := time.Now()

	ori, err := increaseGCLifeTime(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// set it back finally
	defer func() {
		err = UpdateGCLifeTime(ctx, db, ori)
		if err != nil && common.ShouldLogError(err) {
			common.AppLogger.Errorf("[%s] update tikv_gc_life_time error %v", table, errors.ErrorStack(err))
		}
	}()

	// ADMIN CHECKSUM TABLE <table>,<table>  example.
	// 	mysql> admin checksum table test.t;
	// +---------+------------+---------------------+-----------+-------------+
	// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
	// +---------+------------+---------------------+-----------+-------------+
	// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
	// +---------+------------+---------------------+-----------+-------------+

	cs := RemoteChecksum{}
	common.AppLogger.Infof("[%s] doing remote checksum", table)
	query := fmt.Sprintf("ADMIN CHECKSUM TABLE %s", table)
	err = common.QueryRowWithRetry(ctx, db, query, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] do checksum takes %v", table, time.Since(timer))

	return &cs, nil
}

func increaseGCLifeTime(ctx context.Context, db *sql.DB) (oriGCLifeTime string, err error) {
	// checksum command usually takes a long time to execute,
	// so here need to increase the gcLifeTime for single transaction.
	oriGCLifeTime, err = ObtainGCLifeTime(ctx, db)
	if err != nil {
		return "", errors.Trace(err)
	}

	var increaseGCLifeTime bool
	if oriGCLifeTime != "" {
		ori, err := time.ParseDuration(oriGCLifeTime)
		if err != nil {
			return "", errors.Trace(err)
		}
		if ori < defaultGCLifeTime {
			increaseGCLifeTime = true
		}
	} else {
		increaseGCLifeTime = true
	}

	if increaseGCLifeTime {
		err = UpdateGCLifeTime(ctx, db, defaultGCLifeTime.String())
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	return oriGCLifeTime, nil
}

////////////////////////////////////////////////////////////////

const (
	maxKVQueueSize  = 128      // Cache at most this number of rows before blocking the encode loop
	maxDeliverBytes = 31 << 20 // 31 MB. hardcoded by importer, so do we
	minDeliverBytes = 65536    // 64 KB. batch at least this amount of bytes to reduce number of messages
)

func splitIntoDeliveryStreams(totalKVs []kvenc.KvPair, splitSize int) [][]kvenc.KvPair {
	res := make([][]kvenc.KvPair, 0, 1)
	i := 0
	cumSize := 0

	for j, pair := range totalKVs {
		size := len(pair.Key) + len(pair.Val)
		if i < j && cumSize+size > splitSize {
			res = append(res, totalKVs[i:j])
			i = j
			cumSize = 0
		}
		cumSize += size
	}

	return append(res, totalKVs[i:])
}

func writeToEngine(ctx context.Context, engine *kv.OpenedEngine, totalKVs []kvenc.KvPair) error {
	if len(totalKVs) == 0 {
		return nil
	}

	stream, err := engine.NewWriteStream(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	var putError error
	for _, kvs := range splitIntoDeliveryStreams(totalKVs, maxDeliverBytes) {
		putError = stream.Put(kvs)
		if putError != nil {
			break
		}
	}

	if err := stream.Close(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(putError)
}

type deliveredKVs struct {
	kvs    []kvenc.KvPair // if kvs is empty, this indicated we've got the last message.
	offset int64
	rowID  int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}

func (cr *chunkRestore) deliverLoop(
	ctx context.Context,
	kvsCh <-chan deliveredKVs,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *kv.OpenedEngine,
	rc *RestoreController,
) (deliverTotalDur time.Duration, err error) {
	var channelClosed bool
	var dataKVs, indexKVs []kvenc.KvPair

	for !channelClosed {
		var dataChecksum, indexChecksum verify.KVChecksum
		var offset, rowID int64

		// Fetch enough KV pairs from the source.
	populate:
		for dataChecksum.SumSize()+indexChecksum.SumSize() < minDeliverBytes {
			select {
			case d := <-kvsCh:
				if len(d.kvs) == 0 {
					channelClosed = true
					break populate
				}

				for _, kv := range d.kvs {
					if kv.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
						dataKVs = append(dataKVs, kv)
						dataChecksum.UpdateOne(kv)
					} else {
						indexKVs = append(indexKVs, kv)
						indexChecksum.UpdateOne(kv)
					}
				}
				offset = d.offset
				rowID = d.rowID
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}

		// Write KVs into the engine
		start := time.Now()

		if err = writeToEngine(ctx, dataEngine, dataKVs); err != nil {
			return
		}
		if err = writeToEngine(ctx, indexEngine, indexKVs); err != nil {
			return
		}

		deliverDur := time.Since(start)
		deliverTotalDur += deliverDur
		metric.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
		metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumSize()))
		metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumSize()))
		metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumKVS()))
		metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumKVS()))

		dataKVs = dataKVs[:0]
		indexKVs = indexKVs[:0]

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		// No need to apply a lock since this is the only thread updating these variables.
		cr.chunk.Checksum.Add(&dataChecksum)
		cr.chunk.Checksum.Add(&indexChecksum)
		cr.chunk.Chunk.Offset = offset
		cr.chunk.Chunk.PrevRowIDMax = rowID
		if dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0 {
			// No need to save checkpoint if nothing was delivered.
			cr.saveCheckpoint(t, engineID, rc)
		}
	}

	return
}

func (cr *chunkRestore) saveCheckpoint(t *TableRestore, engineID int32, rc *RestoreController) {
	// We need to update the AllocBase every time we've finished a file.
	// The AllocBase is determined by the maximum of the "handle" (_tidb_rowid
	// or integer primary key), which can only be obtained by reading all data.
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &RebaseCheckpointMerger{
			AllocBase: t.alloc.Base() + 1,
		},
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &ChunkCheckpointMerger{
			EngineID: engineID,
			Key:      cr.chunk.Key,
			Checksum: cr.chunk.Checksum,
			Pos:      cr.chunk.Chunk.Offset,
			RowID:    cr.chunk.Chunk.PrevRowIDMax,
		},
	}
}

func (cr *chunkRestore) restore(
	ctx context.Context,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *kv.OpenedEngine,
	rc *RestoreController,
) error {
	// Create the encoder.
	kvEncoder := kv.NewTableKVEncoder(
		t.encTable,
		rc.cfg.TiDB.SQLMode,
	)
	defer func() {
		kvEncoder.Close()
		kvEncoder = nil
	}()

	timer := time.Now()
	readTotalDur := time.Duration(0)
	encodeTotalDur := time.Duration(0)

	kvsCh := make(chan deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	go func() {
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		deliverCompleteCh <- deliverResult{dur, err}
	}()

	defer close(kvsCh)

	initializedColumns := false
outside:
	for {
		offset, _ := cr.parser.Pos()
		if offset >= cr.chunk.Chunk.EndOffset {
			break
		}

		start := time.Now()
		err := cr.parser.ReadRow()
		newOffset, rowID := cr.parser.Pos()
		switch errors.Cause(err) {
		case nil:
			if !initializedColumns {
				if len(cr.chunk.ColumnPermutation) == 0 {
					t.initializeColumns(cr.parser.Columns(), cr.chunk)
				}
				initializedColumns = true
			}
		case io.EOF:
			break outside
		default:
			return errors.Trace(err)
		}

		readDur := time.Since(start)
		readTotalDur += readDur
		metric.RowReadSecondsHistogram.Observe(readDur.Seconds())
		metric.RowReadBytesHistogram.Observe(float64(newOffset - offset))

		// sql -> kv
		start = time.Now()
		lastRow := cr.parser.LastRow()
		kvs, err := kvEncoder.Encode(lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation)
		encodeDur := time.Since(start)
		encodeTotalDur += encodeDur
		metric.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())

		if err != nil {
			common.AppLogger.Errorf("kv encode failed = %s\n", err.Error())
			return errors.Trace(err)
		}

		select {
		case kvsCh <- deliveredKVs{kvs: kvs, offset: newOffset, rowID: rowID}:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult := <-deliverCompleteCh:
			if deliverResult.err == nil {
				panic("unexpected: deliverCompleteCh prematurely fulfilled with no error")
			}
			return errors.Trace(deliverResult.err)
		}
	}

	lastOffset, lastRowID := cr.parser.Pos()
	select {
	case kvsCh <- deliveredKVs{kvs: nil, offset: lastOffset, rowID: lastRowID}:
		break
	case <-ctx.Done():
		return ctx.Err()
	case deliverResult := <-deliverCompleteCh:
		if deliverResult.err == nil {
			panic("unexpected: deliverCompleteCh prematurely fulfilled with no error")
		}
		return errors.Trace(deliverResult.err)
	}

	select {
	case deliverResult := <-deliverCompleteCh:
		if deliverResult.err == nil {
			common.AppLogger.Infof(
				"[%s:%d] restore chunk #%d (%s) takes %v (read: %v, encode: %v, deliver: %v, size: %d, kvs: %d)",
				t.tableName, engineID, cr.index, &cr.chunk.Key, time.Since(timer),
				readTotalDur, encodeTotalDur, deliverResult.totalDur,
				cr.chunk.Checksum.SumSize(), cr.chunk.Checksum.SumKVS(),
			)
		}
		return errors.Trace(deliverResult.err)
	case <-ctx.Done():
		return ctx.Err()
	}
}
