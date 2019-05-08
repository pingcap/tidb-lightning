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
	"github.com/pingcap/failpoint"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/parser/model"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/kvencoder"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/log"
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
		logger := log.L()
		logger.Error("tables failed to be imported", zap.Int("count", errorCount))
		for tableName, errorSummary := range es.summary {
			logger.Error("-",
				zap.String("table", tableName),
				zap.String("status", errorSummary.status.MetricName()),
				log.ShortError(errorSummary.err),
			)
		}
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
	opts := []func(context.Context) error{
		rc.checkRequirements,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.switchToNormalMode,
		rc.cleanCheckpoints,
	}

	task := log.L().Begin(zap.InfoLevel, "the whole procedure")

	var err error
outside:
	for i, process := range opts {
		err = process(ctx)
		logger := task.With(zap.Int("step", i), log.ShortError(err))

		switch {
		case err == nil:
		case log.IsContextCanceledError(err):
			logger.Info("user terminated")
			err = nil
			break outside
		default:
			logger.Error("run failed")
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			break outside // ps : not continue
		}
	}

	task.End(zap.ErrorLevel, err)
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
			task := log.With(zap.String("db", dbMeta.Name)).Begin(zap.InfoLevel, "restore table schema")

			tablesSchema := make(map[string]string)
			for _, tblMeta := range dbMeta.Tables {
				tablesSchema[tblMeta.Name] = tblMeta.GetSchema()
			}
			err = tidbMgr.InitSchema(ctx, dbMeta.Name, tablesSchema)

			task.End(zap.ErrorLevel, err)
			if err != nil {
				return errors.Annotatef(err, "restore table schema %s failed", dbMeta.Name)
			}
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

		failpoint.Inject("FailIfImportedChunk", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(val.(int)) {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfImportedChunk")
			}
		})

		failpoint.Inject("FailIfStatusBecomes", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*StatusCheckpointMerger); ok && merger.EngineID >= 0 && int(merger.Status) == val.(int) {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfStatusBecomes")
			}
		})

		failpoint.Inject("FailIfIndexEngineImported", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*StatusCheckpointMerger); ok &&
				merger.EngineID == wholeTableEngineID &&
				merger.Status == CheckpointStatusIndexImported && val.(int) > 0 {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfIndexEngineImported")
			}
		})

		failpoint.Inject("KillIfImportedChunk", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(val.(int)) {
				common.KillMySelf()
			}
		})
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
			log.L().Warn("stopping periodic actions", log.ShortError(ctx.Err()))
			return
		case <-stop:
			log.L().Info("everything imported, stopping periodic actions")
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

			var state string
			var remaining zap.Field
			if finished >= estimated {
				state = "post-processing"
				remaining = zap.Skip()
			} else if finished > 0 {
				remainNanoseconds := (estimated/finished - 1) * nanoseconds
				state = "writing"
				remaining = zap.Duration("remaining", time.Duration(remainNanoseconds).Round(time.Second))
			} else {
				state = "writing"
				remaining = zap.Skip()
			}

			// Note: a speed of 28 MiB/s roughly corresponds to 100 GiB/hour.
			log.L().Info("progress",
				zap.String("files", fmt.Sprintf("%.0f/%.0f (%.1f%%)", finished, estimated, finished/estimated*100)),
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", completedTables, totalTables, completedTables/totalTables*100)),
				zap.Float64("speed(MiB/s)", bytesRead/(1048576e-9*nanoseconds)),
				zap.String("state", state),
				remaining,
			)
		}
	}
}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	logTask := log.L().Begin(zap.InfoLevel, "restore all tables data")

	var wg sync.WaitGroup

	var restoreErr common.OnceError

	stopPeriodicActions := make(chan struct{}, 1)
	go rc.runPeriodicActions(ctx, stopPeriodicActions)

	type task struct {
		tr *TableRestore
		cp *TableCheckpoint
	}
	taskCh := make(chan task, rc.cfg.App.IndexConcurrency)
	defer close(taskCh)
	for i := 0; i < rc.cfg.App.IndexConcurrency; i++ {
		go func() {
			for task := range taskCh {
				tableLogTask := task.tr.logger.Begin(zap.InfoLevel, "restore table")
				err := task.tr.restoreTable(ctx, rc, task.cp)
				tableLogTask.End(zap.ErrorLevel, err)
				metric.RecordTableCount("completed", err)
				restoreErr.Set(err)
				wg.Done()
			}
		}()
	}

	for _, dbMeta := range rc.dbMetas {
		dbInfo, ok := rc.dbInfos[dbMeta.Name]
		if !ok {
			return errors.Errorf("database %s not found in rc.dbInfos", dbMeta.Name)
		}
		for _, tableMeta := range dbMeta.Tables {
			tableInfo, ok := dbInfo.Tables[tableMeta.Name]
			if !ok {
				return errors.Errorf("table info %s.%s not found", dbMeta.Name, tableMeta.Name)
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
			select {
			case taskCh <- task{tr: tr, cp: cp}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	wg.Wait()
	stopPeriodicActions <- struct{}{}

	err := restoreErr.Get()
	logTask.End(zap.ErrorLevel, err)
	return err
}

func (t *TableRestore) restoreTable(
	ctx context.Context,
	rc *RestoreController,
	cp *TableCheckpoint,
) error {
	// 1. Load the table info.

	// no need to do anything if the chunks are already populated
	if len(cp.Engines) > 0 {
		t.logger.Info("reusing engines and files info from checkpoint",
			zap.Int("enginesCnt", len(cp.Engines)),
			zap.Int("filesCnt", cp.CountChunks()),
		)
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

		logTask := t.logger.Begin(zap.InfoLevel, "import whole table")
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

				engineLogTask := t.logger.With(zap.Int32("engineNumber", eid)).Begin(zap.InfoLevel, "restore engine")
				dataClosedEngine, dataWorker, err := t.restoreEngine(ctx, rc, indexEngine, eid, ecp)
				engineLogTask.End(zap.ErrorLevel, err)
				rc.tableWorkers.Recycle(w)
				if err != nil {
					engineErr.Set(err)
					return
				}

				defer rc.closedEngineLimit.Recycle(dataWorker)
				if err := t.importEngine(ctx, dataClosedEngine, rc, eid, ecp); err != nil {
					engineErr.Set(err)
				}
			}(restoreWorker, engineID, engine)
		}

		wg.Wait()

		err = engineErr.Get()
		logTask.End(zap.ErrorLevel, err)
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

		failpoint.Inject("FailBeforeIndexEngineImported", func() {
			panic("forcing failure due to FailBeforeIndexEngineImported")
		})

		rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusIndexImported)
		if err != nil {
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

	logTask := t.logger.With(zap.Int32("engineNumber", engineID)).Begin(zap.InfoLevel, "encode kv data and write")

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
			chunkErr.Set(err)
		}(restoreWorker, cr)
	}

	wg.Wait()

	// Report some statistics into the log for debugging.
	totalKVSize := uint64(0)
	totalSQLSize := int64(0)
	for _, chunk := range cp.Chunks {
		totalKVSize += chunk.Checksum.SumSize()
		totalSQLSize += chunk.Chunk.EndOffset
	}

	err = chunkErr.Get()
	logTask.End(zap.ErrorLevel, err,
		zap.Int64("read", totalSQLSize),
		zap.Uint64("written", totalKVSize),
	)
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusAllWritten)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	dataWorker := rc.closedEngineLimit.Apply()
	closedDataEngine, err := dataEngine.Close(ctx)
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusClosed)
	if err != nil {
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
			// we ignore level-1 compact failure since it is not fatal.
			// no need log the error, it is done in (*Importer).Compact already.
			var _ = rc.doCompact(ctx, Level1Compact)
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
		err := AlterAutoIncrement(ctx, rc.tidbMgr.db, t.tableName, t.alloc.Base()+1)
		rc.alterTableLock.Unlock()
		rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusAlteredAutoInc)
		if err != nil {
			return err
		}
	}

	// 4. do table checksum
	var localChecksum verify.KVChecksum
	for _, engine := range cp.Engines {
		for _, chunk := range engine.Chunks {
			localChecksum.Add(&chunk.Checksum)
		}
	}

	t.logger.Info("local checksum", zap.Object("checksum", &localChecksum))
	if cp.Status < CheckpointStatusChecksummed {
		if !rc.cfg.PostRestore.Checksum {
			t.logger.Info("skip checksum")
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, nil, CheckpointStatusChecksumSkipped)
		} else {
			err := t.compareChecksum(ctx, rc.tidbMgr.db, localChecksum)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusChecksummed)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// 5. do table analyze
	if cp.Status < CheckpointStatusAnalyzed {
		if !rc.cfg.PostRestore.Analyze {
			t.logger.Info("skip analyze")
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, nil, CheckpointStatusAnalyzeSkipped)
		} else {
			err := t.analyzeTable(ctx, rc.tidbMgr.db)
			rc.saveStatusCheckpoint(t.tableName, wholeTableEngineID, err, CheckpointStatusAnalyzed)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// do full compaction for the whole data.
func (rc *RestoreController) fullCompact(ctx context.Context) error {
	if !rc.cfg.PostRestore.Compact {
		log.L().Info("skip full compaction")
		return nil
	}

	// wait until any existing level-1 compact to complete first.
	task := log.L().Begin(zap.InfoLevel, "wait for completion of existing level 1 compaction")
	for !atomic.CompareAndSwapInt32(&rc.compactState, compactStateIdle, compactStateDoing) {
		time.Sleep(100 * time.Millisecond)
	}
	task.End(zap.ErrorLevel, nil)

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
	// we ignore switch mode failure since it is not fatal.
	// no need log the error, it is done in (*Importer).SwitchMode already.
	rc.importer.SwitchMode(ctx, mode)
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
		log.L().Info("skip clean checkpoints")
		return nil
	}
	task := log.L().Begin(zap.InfoLevel, "clean checkpoints")
	err := rc.checkpointsDB.RemoveCheckpoint(ctx, "all")
	task.End(zap.ErrorLevel, err)
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
	logger    log.Logger
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
		logger:    log.With(zap.String("table", tableName)),
	}, nil
}

func (tr *TableRestore) Close() {
	tr.encTable = nil
	tr.logger.Info("restore done")
}

func (t *TableRestore) populateChunks(cfg *config.Config, cp *TableCheckpoint) error {
	task := t.logger.Begin(zap.InfoLevel, "load engines and files")
	chunks, err := mydump.MakeTableRegions(t.tableMeta, t.tableInfo.Columns, cfg.Mydumper.BatchSize, cfg.Mydumper.BatchImportRatio, cfg.App.TableConcurrency)
	if err == nil {
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
	}
	task.End(zap.ErrorLevel, err,
		zap.Int("enginesCnt", len(cp.Engines)),
		zap.Int("filesCnt", len(chunks)),
	)
	return err
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
				t.logger.Warn("column missing from data file, going to fill with default value",
					zap.Stringer("path", &ccp.Key),
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
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

func (tr *TableRestore) importKV(ctx context.Context, closedEngine *kv.ClosedEngine) error {
	task := closedEngine.Logger().Begin(zap.InfoLevel, "import and cleanup engine")

	err := closedEngine.Import(ctx)
	if err == nil {
		closedEngine.Cleanup(ctx)
	}

	dur := task.End(zap.ErrorLevel, err)

	if err != nil {
		return errors.Trace(err)
	}

	metric.ImportSecondsHistogram.Observe(dur.Seconds())

	failpoint.Inject("SlowDownImport", func() {})

	return nil
}

// do checksum for each table.
func (tr *TableRestore) compareChecksum(ctx context.Context, db *sql.DB, localChecksum verify.KVChecksum) error {
	remoteChecksum, err := DoChecksum(ctx, db, tr.tableName)
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

	tr.logger.Info("checksum pass", zap.Object("local", &localChecksum))
	return nil
}

func (tr *TableRestore) analyzeTable(ctx context.Context, db *sql.DB) error {
	task := tr.logger.Begin(zap.InfoLevel, "analyze")
	err := common.SQLWithRetry{DB: db, Logger: tr.logger}.
		Exec(ctx, "analyze table", "ANALYZE TABLE "+tr.tableName)
	task.End(zap.ErrorLevel, err)
	return err
}

// RemoteChecksum represents a checksum result got from tidb.
type RemoteChecksum struct {
	Schema     string
	Table      string
	Checksum   uint64
	TotalKVs   uint64
	TotalBytes uint64
}

func setSessionConcurrencyVars(ctx context.Context, db *sql.DB, dsn config.DBStore) {
	common.SQLWithRetry{DB: db, Logger: log.L()}.Exec(ctx, "set session concurrency variables", `SET
		SESSION tidb_build_stats_concurrency = ?,
		SESSION tidb_distsql_scan_concurrency = ?,
		SESSION tidb_index_serial_scan_concurrency = ?,
		SESSION tidb_checksum_table_concurrency = ?;
	`, dsn.BuildStatsConcurrency, dsn.DistSQLScanConcurrency, dsn.IndexSerialScanConcurrency, dsn.ChecksumTableConcurrency)
}

// DoChecksum do checksum for tables.
// table should be in <db>.<table>, format.  e.g. foo.bar
func DoChecksum(ctx context.Context, db *sql.DB, table string) (*RemoteChecksum, error) {
	ori, err := increaseGCLifeTime(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// set it back finally
	defer func() {
		err := UpdateGCLifeTime(ctx, db, ori)
		if err != nil {
			query := fmt.Sprintf("UPDATE mysql.tidb SET VARIABLE_VALUE = '%s' WHERE VARIABLE_NAME = 'tikv_gc_life_time'", ori)
			log.L().Warn("revert GC lifetime failed, please reset the GC lifetime manually after Lightning completed",
				zap.String("query", query),
				log.ShortError(err),
			)
		}
	}()

	task := log.With(zap.String("table", table)).Begin(zap.InfoLevel, "remote checksum")

	// ADMIN CHECKSUM TABLE <table>,<table>  example.
	// 	mysql> admin checksum table test.t;
	// +---------+------------+---------------------+-----------+-------------+
	// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
	// +---------+------------+---------------------+-----------+-------------+
	// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
	// +---------+------------+---------------------+-----------+-------------+

	cs := RemoteChecksum{}
	err = common.SQLWithRetry{DB: db, Logger: task.Logger}.QueryRow(ctx, "compute remote checksum",
		"ADMIN CHECKSUM TABLE "+table, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes,
	)
	dur := task.End(zap.ErrorLevel, err)
	metric.ChecksumSecondsHistogram.Observe(dur.Seconds())
	if err != nil {
		return nil, errors.Trace(err)
	}
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
		for i := 0; i < 5; i++ {
			putError = stream.Put(kvs)
			if putError == nil {
				break
			}
		}
		// retry still failed
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

	deliverLogger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
		zap.String("task", "deliver"),
	)

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
			deliverLogger.Error("write to data engine failed", log.ShortError(err))
			return
		}
		if err = writeToEngine(ctx, indexEngine, indexKVs); err != nil {
			deliverLogger.Error("write to index engine failed", log.ShortError(err))
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

	readTotalDur := time.Duration(0)
	encodeTotalDur := time.Duration(0)

	kvsCh := make(chan deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	go func() {
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		deliverCompleteCh <- deliverResult{dur, err}
	}()

	defer close(kvsCh)

	logTask := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
	).Begin(zap.InfoLevel, "restore file")

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
		lastRow := cr.parser.LastRow()
		kvs, err := kvEncoder.Encode(logTask.Logger, lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation)
		encodeDur := time.Since(start)
		encodeTotalDur += encodeDur
		metric.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())

		if err != nil {
			// error is already logged inside kvEncoder.Encode(), just propagate up directly.
			return err
		}

		deliverKvStart := time.Now()
		select {
		case kvsCh <- deliveredKVs{kvs: kvs, offset: newOffset, rowID: rowID}:
			break
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult := <-deliverCompleteCh:
			if deliverResult.err == nil {
				panic("unexpected: deliverCompleteCh prematurely fulfilled with no error")
			}
			return errors.Trace(deliverResult.err)
		}
		metric.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
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
		logTask.End(zap.ErrorLevel, deliverResult.err,
			zap.Duration("readDur", readTotalDur),
			zap.Duration("encodeDur", encodeTotalDur),
			zap.Duration("deliverDur", deliverResult.totalDur),
			zap.Object("checksum", &cr.chunk.Checksum),
		)
		return errors.Trace(deliverResult.err)
	case <-ctx.Done():
		return ctx.Err()
	}
}
