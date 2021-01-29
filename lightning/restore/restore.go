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
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/br/pkg/pdutil"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/glue"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/collate"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"modernc.org/mathutil"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	. "github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	"github.com/pingcap/tidb-lightning/lightning/web"
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
	indexEngineID = -1
)

const (
	compactStateIdle int32 = iota
	compactStateDoing
)

// DeliverPauser is a shared pauser to pause progress to (*chunkRestore).encodeLoop
var DeliverPauser = common.NewPauser()

func init() {
	// used in integration tests
	failpoint.Inject("SetMinDeliverBytes", func(v failpoint.Value) {
		minDeliverBytes = uint64(v.(int))
	})
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
	logger  log.Logger
	summary map[string]errorSummary
}

// makeErrorSummaries returns an initialized errorSummaries instance
func makeErrorSummaries(logger log.Logger) errorSummaries {
	return errorSummaries{
		logger:  logger,
		summary: make(map[string]errorSummary),
	}
}

func (es *errorSummaries) emitLog() {
	es.Lock()
	defer es.Unlock()

	if errorCount := len(es.summary); errorCount > 0 {
		logger := es.logger
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

const (
	diskQuotaStateIdle int32 = iota
	diskQuotaStateChecking
	diskQuotaStateImporting
)

type RestoreController struct {
	cfg             *config.Config
	dbMetas         []*mydump.MDDatabaseMeta
	dbInfos         map[string]*TidbDBInfo
	tableWorkers    *worker.Pool
	indexWorkers    *worker.Pool
	regionWorkers   *worker.Pool
	ioWorkers       *worker.Pool
	checksumWorks   *worker.Pool
	pauser          *common.Pauser
	backend         kv.Backend
	tidbGlue        glue.Glue
	postProcessLock sync.Mutex // a simple way to ensure post-processing is not concurrent without using complicated goroutines
	alterTableLock  sync.Mutex
	compactState    int32
	sysVars         map[string]string
	tls             *common.TLS

	errorSummaries errorSummaries

	checkpointsDB CheckpointsDB
	saveCpCh      chan saveCp
	checkpointsWg sync.WaitGroup

	closedEngineLimit *worker.Pool
	store             storage.ExternalStorage
	checksumManager   ChecksumManager

	diskQuotaLock  sync.RWMutex
	diskQuotaState int32
}

func NewRestoreController(
	ctx context.Context,
	dbMetas []*mydump.MDDatabaseMeta,
	cfg *config.Config,
	s storage.ExternalStorage,
	g glue.Glue,
) (*RestoreController, error) {
	return NewRestoreControllerWithPauser(ctx, dbMetas, cfg, s, DeliverPauser, g)
}

func NewRestoreControllerWithPauser(
	ctx context.Context,
	dbMetas []*mydump.MDDatabaseMeta,
	cfg *config.Config,
	s storage.ExternalStorage,
	pauser *common.Pauser,
	g glue.Glue,
) (*RestoreController, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		return nil, err
	}

	cpdb, err := g.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Annotate(err, "open checkpoint db failed")
	}

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "get task checkpoint failed")
	}
	if err := verifyCheckpoint(cfg, taskCp); err != nil {
		return nil, errors.Trace(err)
	}

	var backend kv.Backend
	switch cfg.TikvImporter.Backend {
	case config.BackendImporter:
		var err error
		backend, err = kv.NewImporter(ctx, tls, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
		if err != nil {
			return nil, errors.Annotate(err, "open importer backend failed")
		}
	case config.BackendTiDB:
		db, err := DBFromConfig(cfg.TiDB)
		if err != nil {
			return nil, errors.Annotate(err, "open tidb backend failed")
		}
		backend = kv.NewTiDBBackend(db, cfg.TikvImporter.OnDuplicate)
	case config.BackendLocal:
		var rLimit uint64
		rLimit, err = kv.GetSystemRLimit()
		if err != nil {
			return nil, err
		}
		maxOpenFiles := int(rLimit / uint64(cfg.App.TableConcurrency))
		// check overflow
		if maxOpenFiles < 0 {
			maxOpenFiles = math.MaxInt32
		}

		backend, err = kv.NewLocalBackend(ctx, tls, cfg.TiDB.PdAddr, int64(cfg.TikvImporter.RegionSplitSize),
			cfg.TikvImporter.SortedKVDir, cfg.TikvImporter.RangeConcurrency, cfg.TikvImporter.SendKVPairs,
			cfg.Checkpoint.Enable, g, maxOpenFiles)
		if err != nil {
			return nil, errors.Annotate(err, "build local backend failed")
		}
		err = verifyLocalFile(ctx, cpdb, cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown backend: " + cfg.TikvImporter.Backend)
	}

	rc := &RestoreController{
		cfg:           cfg,
		dbMetas:       dbMetas,
		tableWorkers:  worker.NewPool(ctx, cfg.App.TableConcurrency, "table"),
		indexWorkers:  worker.NewPool(ctx, cfg.App.IndexConcurrency, "index"),
		regionWorkers: worker.NewPool(ctx, cfg.App.RegionConcurrency, "region"),
		ioWorkers:     worker.NewPool(ctx, cfg.App.IOConcurrency, "io"),
		checksumWorks: worker.NewPool(ctx, cfg.TiDB.ChecksumTableConcurrency, "checksum"),
		pauser:        pauser,
		backend:       backend,
		tidbGlue:      g,
		sysVars:       defaultImportantVariables,
		tls:           tls,

		errorSummaries:    makeErrorSummaries(log.L()),
		checkpointsDB:     cpdb,
		saveCpCh:          make(chan saveCp),
		closedEngineLimit: worker.NewPool(ctx, cfg.App.TableConcurrency*2, "closed-engine"),

		store: s,
	}

	return rc, nil
}

func (rc *RestoreController) Close() {
	rc.backend.Close()
	rc.tidbGlue.GetSQLExecutor().Close()
}

func (rc *RestoreController) Run(ctx context.Context) error {
	opts := []func(context.Context) error{
		rc.checkRequirements,
		rc.setGlobalVariables,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.switchToNormalMode,
		rc.cleanCheckpoints,
	}

	task := log.L().Begin(zap.InfoLevel, "the whole procedure")

	var err error
	finished := false
outside:
	for i, process := range opts {
		err = process(ctx)
		if i == len(opts)-1 {
			finished = true
		}
		logger := task.With(zap.Int("step", i), log.ShortError(err))

		switch {
		case err == nil:
		case log.IsContextCanceledError(err):
			logger.Info("task canceled")
			err = nil
			break outside
		default:
			logger.Error("run failed")
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			break outside // ps : not continue
		}
	}

	// if process is cancelled, should make sure checkpoints are written to db.
	if !finished {
		rc.waitCheckpointFinish()
	}

	task.End(zap.ErrorLevel, err)
	rc.errorSummaries.emitLog()

	return errors.Trace(err)
}

type schemaStmtType int

func (stmtType schemaStmtType) String() string {
	switch stmtType {
	case schemaCreateDatabase:
		return "restore database schema"
	case schemaCreateTable:
		return "restore table schema"
	case schemaCreateView:
		return "restore view schema"
	}
	return "unknown statement of schema"
}

const (
	schemaCreateDatabase schemaStmtType = iota
	schemaCreateTable
	schemaCreateView
)

type schemaJob struct {
	dbName   string
	tblName  string // empty for create db jobs
	stmtType schemaStmtType
	stmts    []*schemaStmt
}

type schemaStmt struct {
	sql string
}

type restoreSchemaWorker struct {
	ctx   context.Context
	quit  context.CancelFunc
	jobCh chan *schemaJob
	errCh chan error
	wg    sync.WaitGroup
	glue  glue.Glue
	store storage.ExternalStorage
}

func (worker *restoreSchemaWorker) makeJobs(dbMetas []*mydump.MDDatabaseMeta) error {
	defer func() {
		close(worker.jobCh)
		worker.quit()
	}()
	var err error
	// 1. restore databases, execute statements concurrency
	for _, dbMeta := range dbMetas {
		restoreSchemaJob := &schemaJob{
			dbName:   dbMeta.Name,
			stmtType: schemaCreateDatabase,
			stmts:    make([]*schemaStmt, 0, 1),
		}
		restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
			sql: createDatabaseIfNotExistStmt(dbMeta.Name),
		})
		err = worker.appendJob(restoreSchemaJob)
		if err != nil {
			return err
		}
	}
	err = worker.wait()
	if err != nil {
		return err
	}
	// 2. restore tables, execute statements concurrency
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			sql := tblMeta.GetSchema(worker.ctx, worker.store)
			if sql != "" {
				stmts, err := createTableIfNotExistsStmt(worker.glue.GetParser(), sql, dbMeta.Name, tblMeta.Name)
				if err != nil {
					return err
				}
				restoreSchemaJob := &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  tblMeta.Name,
					stmtType: schemaCreateTable,
					stmts:    make([]*schemaStmt, 0, len(stmts)),
				}
				for _, sql := range stmts {
					restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
						sql: sql,
					})
				}
				err = worker.appendJob(restoreSchemaJob)
				if err != nil {
					return err
				}
			}
		}
	}
	err = worker.wait()
	if err != nil {
		return err
	}
	// 3. restore views. Since views can cross database we must restore views after all table schemas are restored.
	for _, dbMeta := range dbMetas {
		for _, viewMeta := range dbMeta.Views {
			sql := viewMeta.GetSchema(worker.ctx, worker.store)
			if sql != "" {
				stmts, err := createTableIfNotExistsStmt(worker.glue.GetParser(), sql, dbMeta.Name, viewMeta.Name)
				if err != nil {
					return err
				}
				restoreSchemaJob := &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  viewMeta.Name,
					stmtType: schemaCreateView,
					stmts:    make([]*schemaStmt, 0, len(stmts)),
				}
				for _, sql := range stmts {
					restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
						sql: sql,
					})
				}
				err = worker.appendJob(restoreSchemaJob)
				if err != nil {
					return err
				}
				// we don't support restore views concurrency, cauz it maybe will raise a error
				err = worker.wait()
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (worker *restoreSchemaWorker) doJob() {
	var session checkpoints.Session
	defer func() {
		if session != nil {
			session.Close()
		}
	}()
loop:
	for {
		select {
		case <-worker.ctx.Done():
			// don't `return` or throw `worker.ctx.Err()`here,
			// if we `return`, we can't mark cancelled jobs as done,
			// if we `throw(worker.ctx.Err())`, it will be blocked to death
			break loop
		case job := <-worker.jobCh:
			if job == nil {
				// successful exit
				return
			}
			var err error
			if session == nil {
				session, err = worker.glue.GetSession(worker.ctx)
				if err != nil {
					worker.wg.Done()
					worker.throw(err)
					// don't return
					break loop
				}
			}
			logger := log.With(zap.String("db", job.dbName), zap.String("table", job.tblName))
			for _, stmt := range job.stmts {
				task := logger.Begin(zap.DebugLevel, fmt.Sprintf("execute SQL: %s", stmt.sql))
				_, err = session.Execute(worker.ctx, stmt.sql)
				task.End(zap.ErrorLevel, err)
				if err != nil {
					err = errors.Annotatef(err, "%s %s failed", job.stmtType.String(), common.UniqueTable(job.dbName, job.tblName))
					worker.wg.Done()
					worker.throw(err)
					// don't return
					break loop
				}
			}
			worker.wg.Done()
		}
	}
	// mark the cancelled job as `Done`, a little tricky,
	// cauz we need make sure `worker.wg.Wait()` wouldn't blocked forever
	for range worker.jobCh {
		worker.wg.Done()
	}
}

func (worker *restoreSchemaWorker) wait() error {
	// avoid to `worker.wg.Wait()` blocked forever when all `doJob`'s goroutine exited.
	// don't worry about goroutine below, it never become a zombie,
	// cauz we have mechanism to clean cancelled jobs in `worker.jobCh`.
	// means whole jobs has been send to `worker.jobCh` would be done.
	waitCh := make(chan struct{})
	go func() {
		worker.wg.Wait()
		close(waitCh)
	}()
	select {
	case err := <-worker.errCh:
		return err
	case <-worker.ctx.Done():
		return worker.ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (worker *restoreSchemaWorker) throw(err error) {
	select {
	case <-worker.ctx.Done():
		// don't throw `worker.ctx.Err()` again, it will be blocked to death.
		return
	case worker.errCh <- err:
		worker.quit()
	}
}

func (worker *restoreSchemaWorker) appendJob(job *schemaJob) error {
	worker.wg.Add(1)
	select {
	case err := <-worker.errCh:
		// cancel the job
		worker.wg.Done()
		return err
	case <-worker.ctx.Done():
		// cancel the job
		worker.wg.Done()
		return worker.ctx.Err()
	case worker.jobCh <- job:
		return nil
	}
}

func (rc *RestoreController) restoreSchema(ctx context.Context) error {
	if !rc.cfg.Mydumper.NoSchema {
		logTask := log.L().Begin(zap.InfoLevel, "restore all schema")
		concurrency := utils.MinInt(rc.cfg.App.RegionConcurrency, 8)
		childCtx, cancel := context.WithCancel(ctx)
		worker := restoreSchemaWorker{
			ctx:   childCtx,
			quit:  cancel,
			jobCh: make(chan *schemaJob, concurrency),
			errCh: make(chan error),
			glue:  rc.tidbGlue,
			store: rc.store,
		}
		for i := 0; i < concurrency; i++ {
			go worker.doJob()
		}
		err := worker.makeJobs(rc.dbMetas)
		logTask.End(zap.ErrorLevel, err)
		if err != nil {
			return err
		}
	}
	getTableFunc := rc.backend.FetchRemoteTableModels
	if !rc.tidbGlue.OwnsSQLExecutor() {
		getTableFunc = rc.tidbGlue.GetTables
	}
	dbInfos, err := LoadSchemaInfo(ctx, rc.dbMetas, getTableFunc)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dbInfos = dbInfos

	// Load new checkpoints
	err = rc.checkpointsDB.Initialize(ctx, rc.cfg, dbInfos)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("InitializeCheckpointExit", func() {
		log.L().Warn("exit triggered", zap.String("failpoint", "InitializeCheckpointExit"))
		os.Exit(0)
	})

	go rc.listenCheckpointUpdates()

	rc.sysVars = ObtainImportantVariables(ctx, rc.tidbGlue.GetSQLExecutor())

	// Estimate the number of chunks for progress reporting
	err = rc.estimateChunkCountIntoMetrics(ctx)
	return err
}

// verifyCheckpoint check whether previous task checkpoint is compatible with task config
func verifyCheckpoint(cfg *config.Config, taskCp *TaskCheckpoint) error {
	if taskCp == nil {
		return nil
	}
	// always check the backend value even with 'check-requirements = false'
	retryUsage := "destroy all checkpoints"
	if cfg.Checkpoint.Driver == config.CheckpointDriverFile {
		retryUsage = fmt.Sprintf("delete the file '%s'", cfg.Checkpoint.DSN)
	}
	retryUsage += " and remove all restored tables and try again"

	if cfg.TikvImporter.Backend != taskCp.Backend {
		return errors.Errorf("config 'tikv-importer.backend' value '%s' different from checkpoint value '%s', please %s", cfg.TikvImporter.Backend, taskCp.Backend, retryUsage)
	}

	if cfg.App.CheckRequirements {
		if common.ReleaseVersion != taskCp.LightningVer {
			var displayVer string
			if len(taskCp.LightningVer) != 0 {
				displayVer = fmt.Sprintf("at '%s'", taskCp.LightningVer)
			} else {
				displayVer = "before v4.0.6/v3.0.19"
			}
			return errors.Errorf("lightning version is '%s', but checkpoint was created %s, please %s", common.ReleaseVersion, displayVer, retryUsage)
		}

		errorFmt := "config '%s' value '%s' different from checkpoint value '%s'. You may set 'check-requirements = false' to skip this check or " + retryUsage
		if cfg.Mydumper.SourceDir != taskCp.SourceDir {
			return errors.Errorf(errorFmt, "mydumper.data-source-dir", cfg.Mydumper.SourceDir, taskCp.SourceDir)
		}

		if cfg.TikvImporter.Backend == config.BackendLocal && cfg.TikvImporter.SortedKVDir != taskCp.SortedKVDir {
			return errors.Errorf(errorFmt, "mydumper.sorted-kv-dir", cfg.TikvImporter.SortedKVDir, taskCp.SortedKVDir)
		}

		if cfg.TikvImporter.Backend == config.BackendImporter && cfg.TikvImporter.Addr != taskCp.ImporterAddr {
			return errors.Errorf(errorFmt, "tikv-importer.addr", cfg.TikvImporter.Backend, taskCp.Backend)
		}

		if cfg.TiDB.Host != taskCp.TiDBHost {
			return errors.Errorf(errorFmt, "tidb.host", cfg.TiDB.Host, taskCp.TiDBHost)
		}

		if cfg.TiDB.Port != taskCp.TiDBPort {
			return errors.Errorf(errorFmt, "tidb.port", cfg.TiDB.Port, taskCp.TiDBPort)
		}

		if cfg.TiDB.PdAddr != taskCp.PdAddr {
			return errors.Errorf(errorFmt, "tidb.pd-addr", cfg.TiDB.PdAddr, taskCp.PdAddr)
		}
	}

	return nil
}

// for local backend, we should check if local SST exists in disk, otherwise we'll lost data
func verifyLocalFile(ctx context.Context, cpdb CheckpointsDB, dir string) error {
	targetTables, err := cpdb.GetLocalStoringTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for tableName, engineIDs := range targetTables {
		for _, engineID := range engineIDs {
			_, eID := kv.MakeUUID(tableName, engineID)
			file := kv.LocalFile{Uuid: eID}
			err := file.Exist(dir)
			if err != nil {
				log.L().Error("can't find local file",
					zap.String("table name", tableName),
					zap.Int32("engine ID", engineID))
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (rc *RestoreController) estimateChunkCountIntoMetrics(ctx context.Context) error {
	estimatedChunkCount := 0.0
	estimatedEngineCnt := int64(0)
	batchSize := int64(rc.cfg.Mydumper.BatchSize)
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			tableName := common.UniqueTable(dbMeta.Name, tableMeta.Name)
			dbCp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}

			fileChunks := make(map[string]float64)
			for engineId, eCp := range dbCp.Engines {
				if eCp.Status < CheckpointStatusImported {
					estimatedEngineCnt++
				}
				if engineId == indexEngineID {
					continue
				}
				for _, c := range eCp.Chunks {
					if _, ok := fileChunks[c.Key.Path]; !ok {
						fileChunks[c.Key.Path] = 0.0
					}
					remainChunkCnt := float64(c.Chunk.EndOffset-c.Chunk.Offset) / float64(c.Chunk.EndOffset-c.Key.Offset)
					fileChunks[c.Key.Path] += remainChunkCnt
				}
			}
			// estimate engines count if engine cp is empty
			if len(dbCp.Engines) == 0 {
				estimatedEngineCnt += ((tableMeta.TotalSize + batchSize - 1) / batchSize) + 1
			}
			for _, fileMeta := range tableMeta.DataFiles {
				if cnt, ok := fileChunks[fileMeta.FileMeta.Path]; ok {
					estimatedChunkCount += cnt
					continue
				}
				if fileMeta.FileMeta.Type == mydump.SourceTypeCSV {
					cfg := rc.cfg.Mydumper
					if fileMeta.FileMeta.FileSize > int64(cfg.MaxRegionSize) && cfg.StrictFormat && !cfg.CSV.Header {
						estimatedChunkCount += math.Round(float64(fileMeta.FileMeta.FileSize) / float64(cfg.MaxRegionSize))
					} else {
						estimatedChunkCount += 1
					}
				} else {
					estimatedChunkCount += 1
				}
			}
		}
	}
	metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated).Add(estimatedChunkCount)
	metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess).
		Add(float64(estimatedEngineCnt))
	rc.tidbGlue.Record(glue.RecordEstimatedChunk, uint64(estimatedChunkCount))
	return nil
}

func (rc *RestoreController) saveStatusCheckpoint(tableName string, engineID int32, err error, statusIfSucceed CheckpointStatus) {
	merger := &StatusCheckpointMerger{Status: statusIfSucceed, EngineID: engineID}

	log.L().Debug("update checkpoint", zap.String("table", tableName), zap.Int32("engine_id", engineID),
		zap.Uint8("new_status", uint8(statusIfSucceed)), zap.Error(err))

	switch {
	case err == nil:
		break
	case !common.IsContextCanceledError(err):
		merger.SetInvalid()
		rc.errorSummaries.record(tableName, err, statusIfSucceed)
	default:
		return
	}

	if engineID == WholeTableEngineID {
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
	defer close(hasCheckpoint)

	go func() {
		for range hasCheckpoint {
			lock.Lock()
			cpd := coalesed
			coalesed = make(map[string]*TableCheckpointDiff)
			lock.Unlock()

			if len(cpd) > 0 {
				rc.checkpointsDB.Update(cpd)
				web.BroadcastCheckpointDiff(cpd)
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
				merger.EngineID == WholeTableEngineID &&
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
	// a nil channel blocks forever.
	// if the cron duration is zero we use the nil channel to skip the action.
	var logProgressChan <-chan time.Time
	if rc.cfg.Cron.LogProgress.Duration > 0 {
		logProgressTicker := time.NewTicker(rc.cfg.Cron.LogProgress.Duration)
		defer logProgressTicker.Stop()
		logProgressChan = logProgressTicker.C
	}

	glueProgressTicker := time.NewTicker(3 * time.Second)
	defer glueProgressTicker.Stop()

	var switchModeChan <-chan time.Time
	// tidb backend don't need to switch tikv to import mode
	if rc.cfg.TikvImporter.Backend != config.BackendTiDB && rc.cfg.Cron.SwitchMode.Duration > 0 {
		switchModeTicker := time.NewTicker(rc.cfg.Cron.SwitchMode.Duration)
		defer switchModeTicker.Stop()
		switchModeChan = switchModeTicker.C

		rc.switchToImportMode(ctx)
	}

	var checkQuotaChan <-chan time.Time
	// only local storage has disk quota concern.
	if rc.cfg.TikvImporter.Backend == config.BackendLocal && rc.cfg.Cron.CheckDiskQuota.Duration > 0 {
		checkQuotaTicker := time.NewTicker(rc.cfg.Cron.CheckDiskQuota.Duration)
		defer checkQuotaTicker.Stop()
		checkQuotaChan = checkQuotaTicker.C
	}

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.L().Warn("stopping periodic actions", log.ShortError(ctx.Err()))
			return
		case <-stop:
			log.L().Info("everything imported, stopping periodic actions")
			return

		case <-switchModeChan:
			// periodically switch to import mode, as requested by TiKV 3.0
			rc.switchToImportMode(ctx)

		case <-logProgressChan:
			// log the current progress periodically, so OPS will know that we're still working
			nanoseconds := float64(time.Since(start).Nanoseconds())
			// the estimated chunk is not accurate(likely under estimated), but the actual count is not accurate
			// before the last table start, so use the bigger of the two should be a workaround
			estimated := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated))
			pending := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
			if estimated < pending {
				estimated = pending
			}
			finished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
			totalTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))
			completedTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStateCompleted, metric.TableResultSuccess))
			bytesRead := metric.ReadHistogramSum(metric.RowReadBytesHistogram)
			engineEstimated := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess))
			enginePending := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStatePending, metric.TableResultSuccess))
			if engineEstimated < enginePending {
				engineEstimated = enginePending
			}
			engineFinished := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.TableStateImported, metric.TableResultSuccess))
			bytesWritten := metric.ReadCounter(metric.BytesCounter.WithLabelValues(metric.TableStateWritten))
			bytesImported := metric.ReadCounter(metric.BytesCounter.WithLabelValues(metric.TableStateImported))

			var state string
			var remaining zap.Field
			if finished >= estimated {
				if engineFinished < engineEstimated {
					state = "importing"
				} else {
					state = "post-processing"
				}
				remaining = zap.Skip()
			} else if finished > 0 {

				state = "writing"

			} else {
				state = "preparing"

			}

			// since we can't accurately estimate the extra time cost by import after all writing are finished,
			// so here we use estimatedWritingProgress * 0.8 + estimatedImportingProgress * 0.2 as the total
			// progress.
			remaining = zap.Skip()
			totalPercent := 0.0
			if finished > 0 {
				writePercent := math.Min(finished/estimated, 1.0)
				importPercent := 1.0
				if bytesWritten > 0 {
					totalBytes := bytesWritten / writePercent
					importPercent = math.Min(bytesImported/totalBytes, 1.0)
				}
				totalPercent = writePercent*0.8 + importPercent*0.2
				if totalPercent < 1.0 {
					remainNanoseconds := (1.0 - totalPercent) / totalPercent * nanoseconds
					remaining = zap.Duration("remaining", time.Duration(remainNanoseconds).Round(time.Second))
				}
			}

			formatPercent := func(finish, estimate float64) string {
				speed := ""
				if estimated > 0 {
					speed = fmt.Sprintf(" (%.1f%%)", finish/estimate*100)
				}
				return speed
			}

			// avoid output bytes speed if there are no unfinished chunks
			chunkSpeed := zap.Skip()
			if bytesRead > 0 {
				chunkSpeed = zap.Float64("speed(MiB/s)", bytesRead/(1048576e-9*nanoseconds))
			}

			// Note: a speed of 28 MiB/s roughly corresponds to 100 GiB/hour.
			log.L().Info("progress",
				zap.String("total", fmt.Sprintf("%.1f%%", totalPercent*100)),
				//zap.String("files", fmt.Sprintf("%.0f/%.0f (%.1f%%)", finished, estimated, finished/estimated*100)),
				zap.String("tables", fmt.Sprintf("%.0f/%.0f%s", completedTables, totalTables, formatPercent(completedTables, totalTables))),
				zap.String("chunks", fmt.Sprintf("%.0f/%.0f%s", finished, estimated, formatPercent(finished, estimated))),
				zap.String("engines", fmt.Sprintf("%.f/%.f%s", engineFinished, engineEstimated, formatPercent(engineFinished, engineEstimated))),
				chunkSpeed,
				zap.String("state", state),
				remaining,
			)

		case <-checkQuotaChan:
			// verify the total space occupied by sorted-kv-dir is below the quota,
			// otherwise we perform an emergency import.
			rc.enforceDiskQuota(ctx)

		case <-glueProgressTicker.C:
			finished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
			rc.tidbGlue.Record(glue.RecordFinishedChunk, uint64(finished))
		}
	}
}

var checksumManagerKey struct{}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	logTask := log.L().Begin(zap.InfoLevel, "restore all tables data")

	// for local backend, we should disable some pd scheduler and change some settings, to
	// make split region and ingest sst more stable
	// because importer backend is mostly use for v3.x cluster which doesn't support these api,
	// so we also don't do this for import backend
	if rc.cfg.TikvImporter.Backend == config.BackendLocal {
		// disable some pd schedulers
		pdController, err := pdutil.NewPdController(ctx, rc.cfg.TiDB.PdAddr,
			rc.tls.TLSConfig(), rc.tls.ToPDSecurityOption())
		if err != nil {
			return errors.Trace(err)
		}
		logTask.Info("removing PD leader&region schedulers")
		restoreFn, e := pdController.RemoveSchedulers(ctx)
		defer func() {
			// use context.Background to make sure this restore function can still be executed even if ctx is canceled
			if restoreE := restoreFn(context.Background()); restoreE != nil {
				logTask.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
				return
			}
			logTask.Info("add back PD leader&region schedulers")
		}()
		if e != nil {
			return errors.Trace(err)
		}
	}

	type task struct {
		tr *TableRestore
		cp *TableCheckpoint
	}

	totalTables := 0
	for _, dbMeta := range rc.dbMetas {
		totalTables += len(dbMeta.Tables)
	}
	postProcessTaskChan := make(chan task, totalTables)

	var wg sync.WaitGroup
	var restoreErr common.OnceError

	stopPeriodicActions := make(chan struct{})
	go rc.runPeriodicActions(ctx, stopPeriodicActions)
	defer close(stopPeriodicActions)

	taskCh := make(chan task, rc.cfg.App.IndexConcurrency)
	defer close(taskCh)

	manager, err := newChecksumManager(ctx, rc)
	if err != nil {
		return errors.Trace(err)
	}
	ctx2 := context.WithValue(ctx, &checksumManagerKey, manager)
	for i := 0; i < rc.cfg.App.IndexConcurrency; i++ {
		go func() {
			for task := range taskCh {
				tableLogTask := task.tr.logger.Begin(zap.InfoLevel, "restore table")
				web.BroadcastTableCheckpoint(task.tr.tableName, task.cp)
				needPostProcess, err := task.tr.restoreTable(ctx2, rc, task.cp)
				err = errors.Annotatef(err, "restore table %s failed", task.tr.tableName)
				tableLogTask.End(zap.ErrorLevel, err)
				web.BroadcastError(task.tr.tableName, err)
				metric.RecordTableCount("completed", err)
				restoreErr.Set(err)
				if needPostProcess {
					postProcessTaskChan <- task
				}
				wg.Done()
			}
		}()
	}

	// first collect all tables where the checkpoint is invalid
	allInvalidCheckpoints := make(map[string]CheckpointStatus)
	// collect all tables whose checkpoint's tableID can't match current tableID
	allDirtyCheckpoints := make(map[string]struct{})
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
			if err != nil {
				return errors.Trace(err)
			}
			if cp.Status <= CheckpointStatusMaxInvalid {
				allInvalidCheckpoints[tableName] = cp.Status
			} else if cp.TableID > 0 && cp.TableID != tableInfo.ID {
				allDirtyCheckpoints[tableName] = struct{}{}
			}
		}
	}

	if len(allInvalidCheckpoints) != 0 {
		logger := log.L()
		logger.Error(
			"TiDB Lightning has failed last time. To prevent data loss, this run will stop now. Please resolve errors first",
			zap.Int("count", len(allInvalidCheckpoints)),
		)

		for tableName, status := range allInvalidCheckpoints {
			failedStep := status * 10
			var action strings.Builder
			action.WriteString("./tidb-lightning-ctl --checkpoint-error-")
			switch failedStep {
			case CheckpointStatusAlteredAutoInc, CheckpointStatusAnalyzed:
				action.WriteString("ignore")
			default:
				action.WriteString("destroy")
			}
			action.WriteString("='")
			action.WriteString(tableName)
			action.WriteString("' --config=...")

			logger.Info("-",
				zap.String("table", tableName),
				zap.Uint8("status", uint8(status)),
				zap.String("failedStep", failedStep.MetricName()),
				zap.Stringer("recommendedAction", &action),
			)
		}

		logger.Info("You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch")
		logger.Info("For details of this failure, read the log file from the PREVIOUS run")

		return errors.New("TiDB Lightning has failed last time; please resolve these errors first")
	}
	if len(allDirtyCheckpoints) > 0 {
		logger := log.L()
		logger.Error(
			"TiDB Lightning has detected tables with illegal checkpoints. To prevent data mismatch, this run will stop now. Please remove these checkpoints first",
			zap.Int("count", len(allDirtyCheckpoints)),
		)

		for tableName := range allDirtyCheckpoints {
			logger.Info("-",
				zap.String("table", tableName),
				zap.String("recommendedAction", "./tidb-lightning-ctl --checkpoint-remove='"+tableName+"' --config=..."),
			)
		}

		logger.Info("You may also run `./tidb-lightning-ctl --checkpoint-remove=all --config=...` to start from scratch")

		return errors.New("TiDB Lightning has detected tables with illegal checkpoints; please remove these checkpoints first")
	}

	for _, dbMeta := range rc.dbMetas {
		dbInfo := rc.dbInfos[dbMeta.Name]
		for _, tableMeta := range dbMeta.Tables {
			tableInfo := dbInfo.Tables[tableMeta.Name]
			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
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
	// if context is done, should return directly
	select {
	case <-ctx.Done():
		err = restoreErr.Get()
		if err == nil {
			err = ctx.Err()
		}
		logTask.End(zap.ErrorLevel, err)
		return err
	default:
	}

	close(postProcessTaskChan)
	// otherwise, we should run all tasks in the post-process task chan
	for i := 0; i < rc.cfg.App.TableConcurrency; i++ {
		wg.Add(1)
		go func() {
			for task := range postProcessTaskChan {
				// force all the remain post-process tasks to be executed
				_, err := task.tr.postProcess(ctx2, rc, task.cp, true)
				restoreErr.Set(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	err = restoreErr.Get()
	logTask.End(zap.ErrorLevel, err)
	return err
}

func (t *TableRestore) restoreTable(
	ctx context.Context,
	rc *RestoreController,
	cp *TableCheckpoint,
) (bool, error) {
	// 1. Load the table info.

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	// no need to do anything if the chunks are already populated
	if len(cp.Engines) > 0 {
		t.logger.Info("reusing engines and files info from checkpoint",
			zap.Int("enginesCnt", len(cp.Engines)),
			zap.Int("filesCnt", cp.CountChunks()),
		)
	} else if cp.Status < CheckpointStatusAllWritten {
		if err := t.populateChunks(ctx, rc, cp); err != nil {
			return false, errors.Trace(err)
		}
		if err := rc.checkpointsDB.InsertEngineCheckpoints(ctx, t.tableName, cp.Engines); err != nil {
			return false, errors.Trace(err)
		}
		web.BroadcastTableCheckpoint(t.tableName, cp)

		// rebase the allocator so it exceeds the number of rows.
		if t.tableInfo.Core.PKIsHandle && t.tableInfo.Core.ContainsAutoRandomBits() {
			cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, t.tableInfo.Core.AutoRandID)
			t.alloc.Get(autoid.AutoRandomType).Rebase(t.tableInfo.ID, cp.AllocBase, false)
		} else {
			cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, t.tableInfo.Core.AutoIncID)
			t.alloc.Get(autoid.RowIDAllocType).Rebase(t.tableInfo.ID, cp.AllocBase, false)
		}
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
		return false, errors.Trace(err)
	}

	// 3. Post-process. With the last parameter set to false, we can allow delay analyze execute latter
	return t.postProcess(ctx, rc, cp, false /* force-analyze */)
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

		indexEngine, err := rc.backend.OpenEngine(ctx, t.tableName, indexEngineID)
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
				// Set engineErr and break this for loop to wait all the sub-routines done before return.
				// Directly return may cause panic because caller will close the pebble db but some sub routines
				// are still reading from or writing to the pebble db.
				engineErr.Set(ctx.Err())
			default:
			}
			if engineErr.Get() != nil {
				break
			}

			// Should skip index engine
			if engineID < 0 {
				continue
			}

			if engine.Status < CheckpointStatusImported {
				wg.Add(1)

				// Note: We still need tableWorkers to control the concurrency of tables.
				// In the future, we will investigate more about
				// the difference between restoring tables concurrently and restoring tables one by one.
				restoreWorker := rc.tableWorkers.Apply()

				go func(w *worker.Worker, eid int32, ecp *EngineCheckpoint) {
					defer wg.Done()

					engineLogTask := t.logger.With(zap.Int32("engineNumber", eid)).Begin(zap.InfoLevel, "restore engine")
					dataClosedEngine, err := t.restoreEngine(ctx, rc, indexEngine, eid, ecp)
					engineLogTask.End(zap.ErrorLevel, err)
					rc.tableWorkers.Recycle(w)
					if err != nil {
						engineErr.Set(err)
						return
					}

					failpoint.Inject("FailBeforeDataEngineImported", func() {
						panic("forcing failure due to FailBeforeDataEngineImported")
					})

					dataWorker := rc.closedEngineLimit.Apply()
					defer rc.closedEngineLimit.Recycle(dataWorker)
					if err := t.importEngine(ctx, dataClosedEngine, rc, eid, ecp); err != nil {
						engineErr.Set(err)
					}
				}(restoreWorker, engineID, engine)
			}
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
			closedIndexEngine, err = rc.backend.UnsafeCloseEngine(ctx, t.tableName, indexEngineID)
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
			if !rc.isLocalBackend() {
				rc.postProcessLock.Lock()
			}
			err = t.importKV(ctx, closedIndexEngine, rc, indexEngineID)
			rc.saveStatusCheckpoint(t.tableName, indexEngineID, err, CheckpointStatusImported)
			if !rc.isLocalBackend() {
				rc.postProcessLock.Unlock()
			}
		}

		failpoint.Inject("FailBeforeIndexEngineImported", func() {
			panic("forcing failure due to FailBeforeIndexEngineImported")
		})

		rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, err, CheckpointStatusIndexImported)
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
) (*kv.ClosedEngine, error) {
	if cp.Status >= CheckpointStatusClosed {
		closedEngine, err := rc.backend.UnsafeCloseEngine(ctx, t.tableName, engineID)
		// If any error occurred, recycle worker immediately
		if err != nil {
			return closedEngine, errors.Trace(err)
		}
		return closedEngine, nil
	}

	// In Local backend, the local writer will produce an SST file for batch
	// ingest into the local DB every 1000 KV pairs or up to 512 MiB.
	// There are (region-concurrency) data writers, and (index-concurrency) index writers.
	// Thus, the disk size occupied by these writers are up to
	// (region-concurrency + index-concurrency) * 512 MiB.
	// This number should not exceed the disk quota.
	// Therefore, we need to reduce that "512 MiB" to respect the disk quota:
	localWriterMaxCacheSize := int64(rc.cfg.TikvImporter.DiskQuota) // int64(rc.cfg.App.IndexConcurrency+rc.cfg.App.RegionConcurrency)
	if localWriterMaxCacheSize > config.LocalMemoryTableSize {
		localWriterMaxCacheSize = config.LocalMemoryTableSize
	}

	indexWriter, err := indexEngine.LocalWriter(ctx, localWriterMaxCacheSize)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logTask := t.logger.With(zap.Int32("engineNumber", engineID)).Begin(zap.InfoLevel, "encode kv data and write")

	dataEngine, err := rc.backend.OpenEngine(ctx, t.tableName, engineID)
	if err != nil {
		return nil, errors.Trace(err)
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
			return nil, ctx.Err()
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
		cr, err := newChunkRestore(ctx, chunkIndex, rc.cfg, chunk, rc.ioWorkers, rc.store, t.tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var remainChunkCnt float64
		if chunk.Chunk.Offset < chunk.Chunk.EndOffset {
			remainChunkCnt = float64(chunk.Chunk.EndOffset-chunk.Chunk.Offset) / float64(chunk.Chunk.EndOffset-chunk.Key.Offset)
			metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending).Add(remainChunkCnt)
		}

		restoreWorker := rc.regionWorkers.Apply()
		wg.Add(1)
		dataWriter, err := dataEngine.LocalWriter(ctx, localWriterMaxCacheSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		go func(w *worker.Worker, cr *chunkRestore) {
			// Restore a chunk.
			defer func() {
				cr.close()
				wg.Done()
				rc.regionWorkers.Recycle(w)
			}()
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateRunning).Add(remainChunkCnt)
			err := cr.restore(ctx, t, engineID, dataWriter, indexWriter, rc)
			if err == nil {
				err = dataWriter.Close()
			}
			if err == nil {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Add(remainChunkCnt)
				metric.BytesCounter.WithLabelValues(metric.TableStateWritten).Add(float64(cr.chunk.Checksum.SumSize()))
			} else {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Add(remainChunkCnt)
				chunkErr.Set(err)
			}
		}(restoreWorker, cr)
	}

	wg.Wait()
	if err := indexWriter.Close(); err != nil {
		return nil, errors.Trace(err)
	}

	// Report some statistics into the log for debugging.
	totalKVSize := uint64(0)
	totalSQLSize := int64(0)
	for _, chunk := range cp.Chunks {
		totalKVSize += chunk.Checksum.SumSize()
		totalSQLSize += chunk.Chunk.EndOffset - chunk.Chunk.Offset
	}

	err = chunkErr.Get()
	logTask.End(zap.ErrorLevel, err,
		zap.Int64("read", totalSQLSize),
		zap.Uint64("written", totalKVSize),
	)

	flushAndSaveAllChunks := func() error {
		if err = indexEngine.Flush(ctx); err != nil {
			return errors.Trace(err)
		}
		// Currently we write all the checkpoints after data&index engine are flushed.
		for _, chunk := range cp.Chunks {
			saveCheckpoint(rc, t, engineID, chunk)
		}
		return nil
	}

	// in local mode, this check-point make no sense, because we don't do flush now,
	// so there may be data lose if exit at here. So we don't write this checkpoint
	// here like other mode.
	if !rc.isLocalBackend() {
		rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusAllWritten)
	}
	if err != nil {
		// if process is canceled, we should flush all chunk checkpoints for local backend
		if rc.isLocalBackend() && common.IsContextCanceledError(err) {
			// ctx is canceled, so to avoid Close engine failed, we use `context.Background()` here
			if _, err2 := dataEngine.Close(context.Background()); err2 != nil {
				log.L().Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
				return nil, errors.Trace(err)
			}
			if err2 := flushAndSaveAllChunks(); err2 != nil {
				log.L().Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
			}
		}
		return nil, errors.Trace(err)
	}

	closedDataEngine, err := dataEngine.Close(ctx)
	// For local backend, if checkpoint is enabled, we must flush index engine to avoid data loss.
	// this flush action impact up to 10% of the performance, so we only do it if necessary.
	if err == nil && rc.cfg.Checkpoint.Enable && rc.isLocalBackend() {
		if err = flushAndSaveAllChunks(); err != nil {
			return nil, errors.Trace(err)
		}

		// Currently we write all the checkpoints after data&index engine are flushed.
		for _, chunk := range cp.Chunks {
			saveCheckpoint(rc, t, engineID, chunk)
		}
	}
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusClosed)
	if err != nil {
		// If any error occurred, recycle worker immediately
		return nil, errors.Trace(err)
	}
	return closedDataEngine, nil
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
	if !rc.isLocalBackend() {
		rc.postProcessLock.Lock()
	}
	err := t.importKV(ctx, closedEngine, rc, engineID)
	rc.saveStatusCheckpoint(t.tableName, engineID, err, CheckpointStatusImported)
	if !rc.isLocalBackend() {
		rc.postProcessLock.Unlock()
	}
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

// postProcess execute rebase-auto-id/checksum/analyze according to the task config.
//
// if the parameter forcePostProcess to true, postProcess force run checksum and analyze even if the
// post-process-at-last config is true. And if this two phases are skipped, the first return value will be true.
func (t *TableRestore) postProcess(
	ctx context.Context,
	rc *RestoreController,
	cp *TableCheckpoint,
	forcePostProcess bool,
) (bool, error) {
	// there are no data in this table, no need to do post process
	// this is important for tables that are just the dump table of views
	// because at this stage, the table was already deleted and replaced by the related view
	if len(cp.Engines) == 1 {
		return false, nil
	}

	// 3. alter table set auto_increment
	if cp.Status < CheckpointStatusAlteredAutoInc {
		rc.alterTableLock.Lock()
		tblInfo := t.tableInfo.Core
		var err error
		if tblInfo.PKIsHandle && tblInfo.ContainsAutoRandomBits() {
			err = AlterAutoRandom(ctx, rc.tidbGlue.GetSQLExecutor(), t.tableName, t.alloc.Get(autoid.AutoRandomType).Base()+1)
		} else if common.TableHasAutoRowID(tblInfo) || tblInfo.GetAutoIncrementColInfo() != nil {
			// only alter auto increment id iff table contains auto-increment column or generated handle
			err = AlterAutoIncrement(ctx, rc.tidbGlue.GetSQLExecutor(), t.tableName, t.alloc.Get(autoid.RowIDAllocType).Base()+1)
		}
		rc.alterTableLock.Unlock()
		rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, err, CheckpointStatusAlteredAutoInc)
		if err != nil {
			return false, err
		}
		cp.Status = CheckpointStatusAlteredAutoInc
	}

	// tidb backend don't need checksum & analyze
	if !rc.backend.ShouldPostProcess() {
		t.logger.Debug("skip checksum & analyze, not supported by this backend")
		rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, nil, CheckpointStatusAnalyzeSkipped)
		return false, nil
	}

	w := rc.checksumWorks.Apply()
	defer rc.checksumWorks.Recycle(w)

	finished := true
	if cp.Status < CheckpointStatusChecksummed {
		if rc.cfg.PostRestore.Checksum == config.OpLevelOff {
			t.logger.Info("skip checksum")
			rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, nil, CheckpointStatusChecksumSkipped)
		} else {
			if forcePostProcess || !rc.cfg.PostRestore.PostProcessAtLast {
				// 4. do table checksum
				var localChecksum verify.KVChecksum
				for _, engine := range cp.Engines {
					for _, chunk := range engine.Chunks {
						localChecksum.Add(&chunk.Checksum)
					}
				}
				t.logger.Info("local checksum", zap.Object("checksum", &localChecksum))
				err := t.compareChecksum(ctx, localChecksum)
				// with post restore level 'optional', we will skip checksum error
				if rc.cfg.PostRestore.Checksum == config.OpLevelOptional {
					if err != nil {
						t.logger.Warn("compare checksum failed, will skip this error and go on", log.ShortError(err))
						err = nil
					}
				}
				rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, err, CheckpointStatusChecksummed)
				if err != nil {
					return false, errors.Trace(err)
				}
				cp.Status = CheckpointStatusChecksummed
			} else {
				finished = false
			}

		}
	}
	if !finished {
		return !finished, nil
	}

	// 5. do table analyze
	if cp.Status < CheckpointStatusAnalyzed {
		if rc.cfg.PostRestore.Analyze == config.OpLevelOff {
			t.logger.Info("skip analyze")
			rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, nil, CheckpointStatusAnalyzeSkipped)
			cp.Status = CheckpointStatusAnalyzed
		} else if forcePostProcess || !rc.cfg.PostRestore.PostProcessAtLast {
			err := t.analyzeTable(ctx, rc.tidbGlue.GetSQLExecutor())
			// witch post restore level 'optional', we will skip analyze error
			if rc.cfg.PostRestore.Analyze == config.OpLevelOptional {
				if err != nil {
					t.logger.Warn("analyze table failed, will skip this error and go on", log.ShortError(err))
					err = nil
				}
			}
			rc.saveStatusCheckpoint(t.tableName, WholeTableEngineID, err, CheckpointStatusAnalyzed)
			if err != nil {
				return false, errors.Trace(err)
			}
			cp.Status = CheckpointStatusAnalyzed
		} else {
			finished = false
		}
	}

	return !finished, nil
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
	tls := rc.tls.WithHost(rc.cfg.TiDB.PdAddr)
	return kv.ForAllStores(
		ctx,
		tls,
		kv.StoreStateDisconnected,
		func(c context.Context, store *kv.Store) error {
			return kv.Compact(c, tls, store.Address, level)
		},
	)
}

func (rc *RestoreController) switchToImportMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import)
}

func (rc *RestoreController) switchToNormalMode(ctx context.Context) error {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal)
	return nil
}

func (rc *RestoreController) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) {
	// It is fine if we miss some stores which did not switch to Import mode,
	// since we're running it periodically, so we exclude disconnected stores.
	// But it is essential all stores be switched back to Normal mode to allow
	// normal operation.
	var minState kv.StoreState
	if mode == sstpb.SwitchMode_Import {
		minState = kv.StoreStateOffline
	} else {
		minState = kv.StoreStateDisconnected
	}
	tls := rc.tls.WithHost(rc.cfg.TiDB.PdAddr)
	// we ignore switch mode failure since it is not fatal.
	// no need log the error, it is done in kv.SwitchMode already.
	_ = kv.ForAllStores(
		ctx,
		tls,
		minState,
		func(c context.Context, store *kv.Store) error {
			return kv.SwitchMode(c, tls, store.Address, mode)
		},
	)
}

func (rc *RestoreController) enforceDiskQuota(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&rc.diskQuotaState, diskQuotaStateIdle, diskQuotaStateChecking) {
		// do not run multiple the disk quota check / import simultaneously.
		// (we execute the lock check in background to avoid blocking the cron thread)
		return
	}

	go func() {
		// locker is assigned when we detect the disk quota is exceeded.
		// before the disk quota is confirmed exceeded, we keep the diskQuotaLock
		// unlocked to avoid periodically interrupting the writer threads.
		var locker sync.Locker
		defer func() {
			atomic.StoreInt32(&rc.diskQuotaState, diskQuotaStateIdle)
			if locker != nil {
				locker.Unlock()
			}
		}()

		isRetrying := false

		for {
			// sleep for a cycle if we are retrying because there is nothing new to import.
			if isRetrying {
				select {
				case <-ctx.Done():
					return
				case <-time.After(rc.cfg.Cron.CheckDiskQuota.Duration):
				}
			} else {
				isRetrying = true
			}

			quota := int64(rc.cfg.TikvImporter.DiskQuota)
			largeEngines, inProgressLargeEngines, totalDiskSize, totalMemSize := rc.backend.CheckDiskQuota(quota)
			metric.LocalStorageUsageBytesGauge.WithLabelValues("disk").Set(float64(totalDiskSize))
			metric.LocalStorageUsageBytesGauge.WithLabelValues("mem").Set(float64(totalMemSize))

			logger := log.With(
				zap.Int64("diskSize", totalDiskSize),
				zap.Int64("memSize", totalMemSize),
				zap.Int64("quota", quota),
				zap.Int("largeEnginesCount", len(largeEngines)),
				zap.Int("inProgressLargeEnginesCount", inProgressLargeEngines))

			if len(largeEngines) == 0 && inProgressLargeEngines == 0 {
				logger.Debug("disk quota respected")
				return
			}

			if locker == nil {
				// blocks all writers when we detected disk quota being exceeded.
				rc.diskQuotaLock.Lock()
				locker = &rc.diskQuotaLock
			}

			logger.Warn("disk quota exceeded")
			if len(largeEngines) == 0 {
				logger.Warn("all large engines are already importing, keep blocking all writes")
				continue
			}

			// flush all engines so that checkpoints can be updated.
			if err := rc.backend.FlushAll(ctx); err != nil {
				logger.Error("flush engine for disk quota failed, check again later", log.ShortError(err))
				return
			}

			// at this point, all engines are synchronized on disk.
			// we then import every large engines one by one and complete.
			// if any engine failed to import, we just try again next time, since the data are still intact.
			atomic.StoreInt32(&rc.diskQuotaState, diskQuotaStateImporting)
			task := logger.Begin(zap.WarnLevel, "importing large engines for disk quota")
			var importErr error
			for _, engine := range largeEngines {
				if err := rc.backend.UnsafeImportAndReset(ctx, engine); err != nil {
					importErr = multierr.Append(importErr, err)
				}
			}
			task.End(zap.ErrorLevel, importErr)
			return
		}
	}()
}

func (rc *RestoreController) checkRequirements(ctx context.Context) error {
	// skip requirement check if explicitly turned off
	if !rc.cfg.App.CheckRequirements {
		return nil
	}
	return rc.backend.CheckRequirements(ctx)
}

func (rc *RestoreController) setGlobalVariables(ctx context.Context) error {
	// set new collation flag base on tidb config
	enabled := ObtainNewCollationEnabled(ctx, rc.tidbGlue.GetSQLExecutor())
	// we should enable/disable new collation here since in server mode, tidb config
	// may be different in different tasks
	collate.SetNewCollationEnabledForTest(enabled)
	return nil
}

func (rc *RestoreController) waitCheckpointFinish() {
	// wait checkpoint process finish so that we can do cleanup safely
	close(rc.saveCpCh)
	rc.checkpointsWg.Wait()
}

func (rc *RestoreController) cleanCheckpoints(ctx context.Context) error {
	rc.waitCheckpointFinish()

	if !rc.cfg.Checkpoint.Enable {
		return nil
	}

	logger := log.With(
		zap.Bool("keepAfterSuccess", rc.cfg.Checkpoint.KeepAfterSuccess),
		zap.Int64("taskID", rc.cfg.TaskID),
	)

	task := logger.Begin(zap.InfoLevel, "clean checkpoints")
	var err error
	if rc.cfg.Checkpoint.KeepAfterSuccess {
		err = rc.checkpointsDB.MoveCheckpoints(ctx, rc.cfg.TaskID)
	} else {
		err = rc.checkpointsDB.RemoveCheckpoint(ctx, "all")
	}
	task.End(zap.ErrorLevel, err)
	return errors.Annotate(err, "clean checkpoints")
}

func (rc *RestoreController) isLocalBackend() bool {
	return rc.cfg.TikvImporter.Backend == "local"
}

type chunkRestore struct {
	parser mydump.Parser
	index  int
	chunk  *ChunkCheckpoint
}

func newChunkRestore(
	ctx context.Context,
	index int,
	cfg *config.Config,
	chunk *ChunkCheckpoint,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	tableInfo *TidbTableInfo,
) (*chunkRestore, error) {
	blockBufSize := int64(cfg.Mydumper.ReadBlockSize)

	var reader storage.ReadSeekCloser
	var err error
	if chunk.FileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, store, chunk.FileMeta.Path, chunk.FileMeta.FileSize)
	} else {
		reader, err = store.Open(ctx, chunk.FileMeta.Path)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	var parser mydump.Parser
	switch chunk.FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := cfg.Mydumper.CSV.Header && chunk.Chunk.Offset == 0
		parser = mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", chunk.Key.Path, chunk.FileMeta.Type.String()))
	}

	if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
		return nil, errors.Trace(err)
	}
	if len(chunk.ColumnPermutation) > 0 {
		parser.SetColumns(getColumnNames(tableInfo.Core, chunk.ColumnPermutation))
	}

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
	alloc     autoid.Allocators
	logger    log.Logger
}

func NewTableRestore(
	tableName string,
	tableMeta *mydump.MDTableMeta,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	cp *TableCheckpoint,
) (*TableRestore, error) {
	idAlloc := kv.NewPanickingAllocators(cp.AllocBase)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo.Core)
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

func (t *TableRestore) populateChunks(ctx context.Context, rc *RestoreController, cp *TableCheckpoint) error {
	task := t.logger.Begin(zap.InfoLevel, "load engines and files")
	chunks, err := mydump.MakeTableRegions(ctx, t.tableMeta, len(t.tableInfo.Core.Columns), rc.cfg, rc.ioWorkers, rc.store)
	if err == nil {
		timestamp := time.Now().Unix()
		failpoint.Inject("PopulateChunkTimestamp", func(v failpoint.Value) {
			timestamp = int64(v.(int))
		})
		for _, chunk := range chunks {
			engine, found := cp.Engines[chunk.EngineID]
			if !found {
				engine = &EngineCheckpoint{
					Status: CheckpointStatusLoaded,
				}
				cp.Engines[chunk.EngineID] = engine
			}
			ccp := &ChunkCheckpoint{
				Key: ChunkCheckpointKey{
					Path:   chunk.FileMeta.Path,
					Offset: chunk.Chunk.Offset,
				},
				FileMeta:          chunk.FileMeta,
				ColumnPermutation: nil,
				Chunk:             chunk.Chunk,
				Timestamp:         timestamp,
			}
			if len(chunk.Chunk.Columns) > 0 {
				perms, err := t.parseColumnPermutations(chunk.Chunk.Columns)
				if err != nil {
					return errors.Trace(err)
				}
				ccp.ColumnPermutation = perms
			}
			engine.Chunks = append(engine.Chunks, ccp)
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
//
// The argument `columns` _must_ be in lower case.
func (t *TableRestore) initializeColumns(columns []string, ccp *ChunkCheckpoint) error {
	var colPerm []int
	if len(columns) == 0 {
		colPerm = make([]int, 0, len(t.tableInfo.Core.Columns)+1)
		shouldIncludeRowID := common.TableHasAutoRowID(t.tableInfo.Core)

		// no provided columns, so use identity permutation.
		for i := range t.tableInfo.Core.Columns {
			colPerm = append(colPerm, i)
		}
		if shouldIncludeRowID {
			colPerm = append(colPerm, -1)
		}
	} else {
		var err error
		colPerm, err = t.parseColumnPermutations(columns)
		if err != nil {
			return errors.Trace(err)
		}
	}

	ccp.ColumnPermutation = colPerm
	return nil
}

func (t *TableRestore) parseColumnPermutations(columns []string) ([]int, error) {
	colPerm := make([]int, 0, len(t.tableInfo.Core.Columns)+1)

	columnMap := make(map[string]int)
	for i, column := range columns {
		columnMap[column] = i
	}

	tableColumnMap := make(map[string]int)
	for i, col := range t.tableInfo.Core.Columns {
		tableColumnMap[col.Name.L] = i
	}

	// check if there are some unknown columns
	var unknownCols []string
	for _, c := range columns {
		if _, ok := tableColumnMap[c]; !ok && c != model.ExtraHandleName.L {
			unknownCols = append(unknownCols, c)
		}
	}
	if len(unknownCols) > 0 {
		return colPerm, errors.Errorf("unknown columns in header %s", unknownCols)
	}

	for _, colInfo := range t.tableInfo.Core.Columns {
		if i, ok := columnMap[colInfo.Name.L]; ok {
			colPerm = append(colPerm, i)
		} else {
			if len(colInfo.GeneratedExprString) == 0 {
				t.logger.Warn("column missing from data file, going to fill with default value",
					zap.String("colName", colInfo.Name.O),
					zap.Stringer("colType", &colInfo.FieldType),
				)
			}
			colPerm = append(colPerm, -1)
		}
	}
	if i, ok := columnMap[model.ExtraHandleName.L]; ok {
		colPerm = append(colPerm, i)
	} else if common.TableHasAutoRowID(t.tableInfo.Core) {
		colPerm = append(colPerm, -1)
	}

	return colPerm, nil
}

func getColumnNames(tableInfo *model.TableInfo, permutation []int) []string {
	colIndexes := make([]int, 0, len(permutation))
	for i := 0; i < len(permutation); i++ {
		colIndexes = append(colIndexes, -1)
	}
	colCnt := 0
	for i, p := range permutation {
		if p >= 0 {
			colIndexes[p] = i
			colCnt++
		}
	}

	names := make([]string, 0, colCnt)
	for _, idx := range colIndexes {
		// skip columns with index -1
		if idx >= 0 {
			// original fiels contains _tidb_rowid field
			if idx == len(tableInfo.Columns) {
				names = append(names, model.ExtraHandleName.O)
			} else {
				names = append(names, tableInfo.Columns[idx].Name.O)
			}
		}
	}
	return names
}

func (tr *TableRestore) importKV(
	ctx context.Context,
	closedEngine *kv.ClosedEngine,
	rc *RestoreController,
	engineID int32,
) error {
	task := closedEngine.Logger().Begin(zap.InfoLevel, "import and cleanup engine")

	err := closedEngine.Import(ctx)
	rc.saveStatusCheckpoint(tr.tableName, engineID, err, CheckpointStatusImported)
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
func (tr *TableRestore) compareChecksum(ctx context.Context, localChecksum verify.KVChecksum) error {
	remoteChecksum, err := DoChecksum(ctx, tr.tableInfo)
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

func (tr *TableRestore) analyzeTable(ctx context.Context, g glue.SQLExecutor) error {
	task := tr.logger.Begin(zap.InfoLevel, "analyze")
	err := g.ExecuteWithLog(ctx, "ANALYZE TABLE "+tr.tableName, "analyze table", tr.logger)
	task.End(zap.ErrorLevel, err)
	return err
}

////////////////////////////////////////////////////////////////

var (
	maxKVQueueSize         = 128   // Cache at most this number of rows before blocking the encode loop
	minDeliverBytes uint64 = 65536 // 64 KB. batch at least this amount of bytes to reduce number of messages
)

type deliveredKVs struct {
	kvs     kv.Row // if kvs is nil, this indicated we've got the last message.
	columns []string
	offset  int64
	rowID   int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}

func (cr *chunkRestore) deliverLoop(
	ctx context.Context,
	kvsCh <-chan []deliveredKVs,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *kv.LocalEngineWriter,
	rc *RestoreController,
) (deliverTotalDur time.Duration, err error) {
	var channelClosed bool

	deliverLogger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
		zap.String("task", "deliver"),
	)

	for !channelClosed {
		var dataChecksum, indexChecksum verify.KVChecksum
		var columns []string
		var kvPacket []deliveredKVs
		// init these two field as checkpoint current value, so even if there are no kv pairs delivered,
		// chunk checkpoint should stay the same
		offset := cr.chunk.Chunk.Offset
		rowID := cr.chunk.Chunk.PrevRowIDMax
		// Fetch enough KV pairs from the source.
		dataKVs := rc.backend.MakeEmptyRows()
		indexKVs := rc.backend.MakeEmptyRows()

	populate:
		for dataChecksum.SumSize() < minDeliverBytes {
			select {
			case kvPacket = <-kvsCh:
				if len(kvPacket) == 0 {
					channelClosed = true
					break populate
				}
				for _, p := range kvPacket {
					p.kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
					columns = p.columns
					offset = p.offset
					rowID = p.rowID
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}

		// we are allowed to save checkpoint when the disk quota state moved to "importing"
		// since all engines are flushed.
		if atomic.LoadInt32(&rc.diskQuotaState) == diskQuotaStateImporting {
			saveCheckpoint(rc, t, engineID, cr.chunk)
		}

		func() {
			rc.diskQuotaLock.RLock()
			defer rc.diskQuotaLock.RUnlock()

			// Write KVs into the engine
			start := time.Now()

			if err = dataEngine.WriteRows(ctx, columns, dataKVs); err != nil {
				deliverLogger.Error("write to data engine failed", log.ShortError(err))
				return
			}
			if err = indexEngine.WriteRows(ctx, columns, indexKVs); err != nil {
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
		}()

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		// No need to apply a lock since this is the only thread updating `cr.chunk.**`.
		// In local mode, we should write these checkpoint after engine flushed.
		cr.chunk.Checksum.Add(&dataChecksum)
		cr.chunk.Checksum.Add(&indexChecksum)
		cr.chunk.Chunk.Offset = offset
		cr.chunk.Chunk.PrevRowIDMax = rowID
		if !rc.isLocalBackend() && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
			// No need to save checkpoint if nothing was delivered.
			saveCheckpoint(rc, t, engineID, cr.chunk)
		}
		failpoint.Inject("SlowDownWriteRows", func() {
			deliverLogger.Warn("Slowed down write rows")
		})
		failpoint.Inject("FailAfterWriteRows", nil)
		// TODO: for local backend, we may save checkpoint more frequently, e.g. after writen
		// 10GB kv pairs to data engine, we can do a flush for both data & index engine, then we
		// can safely update current checkpoint.

		failpoint.Inject("LocalBackendSaveCheckpoint", func() {
			if !rc.isLocalBackend() && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
				// No need to save checkpoint if nothing was delivered.
				saveCheckpoint(rc, t, engineID, cr.chunk)
			}
		})
	}

	return
}

func saveCheckpoint(rc *RestoreController, t *TableRestore, engineID int32, chunk *ChunkCheckpoint) {
	// We need to update the AllocBase every time we've finished a file.
	// The AllocBase is determined by the maximum of the "handle" (_tidb_rowid
	// or integer primary key), which can only be obtained by reading all data.

	var base int64
	if t.tableInfo.Core.PKIsHandle && t.tableInfo.Core.ContainsAutoRandomBits() {
		base = t.alloc.Get(autoid.AutoRandomType).Base() + 1
	} else {
		base = t.alloc.Get(autoid.RowIDAllocType).Base() + 1
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &RebaseCheckpointMerger{
			AllocBase: base,
		},
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &ChunkCheckpointMerger{
			EngineID:          engineID,
			Key:               chunk.Key,
			Checksum:          chunk.Checksum,
			Pos:               chunk.Chunk.Offset,
			RowID:             chunk.Chunk.PrevRowIDMax,
			ColumnPermutation: chunk.ColumnPermutation,
		},
	}
}

func (cr *chunkRestore) encodeLoop(
	ctx context.Context,
	kvsCh chan<- []deliveredKVs,
	t *TableRestore,
	logger log.Logger,
	kvEncoder kv.Encoder,
	deliverCompleteCh <-chan deliverResult,
	rc *RestoreController,
) (readTotalDur time.Duration, encodeTotalDur time.Duration, err error) {
	send := func(kvs []deliveredKVs) error {
		select {
		case kvsCh <- kvs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult, ok := <-deliverCompleteCh:
			if deliverResult.err == nil && !ok {
				deliverResult.err = ctx.Err()
			}
			if deliverResult.err == nil {
				deliverResult.err = errors.New("unexpected premature fulfillment")
				logger.DPanic("unexpected: deliverCompleteCh prematurely fulfilled with no error", zap.Bool("chIsOpen", ok))
			}
			return errors.Trace(deliverResult.err)
		}
	}

	pauser, maxKvPairsCnt := rc.pauser, rc.cfg.TikvImporter.MaxKVPairs
	initializedColumns, reachEOF := false, false
	for !reachEOF {
		if err = pauser.Wait(ctx); err != nil {
			return
		}
		offset, _ := cr.parser.Pos()
		if offset >= cr.chunk.Chunk.EndOffset {
			break
		}

		var readDur, encodeDur time.Duration
		canDeliver := false
		kvPacket := make([]deliveredKVs, 0, maxKvPairsCnt)
		var newOffset, rowID int64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = cr.parser.ReadRow()
			columnNames := cr.parser.Columns()
			newOffset, rowID = cr.parser.Pos()
			switch errors.Cause(err) {
			case nil:
				if !initializedColumns {
					if len(cr.chunk.ColumnPermutation) == 0 {
						if err = t.initializeColumns(columnNames, cr.chunk); err != nil {
							return
						}
					}
					initializedColumns = true
				}
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				err = errors.Annotatef(err, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := cr.parser.LastRow()
			// sql -> kv
			kvs, encodeErr := kvEncoder.Encode(logger, lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation)
			encodeDur += time.Since(encodeDurStart)
			cr.parser.RecycleRow(lastRow)
			if encodeErr != nil {
				err = errors.Annotatef(encodeErr, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, columns: columnNames, offset: newOffset, rowID: rowID})
			if len(kvPacket) >= maxKvPairsCnt || newOffset == cr.chunk.Chunk.EndOffset {
				canDeliver = true
			}
		}
		encodeTotalDur += encodeDur
		metric.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
		readTotalDur += readDur
		metric.RowReadSecondsHistogram.Observe(readDur.Seconds())
		metric.RowReadBytesHistogram.Observe(float64(newOffset - offset))

		if len(kvPacket) != 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return
			}
			metric.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
		}
	}

	err = send([]deliveredKVs{})
	return
}

func (cr *chunkRestore) restore(
	ctx context.Context,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *kv.LocalEngineWriter,
	rc *RestoreController,
) error {
	// Create the encoder.
	kvEncoder, err := rc.backend.NewEncoder(t.encTable, &kv.SessionOptions{
		SQLMode:   rc.cfg.TiDB.SQLMode,
		Timestamp: cr.chunk.Timestamp,
		SysVars:   rc.sysVars,
		// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
		AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
	})
	if err != nil {
		return err
	}

	kvsCh := make(chan []deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	defer func() {
		kvEncoder.Close()
		kvEncoder = nil
		close(kvsCh)
	}()

	go func() {
		defer close(deliverCompleteCh)
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		select {
		case <-ctx.Done():
		case deliverCompleteCh <- deliverResult{dur, err}:
		}
	}()

	logTask := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
	).Begin(zap.InfoLevel, "restore file")

	readTotalDur, encodeTotalDur, err := cr.encodeLoop(ctx, kvsCh, t, logTask.Logger, kvEncoder, deliverCompleteCh, rc)
	if err != nil {
		return err
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
