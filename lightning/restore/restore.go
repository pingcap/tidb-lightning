package restore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/cznic/mathutil"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/pkg/errors"
)

const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

var metrics = common.NewMetrics()

const (
	defaultGCLifeTime = 100 * time.Hour
)

var (
	requiredTiDBVersion = *semver.New("2.0.4")
	requiredPDVersion   = *semver.New("2.0.4")
	requiredTiKVVersion = *semver.New("2.0.4")
)

func init() {
	cfg := tidbcfg.GetGlobalConfig()
	cfg.Log.SlowThreshold = 3000

	kv.InitMembufCap(defReadBlockSize)
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
	dbMetas         map[string]*mydump.MDDatabaseMeta
	dbInfos         map[string]*TidbDBInfo
	tableWorkers    *RestoreWorkerPool
	regionWorkers   *RestoreWorkerPool
	importer        *kv.Importer
	postProcessLock sync.Mutex // a simple way to ensure post-processing is not concurrent without using complicated goroutines

	errorSummaries errorSummaries

	checkpointsDB CheckpointsDB
	saveCpCh      chan saveCp
	checkpointsWg sync.WaitGroup
}

func NewRestoreController(ctx context.Context, dbMetas map[string]*mydump.MDDatabaseMeta, cfg *config.Config) (*RestoreController, error) {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cpdb, err := OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rc := &RestoreController{
		cfg:           cfg,
		dbMetas:       dbMetas,
		tableWorkers:  NewRestoreWorkerPool(ctx, cfg.App.TableConcurrency, "table"),
		regionWorkers: NewRestoreWorkerPool(ctx, cfg.App.RegionConcurrency, "region"),
		importer:      importer,

		errorSummaries: errorSummaries{
			summary: make(map[string]errorSummary),
		},

		checkpointsDB: cpdb,
		saveCpCh:      make(chan saveCp),
	}

	return rc, nil
}

func OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (CheckpointsDB, error) {
	if !cfg.Checkpoint.Enable {
		return NewNullCheckpointsDB(), nil
	}
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
}

func (rc *RestoreController) Wait() {
	rc.checkpointsWg.Wait()
}

func (rc *RestoreController) Close() {
	rc.importer.Close()
}

func (rc *RestoreController) Run(ctx context.Context) error {
	timer := time.Now()
	opts := []func(context.Context) error{
		rc.checkRequirements,
		rc.switchToImportMode,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.analyze,
		rc.switchToNormalMode,
		rc.cleanCheckpoints,
	}

	var err error
outside:
	for _, process := range opts {
		err = process(ctx)
		switch {
		case err == nil:
		case common.IsContextCanceledError(err):
			common.AppLogger.Infof("user terminated : %v", err)
			err = nil
			break outside
		default:
			common.AppLogger.Errorf("run cause error : %s", errors.ErrorStack(err))
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			break outside // ps : not continue
		}
	}

	statistic := metrics.DumpTiming()
	common.AppLogger.Infof("Timing statistic :\n%s", statistic)
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
		for db, dbMeta := range rc.dbMetas {
			timer := time.Now()
			common.AppLogger.Infof("restore table schema for `%s`", dbMeta.Name)
			tablesSchema := make(map[string]string)
			for tbl, tblMeta := range dbMeta.Tables {
				tablesSchema[tbl] = tblMeta.GetSchema()
			}
			err = tidbMgr.InitSchema(ctx, db, tablesSchema)
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

	go rc.listenCheckpointUpdates(&rc.checkpointsWg)

	// Estimate the number of chunks for progress reporting
	rc.estimateChunkCountIntoMetrics()
	return nil
}

func (rc *RestoreController) estimateChunkCountIntoMetrics() {
	estimatedChunkCount := int64(0)
	minRegionSize := rc.cfg.Mydumper.MinRegionSize
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			for _, dataFile := range tableMeta.DataFiles {
				info, err := os.Stat(dataFile)
				if err == nil {
					estimatedChunkCount += (info.Size() + minRegionSize - 1) / minRegionSize
				}
			}
		}
	}
	metric.ChunkCounter.WithLabelValues("estimated").Add(float64(estimatedChunkCount))
}

func (rc *RestoreController) saveStatusCheckpoint(tableName string, err error, statusIfSucceed CheckpointStatus) {
	merger := &StatusCheckpointMerger{Status: statusIfSucceed}

	switch {
	case err == nil:
		break
	case !common.IsContextCanceledError(err):
		merger.SetInvalid()
		rc.errorSummaries.record(tableName, err, statusIfSucceed)
	default:
		return
	}

	metric.RecordTableCount(statusIfSucceed.MetricName(), err)
	rc.saveCpCh <- saveCp{tableName: tableName, merger: merger}
}

// listenCheckpointUpdates will combine several checkpoints together to reduce database load.
func (rc *RestoreController) listenCheckpointUpdates(wg *sync.WaitGroup) {
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
			wg.Done()
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
			wg.Add(1)
			hasCheckpoint <- struct{}{}
		}

		lock.Unlock()
	}
}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	timer := time.Now()
	var wg sync.WaitGroup

	var (
		restoreErrLock sync.Mutex
		restoreErr     error
	)

	for dbName, dbMeta := range rc.dbMetas {
		dbInfo, ok := rc.dbInfos[dbName]
		if !ok {
			common.AppLogger.Errorf("database %s not found in rc.dbInfos", dbName)
			continue
		}
		for tbl, tableMeta := range dbMeta.Tables {
			tableInfo, ok := dbInfo.Tables[tbl]
			if !ok {
				return errors.Errorf("table info %s not found", tbl)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			tr, err := NewTableRestore(tableName, tableMeta, dbInfo, tableInfo, cp)
			if err != nil {
				return errors.Trace(err)
			}

			// Note: We still need tableWorkers to control the concurrency of tables. In the future, we will investigate more about
			// the difference between restoring tables concurrently and restoring tables one by one.

			worker := rc.tableWorkers.Apply()
			wg.Add(1)
			go func(w *RestoreWorker, t *TableRestore, cp *TableCheckpoint) {
				defer wg.Done()

				closedEngine, err := t.restore(ctx, rc, cp)
				defer func() {
					metric.RecordTableCount("completed", err)
					if err != nil {
						restoreErrLock.Lock()
						if restoreErr == nil {
							restoreErr = err
						}
						restoreErrLock.Unlock()
					}
				}()
				t.Close()
				rc.tableWorkers.Recycle(w)
				if err != nil {
					if !common.IsContextCanceledError(err) {
						common.AppLogger.Errorf("[%s] restore error %v", t.tableName, errors.ErrorStack(err))
					}
					return
				}

				err = t.postProcess(ctx, closedEngine, rc, cp)
			}(worker, tr, cp)
		}
	}

	wg.Wait()
	common.AppLogger.Infof("restore all tables data takes %v", time.Since(timer))

	restoreErrLock.Lock()
	defer restoreErrLock.Unlock()
	return errors.Trace(restoreErr)
}

func (t *TableRestore) restore(ctx context.Context, rc *RestoreController, cp *TableCheckpoint) (*kv.ClosedEngine, error) {
	if cp.Status >= CheckpointStatusClosed {
		closedEngine, err := rc.importer.UnsafeCloseEngine(ctx, t.tableName, cp.Engine)
		return closedEngine, errors.Trace(err)
	}

	engine, err := rc.importer.OpenEngine(ctx, t.tableName, cp.Engine)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// no need to do anything if the chunks are already populated
	if len(cp.Chunks) > 0 {
		common.AppLogger.Infof("[%s] reusing %d chunks from checkpoint", t.tableName, len(cp.Chunks))
	} else if cp.Status < CheckpointStatusAllWritten {
		if err := t.populateChunks(rc.cfg.Mydumper.MinRegionSize, cp, t.tableInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if err := rc.checkpointsDB.InsertChunkCheckpoints(ctx, t.tableName, cp.Chunks); err != nil {
			return nil, errors.Trace(err)
		}
	}

	var wg sync.WaitGroup
	var (
		chunkErrMutex sync.Mutex
		chunkErr      error
	)

	timer := time.Now()
	handledChunksCount := new(int32)

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

		chunkErrMutex.Lock()
		err := chunkErr
		chunkErrMutex.Unlock()
		if err != nil {
			break
		}

		// Flows :
		// 	1. read mydump file
		// 	2. sql -> kvs
		// 	3. load kvs data (into kv deliver server)
		// 	4. flush kvs data (into tikv node)

		cr, err := newChunkRestore(chunkIndex, chunk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending).Inc()

		worker := rc.regionWorkers.Apply()
		wg.Add(1)
		go func(w *RestoreWorker, cr *chunkRestore) {
			// Restore a chunk.
			defer func() {
				cr.close()
				wg.Done()
				rc.regionWorkers.Recycle(w)
			}()
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateRunning).Inc()
			err := cr.restore(ctx, t, engine, rc)
			if err != nil {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Inc()
				if !common.IsContextCanceledError(err) {
					common.AppLogger.Errorf("[%s] chunk #%d (%s) run task error %s", t.tableName, cr.index, &cr.chunk.Key, errors.ErrorStack(err))
				}
				chunkErrMutex.Lock()
				if chunkErr == nil {
					chunkErr = err
				}
				chunkErrMutex.Unlock()
				return
			}
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()

			handled := int(atomic.AddInt32(handledChunksCount, 1))
			common.AppLogger.Infof("[%s] handled region count = %d (%s)", t.tableName, handled, common.Percent(handled, len(cp.Chunks)))
		}(worker, cr)
	}

	wg.Wait()
	common.AppLogger.Infof("[%s] encode kv data and write takes %v", t.tableName, time.Since(timer))
	chunkErrMutex.Lock()
	err = chunkErr
	chunkErrMutex.Unlock()
	rc.saveStatusCheckpoint(t.tableName, err, CheckpointStatusAllWritten)
	if err != nil {
		return nil, errors.Trace(err)
	}

	closedEngine, err := engine.Close(ctx)
	rc.saveStatusCheckpoint(t.tableName, err, CheckpointStatusClosed)
	if err != nil {
		common.AppLogger.Errorf("[kv-deliver] flush stage with error (step = close) : %s", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	return closedEngine, nil
}

func (t *TableRestore) postProcess(ctx context.Context, closedEngine *kv.ClosedEngine, rc *RestoreController, cp *TableCheckpoint) error {
	// 1. close engine, then calling import
	// FIXME: flush is an asynchronous operation, what if flush failed?
	if cp.Status < CheckpointStatusImported {
		// the lock ensures the import() step will not be concurrent.
		rc.postProcessLock.Lock()
		err := t.importKV(ctx, closedEngine)
		rc.postProcessLock.Unlock()
		rc.saveStatusCheckpoint(t.tableName, err, CheckpointStatusImported)
		if err != nil {
			return errors.Trace(err)
		}

		// 2. compact level 1
		err = rc.doCompact(ctx, Level1Compact)
		if err != nil {
			// log it and continue
			common.AppLogger.Warnf("[%s] do compact %d failed err %v", t.tableName, Level1Compact, errors.ErrorStack(err))
		}
	}

	// 3. alter table set auto_increment
	if cp.Status < CheckpointStatusAlteredAutoInc {
		err := t.restoreTableMeta(ctx, rc.cfg)
		rc.saveStatusCheckpoint(t.tableName, err, CheckpointStatusAlteredAutoInc)
		if err != nil {
			common.AppLogger.Errorf(
				"[%[1]s] failed to AUTO TABLE %[1]s SET AUTO_INCREMENT=%[2]d : %[3]v",
				t.tableName, t.alloc.Base()+1, err.Error(),
			)
			return errors.Trace(err)
		}
	}

	// 4. do table checksum
	if cp.Status < CheckpointStatusCompleted {
		err := t.compareChecksum(ctx, rc.cfg, cp)
		rc.saveStatusCheckpoint(t.tableName, err, CheckpointStatusCompleted)
		if err != nil {
			common.AppLogger.Errorf("[%s] checksum failed: %v", t.tableName, err.Error())
			return errors.Trace(err)
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

	return errors.Trace(rc.doCompact(ctx, FullLevelCompact))
}

func (rc *RestoreController) doCompact(ctx context.Context, level int32) error {
	return errors.Trace(rc.importer.Compact(ctx, level))
}

// analyze will analyze table for all tables.
func (rc *RestoreController) analyze(ctx context.Context) error {
	if !rc.cfg.PostRestore.Analyze {
		common.AppLogger.Info("Skip analyze table.")
		return nil
	}

	tables := rc.getTables()
	err := analyzeTable(ctx, rc.cfg.TiDB, tables)
	return errors.Trace(err)
}

func (rc *RestoreController) switchToImportMode(ctx context.Context) error {
	return errors.Trace(rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import))
}

func (rc *RestoreController) switchToNormalMode(ctx context.Context) error {
	return errors.Trace(rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal))
}

func (rc *RestoreController) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) error {
	return errors.Trace(rc.importer.SwitchMode(ctx, mode))
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
	// TODO: Reenable the PD/TiKV version check after we upgrade the dependency to 2.1.
	if err := rc.checkPDVersion(client); err != nil {
		// return errors.Trace(err)
		common.AppLogger.Infof("PD version check failed: %v", err)
	}
	if err := rc.checkTiKVVersion(client); err != nil {
		// return errors.Trace(err)
		common.AppLogger.Infof("TiKV version check failed: %v", err)
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
	// The version is generated by `git describe --tags` on the TiDB repository.
	versions := strings.Split(version, "-")
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

func (rc *RestoreController) getTables() []string {
	var numOfTables int
	for _, dbMeta := range rc.dbMetas {
		numOfTables += len(dbMeta.Tables)
	}
	tables := make([]string, 0, numOfTables)

	for _, dbMeta := range rc.dbMetas {
		for tbl := range dbMeta.Tables {
			tables = append(tables, common.UniqueTable(dbMeta.Name, tbl))
		}
	}

	return tables
}

func analyzeTable(ctx context.Context, dsn config.DBStore, tables []string) error {
	totalTimer := time.Now()
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		common.AppLogger.Errorf("connect db failed %v, the next operation is: ANALYZE TABLE. You should do it one by one manually", err)
		return errors.Trace(err)
	}
	defer db.Close()

	// speed up executing analyze table temporarily
	setSessionVarInt(ctx, db, "tidb_build_stats_concurrency", 16)
	setSessionVarInt(ctx, db, "tidb_distsql_scan_concurrency", dsn.DistSQLScanConcurrency)

	// TODO: do it concurrently.
	var analyzeErr error
	for _, table := range tables {
		timer := time.Now()
		common.AppLogger.Infof("[%s] analyze", table)
		query := fmt.Sprintf("ANALYZE TABLE %s", table)
		err := common.ExecWithRetry(ctx, db, query, query)
		if err != nil {
			if analyzeErr == nil {
				analyzeErr = err
			}
			common.AppLogger.Errorf("%s error %s", query, errors.ErrorStack(err))
			continue
		}
		common.AppLogger.Infof("[%s] analyze takes %v", table, time.Since(timer))
	}

	common.AppLogger.Infof("doing all tables analyze takes %v", time.Since(totalTimer))
	return errors.Trace(analyzeErr)
}

////////////////////////////////////////////////////////////////

func setSessionVarInt(ctx context.Context, db *sql.DB, name string, value int) {
	stmt := fmt.Sprintf("set session %s = ?", name)
	if err := common.ExecWithRetry(ctx, db, stmt, stmt, value); err != nil {
		common.AppLogger.Warnf("failed to set variable @%s to %d: %s", name, value, err.Error())
	}
}

////////////////////////////////////////////////////////////////

type RestoreWorkerPool struct {
	limit   int
	workers chan *RestoreWorker
	name    string
}

type RestoreWorker struct {
	ID int64
}

func NewRestoreWorkerPool(ctx context.Context, limit int, name string) *RestoreWorkerPool {
	workers := make(chan *RestoreWorker, limit)
	for i := 0; i < limit; i++ {
		workers <- &RestoreWorker{ID: int64(i + 1)}
	}

	metric.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	return &RestoreWorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

func (pool *RestoreWorkerPool) Apply() *RestoreWorker {
	worker := <-pool.workers
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	return worker
}
func (pool *RestoreWorkerPool) Recycle(worker *RestoreWorker) {
	pool.workers <- worker
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
}

////////////////////////////////////////////////////////////////

type chunkRestore struct {
	parser *mydump.ChunkParser
	index  int
	chunk  *ChunkCheckpoint
}

func newChunkRestore(index int, chunk *ChunkCheckpoint) (*chunkRestore, error) {
	reader, err := os.Open(chunk.Key.Path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	parser := mydump.NewChunkParser(reader)

	reader.Seek(chunk.Chunk.Offset, io.SeekStart)
	parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax)

	return &chunkRestore{
		parser: parser,
		index:  index,
		chunk:  chunk,
	}, nil
}

func (cr *chunkRestore) close() {
	cr.parser.Reader().(*os.File).Close()
}

type TableRestore struct {
	// The unique table name in the form "`db`.`tbl`".
	tableName string
	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encoder   kvenc.KvEncoder
	alloc     autoid.Allocator

	checkpointStatus CheckpointStatus
	engine           *kv.OpenedEngine
}

func NewTableRestore(
	tableName string,
	tableMeta *mydump.MDTableMeta,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	cp *TableCheckpoint,
) (*TableRestore, error) {
	idAlloc := kv.NewPanickingAllocator(cp.AllocBase)
	encoder, err := kvenc.New(dbInfo.Name, idAlloc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create table in encoder.
	err = encoder.ExecDDLSQL(tableInfo.CreateTableStmt)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &TableRestore{
		tableName: tableName,
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
		encoder:   encoder,
		alloc:     idAlloc,
	}, nil
}

func (tr *TableRestore) Close() {
	tr.encoder.Close()
	common.AppLogger.Infof("[%s] restore done", tr.tableName)
}

var tidbRowIDColumnRegex = regexp.MustCompile(fmt.Sprintf("`%[1]s`|(?i:\\b%[1]s\\b)", model.ExtraHandleName))

func (t *TableRestore) populateChunks(minChunkSize int64, cp *TableCheckpoint, tableInfo *TidbTableInfo) error {
	common.AppLogger.Infof("[%s] load chunks", t.tableName)
	timer := time.Now()

	founder := mydump.NewRegionFounder(minChunkSize)
	chunks, err := founder.MakeTableRegions(t.tableMeta)
	if err != nil {
		return errors.Trace(err)
	}

	cp.Chunks = make([]*ChunkCheckpoint, 0, len(chunks))

	for _, chunk := range chunks {
		columns := chunk.Columns

		shouldIncludeRowID := !tableInfo.core.PKIsHandle && !tidbRowIDColumnRegex.Match(columns)
		if shouldIncludeRowID {
			// we need to inject the _tidb_rowid column
			if len(columns) != 0 {
				// column listing already exists, just append the new column.
				columns = append(columns[:len(columns)-1], (",`" + model.ExtraHandleName.String() + "`)")...)
			} else {
				// we need to recreate the columns
				var buf bytes.Buffer
				buf.WriteString("(`")
				for _, columnInfo := range tableInfo.core.Columns {
					buf.WriteString(columnInfo.Name.String())
					buf.WriteString("`,`")
				}
				buf.WriteString(model.ExtraHandleName.String())
				buf.WriteString("`)")
				columns = buf.Bytes()
			}
		}

		cp.Chunks = append(cp.Chunks, &ChunkCheckpoint{
			Key: ChunkCheckpointKey{
				Path:   chunk.File,
				Offset: chunk.Chunk.Offset,
			},
			Columns:            columns,
			ShouldIncludeRowID: shouldIncludeRowID,
			Chunk:              chunk.Chunk,
		})
	}

	common.AppLogger.Infof("[%s] load %d chunks takes %v", t.tableName, len(chunks), time.Since(timer))
	return nil
}

func (tr *TableRestore) restoreTableMeta(ctx context.Context, cfg *config.Config) error {
	timer := time.Now()
	dsn := cfg.TiDB
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	err = AlterAutoIncrement(ctx, db, tr.tableMeta.DB, tr.tableMeta.Name, tr.alloc.Base()+1)
	if err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] alter table set auto_id takes %v", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name), time.Since(timer))
	return nil
}

func (tr *TableRestore) importKV(ctx context.Context, closedEngine *kv.ClosedEngine) error {
	common.AppLogger.Infof("[%s] flush kv deliver ...", tr.tableName)

	start := time.Now()
	defer func() {
		metrics.MarkTiming(fmt.Sprintf("[%s]_kv_flush", tr.tableName), start)
		common.AppLogger.Infof("[%s] kv deliver all flushed !", tr.tableName)
	}()

	err := closedEngine.Import(ctx)
	if err != nil {
		if !common.IsContextCanceledError(err) {
			common.AppLogger.Errorf("[%s] failed to flush kvs : %s", tr.tableName, err.Error())
		}
		return errors.Trace(err)
	}
	closedEngine.Cleanup(ctx)
	return nil
}

// do checksum for each table.
func (tr *TableRestore) compareChecksum(ctx context.Context, cfg *config.Config, cp *TableCheckpoint) error {
	if !cfg.PostRestore.Checksum {
		common.AppLogger.Infof("[%s] Skip checksum.", tr.tableName)
		return nil
	}

	var localChecksum verify.KVChecksum
	for _, chunk := range cp.Chunks {
		localChecksum.Add(&chunk.Checksum)
	}
	common.AppLogger.Infof("[%s] local checksum %+v", tr.tableName, localChecksum)

	remoteChecksum, err := DoChecksum(ctx, cfg.TiDB, tr.tableName)
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

	common.AppLogger.Infof("[%s] checksum pass", tr.tableName)
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

// DoChecksum do checksum for tables.
// table should be in <db>.<table>, format.  e.g. foo.bar
func DoChecksum(ctx context.Context, dsn config.DBStore, table string) (*RemoteChecksum, error) {
	timer := time.Now()
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer db.Close()

	ori, err := increaseGCLifeTime(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// set it back finally
	defer func() {
		err = UpdateGCLifeTime(ctx, db, ori)
		if err != nil && !common.IsContextCanceledError(err) {
			common.AppLogger.Errorf("[%s] update tikv_gc_life_time error %v", table, errors.ErrorStack(err))
		}
	}()

	// speed up executing checksum table temporarily
	// FIXME: now we do table checksum separately, will it be too frequent to update these variables?
	setSessionVarInt(ctx, db, "tidb_checksum_table_concurrency", 16)
	setSessionVarInt(ctx, db, "tidb_distsql_scan_concurrency", dsn.DistSQLScanConcurrency)

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

func (cr *chunkRestore) restore(
	ctx context.Context,
	t *TableRestore,
	engine *kv.OpenedEngine,
	rc *RestoreController,
) error {
	// Create the encoder.
	kvEncoder, err := kv.NewTableKVEncoder(
		t.dbInfo.Name,
		t.tableInfo.Name,
		t.tableInfo.ID,
		t.tableInfo.Columns,
		rc.cfg.TiDB.SQLMode,
		t.alloc,
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		closeErr := kvEncoder.Close()
		kvEncoder = nil
		if closeErr != nil {
			common.AppLogger.Errorf("restore chunk task err %v", errors.ErrorStack(closeErr))
		}
	}()

	readMark := fmt.Sprintf("[%s]_read_file", t.tableName)
	encodeMark := fmt.Sprintf("[%s]_sql_2_kv", t.tableName)
	deliverMark := fmt.Sprintf("[%s]_deliver_write", t.tableName)

	timer := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		endOffset := mathutil.MinInt64(cr.chunk.Chunk.EndOffset, cr.parser.Pos()+rc.cfg.Mydumper.ReadBlockSize)
		if cr.parser.Pos() >= endOffset {
			break
		}

		start := time.Now()

		var sqls strings.Builder
		sqls.WriteString("INSERT INTO ")
		sqls.WriteString(t.tableName)
		sqls.Write(cr.chunk.Columns)
		sqls.WriteString(" VALUES")
		var sep byte = ' '
	readLoop:
		for cr.parser.Pos() < endOffset {
			err := cr.parser.ReadRow()
			switch errors.Cause(err) {
			case nil:
				sqls.WriteByte(sep)
				sep = ','
				lastRow := cr.parser.LastRow()
				if cr.chunk.ShouldIncludeRowID {
					sqls.Write(lastRow.Row[:len(lastRow.Row)-1])
					fmt.Fprintf(&sqls, ",%d)", lastRow.RowID)
				} else {
					sqls.Write(lastRow.Row)
				}
			case io.EOF:
				break readLoop
			default:
				return errors.Trace(err)
			}
		}
		if sep != ',' { // quick and dirty way to check if `sqls` actually contained any values
			continue
		}
		sqls.WriteByte(';')

		metrics.MarkTiming(readMark, start)

		var (
			totalKVs      []kvenc.KvPair
			localChecksum verify.KVChecksum
		)
		// sql -> kv
		start = time.Now()
		kvs, _, err := kvEncoder.SQL2KV(sqls.String())
		metrics.MarkTiming(encodeMark, start)
		common.AppLogger.Debugf("len(kvs) %d, len(sql) %d", len(kvs), sqls.Len())
		if err != nil {
			common.AppLogger.Errorf("kv encode failed = %s\n", err.Error())
			return errors.Trace(err)
		}

		totalKVs = append(totalKVs, kvs...)
		localChecksum.Update(kvs)

		// kv -> deliver ( -> tikv )
		start = time.Now()
		stream, err := engine.NewWriteStream(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		err = stream.Put(totalKVs)
		if e := stream.Close(); e != nil {
			if err != nil {
				common.AppLogger.Warnf("failed to close write stream: %s", e.Error())
			} else {
				err = e
			}
		}
		metrics.MarkTiming(deliverMark, start)
		if err != nil {
			// TODO : retry ~
			common.AppLogger.Errorf("kv deliver failed = %s\n", err.Error())
			return errors.Trace(err)
		}

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		cr.chunk.Checksum.Add(&localChecksum)
		cr.chunk.Chunk.Offset = cr.parser.Pos()
		cr.chunk.Chunk.PrevRowIDMax = cr.parser.LastRow().RowID
		rc.saveCpCh <- saveCp{
			tableName: t.tableName,
			merger: &ChunkCheckpointMerger{
				Key:       cr.chunk.Key,
				AllocBase: t.alloc.Base() + 1,
				Checksum:  cr.chunk.Checksum,
				Pos:       cr.chunk.Chunk.Offset,
				RowID:     cr.chunk.Chunk.PrevRowIDMax,
			},
		}
	}

	common.AppLogger.Infof("[%s] restore chunk #%d (%s) takes %v", t.tableName, cr.index, &cr.chunk.Key, time.Since(timer))

	return nil
}
