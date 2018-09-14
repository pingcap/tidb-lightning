package restore

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/juju/errors"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/kvencoder"

	// hack for glide update, delete later
	_ "github.com/pingcap/tidb-tools/pkg/table-router"
	_ "github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

var metrics = common.NewMetrics()

const (
	defaultGCLifeTime           = 100 * time.Hour
	closeEngineRetryDuration    = 6 * time.Second
	closeEngineRetryMaxDuration = 30 * time.Second
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

type RestoreController struct {
	cfg             *config.Config
	dbMetas         map[string]*mydump.MDDatabaseMeta
	dbInfos         map[string]*TidbDBInfo
	tableWorkers    *RestoreWorkerPool
	regionWorkers   *RestoreWorkerPool
	importer        *kv.Importer
	postProcessLock sync.Mutex // a simple way to ensure post-processing is not concurrent without using complicated goroutines

	checkpointsDB CheckpointsDB
	saveCpCh      chan checkpointUpdater
}

func NewRestoreController(ctx context.Context, dbMetas map[string]*mydump.MDDatabaseMeta, cfg *config.Config) (*RestoreController, error) {
	importer, err := kv.NewImporter(ctx, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rc := &RestoreController{
		cfg:           cfg,
		dbMetas:       dbMetas,
		tableWorkers:  NewRestoreWorkerPool(ctx, cfg.App.TableConcurrency, "table"),
		regionWorkers: NewRestoreWorkerPool(ctx, cfg.App.RegionConcurrency, "region"),
		importer:      importer,

		checkpointsDB: NewNullCheckpointsDB(),
		saveCpCh:      make(chan checkpointUpdater),
	}

	return rc, nil
}

func (rc *RestoreController) Close() {
	rc.importer.Close()
}

func (rc *RestoreController) Run(ctx context.Context) {
	timer := time.Now()
	opts := []func(context.Context) error{
		rc.checkRequirements,
		rc.switchToImportMode,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.analyze,
		rc.switchToNormalMode,
	}

outside:
	for _, process := range opts {
		err := process(ctx)
		switch {
		case err == nil:
		case common.IsContextCanceledError(err):
			common.AppLogger.Infof("user terminated : %v", err)
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

	return
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

	err = rc.checkpointsDB.Load(ctx, dbInfos)
	if err != nil {
		return errors.Trace(err)
	}
	go rc.listenCheckpointUpdates(ctx)

	// Estimate the number of chunks for progress reporting
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

	return nil
}

type checkpointUpdater interface {
	updateCheckpointsDB(ctx context.Context, cpdb CheckpointsDB)
}

type saveChunkCheckpoint struct {
	tableName string
	alloc     *kvenc.Allocator
	checksum  verify.KVChecksum
	path      string
	offset    int64
}
type saveStatusCheckpoint struct {
	tableName string
	status    CheckpointStatus
}
type saveFailureCheckpoint struct {
	tableName string
}

func (u *saveChunkCheckpoint) updateCheckpointsDB(ctx context.Context, cpdb CheckpointsDB) {
	cpdb.UpdateChunk(ctx, u.tableName, u.alloc.Base()+1, u.checksum, u.path, u.offset)
}
func (u *saveStatusCheckpoint) updateCheckpointsDB(ctx context.Context, cpdb CheckpointsDB) {
	cpdb.UpdateStatus(ctx, u.tableName, u.status)
}
func (u *saveFailureCheckpoint) updateCheckpointsDB(ctx context.Context, cpdb CheckpointsDB) {
	cpdb.UpdateFailure(ctx, u.tableName)
}

func (rc *RestoreController) saveStatusCheckpoint(ctx context.Context, tableName string, err error, statusIfSucceed CheckpointStatus) {
	var updator checkpointUpdater

	switch {
	case err == nil:
		updator = &saveStatusCheckpoint{tableName: tableName, status: statusIfSucceed}
	case !common.IsContextCanceledError(err):
		common.AppLogger.Warnf("Save checkpoint error for table %s before step %d: %+v", tableName, statusIfSucceed, err)
		updator = &saveFailureCheckpoint{tableName: tableName}
	default:
		return
	}

	metric.RecordTableCount(statusIfSucceed.MetricName(), err)
	rc.saveCpCh <- updator
}

func (rc *RestoreController) listenCheckpointUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// TODO find a safer alternative
			close(rc.saveCpCh)
			return
		case updater := <-rc.saveCpCh:
			updater.updateCheckpointsDB(ctx, rc.checkpointsDB)
		}
	}
}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	timer := time.Now()
	var wg sync.WaitGroup

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

			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			tr, err := NewTableRestore(tableName, tableMeta, dbInfo, tableInfo, cp)
			if err != nil {
				return errors.Trace(err)
			}

			select {
			case <-ctx.Done():
				return context.Canceled
			default:
			}

			// Note: We still need tableWorkers to control the concurrency of tables. In the future, we will investigate more about
			// the difference between restoring tables concurrently and restoring tables one by one.

			worker := rc.tableWorkers.Apply()
			wg.Add(1)
			go func(w *RestoreWorker, t *TableRestore, cp *TableCheckpoint) {
				defer wg.Done()

				closedEngine, err := t.restore(ctx, rc, cp)
				defer metric.RecordTableCount("completed", err)
				t.Close()
				rc.tableWorkers.Recycle(w)
				if err != nil {
					common.AppLogger.Errorf("[%s] restore error %v", t.tableName, errors.ErrorStack(err))
					return
				}

				err = t.postProcess(ctx, closedEngine, rc, cp)
			}(worker, tr, cp)
		}
	}

	wg.Wait()
	common.AppLogger.Infof("restore all tables data takes %v", time.Since(timer))

	return nil
}

func (t *TableRestore) restore(ctx context.Context, rc *RestoreController, cp *TableCheckpoint) (*kv.ClosedEngine, error) {
	if cp.Status >= CheckpointStatusClosed {
		return rc.importer.UnsafeNewClosedEngine(t.tableName, cp.Engine), nil
	}

	engine, err := rc.importer.OpenEngine(ctx, t.tableName, cp.Engine)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var chunks []*mydump.TableRegion
	if cp.Status < CheckpointStatusAllWritten {
		chunks = t.loadChunks(rc.cfg.Mydumper.MinRegionSize, cp)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	timer := time.Now()
	handledChunksCount := new(int32)

	// Restore table data
	for _, chunk := range chunks {
		select {
		case err := <-errCh:
			rc.saveStatusCheckpoint(ctx, t.tableName, err, CheckpointStatusAllWritten)
			return nil, errors.Trace(err)
		case <-ctx.Done():
			return nil, context.Canceled
		default:
		}

		// Flows :
		// 	1. read mydump file
		// 	2. sql -> kvs
		// 	3. load kvs data (into kv deliver server)
		// 	4. flush kvs data (into tikv node)

		cr, err := newChunkRestore(chunk)
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
			err := cr.restore(ctx, t, engine, rc.cfg)
			if err != nil {
				metric.ChunkCounter.WithLabelValues(metric.ChunkStateFailed).Inc()
				common.AppLogger.Errorf("[%s] chunk %s run task error %s", t.tableName, cr.name, errors.ErrorStack(err))
				errCh <- err
				return
			}
			metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()

			handled := int(atomic.AddInt32(handledChunksCount, 1))
			common.AppLogger.Infof("[%s] handled region count = %d (%s)", t.tableName, handled, common.Percent(handled, len(chunks)))

			// Update the table, and save a checkpoint.
			t.checksumLock.Lock()
			defer t.checksumLock.Unlock()
			t.checksum.Add(&cr.checksum)
			t.rows += cr.rows
			rc.saveCpCh <- &saveChunkCheckpoint{
				tableName: t.tableName,
				alloc:     t.alloc,
				checksum:  t.checksum,
				path:      cr.path,
				offset:    cr.offset,
			}
		}(worker, cr)
	}

	wg.Wait()
	common.AppLogger.Infof("[%s] encode kv data and write takes %v", t.tableName, time.Since(timer))
	rc.saveStatusCheckpoint(ctx, t.tableName, nil, CheckpointStatusAllWritten)

	closedEngine, err := closeWithRetry(ctx, engine)
	rc.saveStatusCheckpoint(ctx, t.tableName, err, CheckpointStatusClosed)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return closedEngine, nil
}

func closeWithRetry(ctx context.Context, engine *kv.OpenedEngine) (*kv.ClosedEngine, error) {
	// ensure the engine is closed (thus the memory for tikv-importer is freed)
	// before recycling the table workers.

	var err error

	closeTicker := time.NewTicker(closeEngineRetryDuration)
	timeoutTimer := time.NewTimer(closeEngineRetryMaxDuration)
	closeImmediately := make(chan struct{}, 1)
	defer closeTicker.Stop()
	defer timeoutTimer.Stop()
	retryCount := 0

	closeImmediately <- struct{}{}

outside:
	for {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-timeoutTimer.C:
			break outside
		case <-closeTicker.C:
		case <-closeImmediately:
		}
		closedEngine, err := engine.Close(ctx)
		if err == nil {
			return closedEngine, nil
		}
		if !strings.Contains(err.Error(), "EngineInUse") {
			// error cannot be recovered, return immediately
			break outside
		}
		retryCount++
		common.AppLogger.Infof("engine still not fully closed, retry #%d after %s : %s", retryCount, closeEngineRetryDuration, err)
	}

	common.AppLogger.Errorf("[kv-deliver] flush stage with error (step = close) : %s", errors.ErrorStack(err))
	return nil, errors.Trace(err)
}

func (t *TableRestore) postProcess(ctx context.Context, closedEngine *kv.ClosedEngine, rc *RestoreController, cp *TableCheckpoint) error {
	// 1. close engine, then calling import
	// FIXME: flush is an asynchronous operation, what if flush failed?
	if cp.Status < CheckpointStatusImported {
		// the lock ensures the import() step will not be concurrent.
		rc.postProcessLock.Lock()
		err := t.importKV(ctx, closedEngine)
		rc.postProcessLock.Unlock()
		rc.saveStatusCheckpoint(ctx, t.tableName, err, CheckpointStatusImported)
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
		rc.saveStatusCheckpoint(ctx, t.tableName, err, CheckpointStatusAlteredAutoInc)
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
		err := t.compareChecksum(ctx, rc.cfg)
		rc.saveStatusCheckpoint(ctx, t.tableName, err, CheckpointStatusCompleted)
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
	for _, table := range tables {
		timer := time.Now()
		common.AppLogger.Infof("[%s] analyze", table)
		query := fmt.Sprintf("ANALYZE TABLE %s", table)
		err := common.ExecWithRetry(ctx, db, []string{query})
		if err != nil {
			common.AppLogger.Errorf("%s error %s", query, errors.ErrorStack(err))
			continue
		}
		common.AppLogger.Infof("[%s] analyze takes %v", table, time.Since(timer))
	}

	common.AppLogger.Infof("doing all tables analyze takes %v", time.Since(totalTimer))
	return nil
}

////////////////////////////////////////////////////////////////

func setSessionVarInt(ctx context.Context, db *sql.DB, name string, value int) {
	stmt := fmt.Sprintf("set session %s = %d", name, value)
	if err := common.ExecWithRetry(ctx, db, []string{stmt}); err != nil {
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
	reader *mydump.RegionReader
	path   string
	offset int64
	name   string

	checksum verify.KVChecksum
	rows     uint64
}

func newChunkRestore(chunk *mydump.TableRegion) (*chunkRestore, error) {
	reader, err := mydump.NewRegionReader(chunk.File, chunk.Offset, chunk.Size)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &chunkRestore{
		reader: reader,
		path:   chunk.File,
		offset: chunk.Offset,
		name:   chunk.Name(),
	}, nil
}

func (cr *chunkRestore) close() {
	cr.reader.Close()
}

type TableRestore struct {
	// The unique table name in the form "`db`.`tbl`".
	tableName string
	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encoder   kvenc.KvEncoder
	alloc     *kvenc.Allocator

	checksumLock     sync.Mutex
	checksum         verify.KVChecksum
	rows             uint64
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
	idAlloc := kvenc.NewAllocator()
	idAlloc.Reset(cp.AllocBase)
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
		checksum:  cp.Checksum,
	}, nil
}

func (tr *TableRestore) Close() {
	tr.encoder.Close()
	common.AppLogger.Infof("[%s] restore done", tr.tableName)
}

func (t *TableRestore) loadChunks(minChunkSize int64, cp *TableCheckpoint) []*mydump.TableRegion {
	common.AppLogger.Infof("[%s] load chunks", t.tableName)
	timer := time.Now()

	founder := mydump.NewRegionFounder(minChunkSize)
	chunks := founder.MakeTableRegions(t.tableMeta)

	// Ref: https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	// Remove all regions which have been imported
	newChunks := chunks[:0]
	for _, chunk := range chunks {
		if !cp.HasChunk(chunk.File, chunk.Offset) {
			newChunks = append(newChunks, chunk)
		}
	}

	common.AppLogger.Infof(
		"[%s] load %d chunks (%d are new) takes %v",
		t.tableName, len(chunks), len(newChunks), time.Since(timer),
	)
	return newChunks
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
		common.AppLogger.Errorf("[%s] failed to flush kvs : %s", tr.tableName, err.Error())
		return errors.Trace(err)
	}
	closedEngine.Cleanup(ctx)
	common.AppLogger.Infof("[%s] local checksum %v, has imported %d rows", tr.tableName, tr.checksum, tr.rows)
	return nil
}

// do checksum for each table.
func (tr *TableRestore) compareChecksum(ctx context.Context, cfg *config.Config) error {
	if !cfg.PostRestore.Checksum {
		common.AppLogger.Infof("[%s] Skip checksum.", tr.tableName)
		return nil
	}

	remoteChecksum, err := DoChecksum(ctx, cfg.TiDB, tr.tableName)
	if err != nil {
		return errors.Trace(err)
	}

	if remoteChecksum.Checksum != tr.checksum.Sum() ||
		remoteChecksum.TotalKVs != tr.checksum.SumKVS() ||
		remoteChecksum.TotalBytes != tr.checksum.SumSize() {
		return errors.Errorf("checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
			remoteChecksum.Checksum, tr.checksum.Sum(),
			remoteChecksum.TotalKVs, tr.checksum.SumKVS(),
			remoteChecksum.TotalBytes, tr.checksum.SumSize(),
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
		if err != nil {
			common.AppLogger.Errorf("[%s] update tikv_gc_life_time error %s", table, errors.ErrorStack(err))
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

func (cr *chunkRestore) restore(ctx context.Context, t *TableRestore, engine *kv.OpenedEngine, cfg *config.Config) error {
	// Create the write stream
	stream, err := engine.NewWriteStream(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer stream.Close()

	// Create the encoder.
	kvEncoder, err := kv.NewTableKVEncoder(
		t.dbInfo.Name,
		t.tableInfo.Name,
		t.tableInfo.ID,
		t.tableInfo.Columns,
		cfg.TiDB.SQLMode,
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

outside:
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		start := time.Now()
		sqls, err := cr.reader.Read(cfg.Mydumper.ReadBlockSize)
		switch errors.Cause(err) {
		case nil:
		case io.EOF:
			break outside
		default:
			return errors.Trace(err)
		}
		metrics.MarkTiming(readMark, start)

		for _, stmt := range sqls {
			// sql -> kv
			start = time.Now()
			kvs, affectedRows, err := kvEncoder.SQL2KV(stmt)
			metrics.MarkTiming(encodeMark, start)
			common.AppLogger.Debugf("len(kvs) %d, len(sql) %d", len(kvs), len(stmt))
			if err != nil {
				common.AppLogger.Errorf("kv encode failed = %s\n", err.Error())
				return errors.Trace(err)
			}

			// kv -> deliver ( -> tikv )
			start = time.Now()
			err = stream.Put(kvs)
			metrics.MarkTiming(deliverMark, start)
			if err != nil {
				// TODO : retry ~
				common.AppLogger.Errorf("kv deliver failed = %s\n", err.Error())
				return errors.Trace(err)
			}

			cr.checksum.Update(kvs)
			cr.rows += affectedRows
		}
	}

	common.AppLogger.Infof("[%s] restore chunk [%s] takes %v", t.tableName, cr.name, time.Since(timer))

	return nil
}
