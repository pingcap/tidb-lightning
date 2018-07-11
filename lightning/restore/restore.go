package restore

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/juju/errors"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	tidbcfg "github.com/pingcap/tidb/config"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	"github.com/satori/go.uuid"
)

const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

var (
	errCtxAborted = errors.New("context aborted error")
	metrics       = common.NewMetrics()
)

const (
	defaultGCLifeTime = 100 * time.Hour
)

func init() {
	cfg := tidbcfg.GetGlobalConfig()
	cfg.Log.SlowThreshold = 3000

	kv.InitMembufCap(defReadBlockSize)
}

type RestoreController struct {
	cfg              *config.Config
	dbMeta           *mydump.MDDatabaseMeta
	dbInfo           *TidbDBInfo
	tableWorkers     *RestoreWorkerPool
	regionWorkers    *RestoreWorkerPool
	deliverMgr       *kv.KVDeliverKeeper
	postProcessQueue chan *TableRestore
}

func NewRestoreControlloer(ctx context.Context, dbMeta *mydump.MDDatabaseMeta, cfg *config.Config) *RestoreController {
	rc := &RestoreController{
		cfg:              cfg,
		dbMeta:           dbMeta,
		tableWorkers:     NewRestoreWorkerPool(cfg.App.WorkerPoolSize / 2),
		regionWorkers:    NewRestoreWorkerPool(cfg.App.WorkerPoolSize),
		deliverMgr:       kv.NewKVDeliverKeeper(cfg.TikvImporter.Addr, cfg.TiDB.PdAddr),
		postProcessQueue: make(chan *TableRestore),
	}

	go rc.handlePostProcessing(ctx)
	return rc
}

func (rc *RestoreController) Close() {
	rc.deliverMgr.Close()
}

func (rc *RestoreController) Run(ctx context.Context) {
	timer := time.Now()
	opts := []func(context.Context) error{
		rc.switchToImportMode,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.analyze,
		rc.switchToNormalMode,
	}

	for _, process := range opts {
		err := process(ctx)
		if errors.Cause(err) == errCtxAborted {
			break
		}
		if err != nil {
			common.AppLogger.Errorf("run cause error : %s", errors.ErrorStack(err))
			break // ps : not continue
		}
	}

	// show metrics
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

	database := rc.dbMeta.Name

	if !rc.cfg.Mydumper.NoSchema {
		timer := time.Now()
		common.AppLogger.Infof("restore table schema for `%s`", rc.dbMeta.Name)
		tablesSchema := make(map[string]string)
		for tbl, tblMeta := range rc.dbMeta.Tables {
			tablesSchema[tbl] = tblMeta.GetSchema()
		}
		err = tidbMgr.InitSchema(ctx, database, tablesSchema)
		if err != nil {
			return errors.Errorf("db schema failed to init : %v", err)
		}
		common.AppLogger.Infof("restore table schema for `%s` takes %v", rc.dbMeta.Name, time.Since(timer))
	}
	dbInfo, err := tidbMgr.LoadSchemaInfo(ctx, database)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dbInfo = dbInfo
	return nil
}

func (rc *RestoreController) restoreTables(ctx context.Context) error {
	timer := time.Now()
	dbInfo := rc.dbInfo
	var wg sync.WaitGroup

	for tbl, tableMeta := range rc.dbMeta.Tables {
		tableInfo, ok := dbInfo.Tables[tbl]
		if !ok {
			return errors.Errorf("table info %s not found", tbl)
		}

		tr := NewTableRestore(ctx, dbInfo, tableInfo, tableMeta, rc.cfg, rc.deliverMgr)

		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

		worker := rc.tableWorkers.Apply()
		wg.Add(1)
		go func(w *RestoreWorker, t *TableRestore) {
			defer rc.tableWorkers.Recycle(w)
			defer wg.Done()
			table := common.UniqueTable(t.tableMeta.DB, t.tableMeta.Name)
			err := rc.restoreTable(ctx, tr)
			if err != nil {
				common.AppLogger.Errorf("[%s] restore error %v", table, errors.ErrorStack(err))
			}
		}(worker, tr)
	}

	wg.Wait()
	common.AppLogger.Infof("restore all tables data takes %v", time.Since(timer))

	return nil
}

//FIXME: it seems that we don't need to split table into multiple regions.
func (rc *RestoreController) restoreTable(ctx context.Context, t *TableRestore) error {
	defer t.Close()
	timer := time.Now()
	table := common.UniqueTable(t.tableMeta.DB, t.tableMeta.Name)

	var wg sync.WaitGroup

	//1. restore table data
	for _, task := range t.tasks {
		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

		worker := rc.regionWorkers.Apply()
		wg.Add(1)
		go func(w *RestoreWorker, t *regionRestoreTask) {
			defer rc.regionWorkers.Recycle(w)
			defer wg.Done()
			err := t.Run(ctx)
			if err != nil {
				common.AppLogger.Errorf("[%s] region %s run task error %s", table, t.region.Name(), errors.ErrorStack(err))
			}
		}(worker, task)
	}
	wg.Wait()
	common.AppLogger.Infof("[%s] encode kv data and write takes %v", table, time.Since(timer))

	var (
		tableRows uint64
		checksum  = verify.NewKVChecksum(0)
	)
	for _, regStat := range t.handledRegions {
		tableRows += regStat.rows
		if regStat.maxRowID > t.tableMaxRowID {
			t.tableMaxRowID = regStat.maxRowID
		}
		checksum.Add(regStat.checksum)
	}
	t.localChecksum = checksum
	t.tableRows = tableRows

	err := rc.postProcessing(t)
	return errors.Trace(err)
}

func (rc *RestoreController) postProcessing(t *TableRestore) error {
	postProcessError := make(chan error, 1)
	t.postProcessError = postProcessError
	rc.postProcessQueue <- t
	if err := <-postProcessError; err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *RestoreController) handlePostProcessing(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case table := <-rc.postProcessQueue:
			table.postProcessError <- rc.doPostProcessing(ctx, table)
		}
	}
}

func (rc *RestoreController) doPostProcessing(ctx context.Context, t *TableRestore) error {
	uniqueTable := common.UniqueTable(t.tableMeta.DB, t.tableMeta.Name)
	// 1. close engine, then calling import
	if err := t.importKV(); err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] local checksum %s, has imported %d rows", uniqueTable, t.localChecksum, t.tableRows)

	// 2. compact level 1
	if err := rc.doCompact(ctx, Level1Compact); err != nil {
		// log it and continue
		common.AppLogger.Warnf("[%s] do compact %d failed err %v", uniqueTable, Level1Compact, errors.ErrorStack(err))
	}

	// 3. alter table set auto_increment
	if err := t.restoreTableMeta(ctx); err != nil {
		return errors.Trace(err)
	}

	// 4. do table checksum
	if err := t.checksum(ctx); err != nil {
		return errors.Trace(err)
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
	cli, err := kv.NewKVDeliverClient(ctx, uuid.Nil, rc.cfg.TikvImporter.Addr, rc.cfg.TiDB.PdAddr, "")
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	return errors.Trace(cli.Compact(level))
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
	cli, err := kv.NewKVDeliverClient(ctx, uuid.Nil, rc.cfg.TikvImporter.Addr, rc.cfg.TiDB.PdAddr, "")
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	return errors.Trace(cli.Switch(mode))
}

func (rc *RestoreController) getTables() []string {
	tables := make([]string, 0, len(rc.dbMeta.Tables))
	dbInfo := rc.dbInfo
	for tbl := range rc.dbMeta.Tables {
		// FIXME: it seems a little bit of redundance. Simplify it in the future.  @chendahui
		_, ok := dbInfo.Tables[tbl]
		if !ok {
			common.AppLogger.Warnf("table info not found : %s", tbl)
			continue
		}
		tables = append(tables, common.UniqueTable(dbInfo.Name, tbl))
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
}

type RestoreWorker struct {
	ID int64
}

func NewRestoreWorkerPool(limit int) *RestoreWorkerPool {
	workers := make(chan *RestoreWorker, limit)
	for i := 0; i < limit; i++ {
		workers <- &RestoreWorker{ID: int64(i + 1)}
	}

	return &RestoreWorkerPool{
		limit:   limit,
		workers: workers,
	}
}

func (pool *RestoreWorkerPool) Apply() *RestoreWorker         { return <-pool.workers }
func (pool *RestoreWorkerPool) Recycle(worker *RestoreWorker) { pool.workers <- worker }

////////////////////////////////////////////////////////////////

const (
	statPending  string = "pending"
	statRunning  string = "running"
	statFinished string = "finished"
	statFailed   string = "failed"
)

type restoreCallback func(ctx context.Context, regionID int, maxRowID int64, rows uint64, checksum *verify.KVChecksum) error

type regionRestoreTask struct {
	status   string
	region   *mydump.TableRegion
	executor *RegionRestoreExectuor
	encoders *kvEncoderPool
	delivers *kv.KVDeliverKeeper
	// TODO : progress ...
	callback restoreCallback
}

func newRegionRestoreTask(
	region *mydump.TableRegion,
	executor *RegionRestoreExectuor,
	encoders *kvEncoderPool,
	delivers *kv.KVDeliverKeeper,
	callback restoreCallback) *regionRestoreTask {

	return &regionRestoreTask{
		status:   statPending,
		region:   region,
		executor: executor,
		delivers: delivers,
		encoders: encoders,
		callback: callback,
	}
}

func (t *regionRestoreTask) Run(ctx context.Context) error {
	timer := time.Now()
	region := t.region
	table := common.UniqueTable(region.DB, region.Table)
	common.AppLogger.Infof("[%s] restore region [%s]", table, region.Name())

	t.status = statRunning
	maxRowID, rows, checksum, err := t.run(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	common.AppLogger.Infof("[%s] restore region [%s] takes %v", table, region.Name(), time.Since(timer))
	err = t.callback(ctx, region.ID, maxRowID, rows, checksum)
	if err != nil {
		return errors.Trace(err)
	}
	t.status = statFinished

	return nil
}

func (t *regionRestoreTask) run(ctx context.Context) (int64, uint64, *verify.KVChecksum, error) {
	kvEncoder := t.encoders.Apply()
	defer t.encoders.Recycle(kvEncoder)

	kvDeliver := t.delivers.AcquireClient(t.executor.dbInfo.Name, t.executor.tableInfo.Name)
	// cause bug here.
	defer t.delivers.RecycleClient(kvDeliver)

	nextRowID, affectedRows, checksum, err := t.executor.Run(ctx, t.region, kvEncoder, kvDeliver)
	return nextRowID, affectedRows, checksum, errors.Trace(err)
}

////////////////////////////////////////////////////////////////

type kvEncoderPool struct {
	mux       sync.Mutex
	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encoders  []*kv.TableKVEncoder
	sqlMode   string
	idAlloc   *kvec.Allocator
}

func newKvEncoderPool(
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	tableMeta *mydump.MDTableMeta, sqlMode string) *kvEncoderPool {

	idAllocator := kvec.NewAllocator()

	return &kvEncoderPool{
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
		encoders:  []*kv.TableKVEncoder{},
		sqlMode:   sqlMode,
		idAlloc:   idAllocator,
	}
}

func (p *kvEncoderPool) init(size int) *kvEncoderPool {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.encoders = make([]*kv.TableKVEncoder, 0, size)

	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ec, err := kv.NewTableKVEncoder(
				p.dbInfo.Name, p.tableInfo.Name, p.tableInfo.ID,
				p.tableInfo.Columns, p.tableInfo.CreateTableStmt, p.sqlMode, p.idAlloc)
			if err == nil {
				p.encoders = append(p.encoders, ec)
			}
		}()
	}

	return p
}

func (p *kvEncoderPool) Apply() *kv.TableKVEncoder {
	p.mux.Lock()
	defer p.mux.Unlock()

	size := len(p.encoders)
	if size == 0 {
		encoder, err := kv.NewTableKVEncoder(
			p.dbInfo.Name, p.tableInfo.Name, p.tableInfo.ID,
			p.tableInfo.Columns, p.tableInfo.CreateTableStmt, p.sqlMode, p.idAlloc)
		if err != nil {
			common.AppLogger.Errorf("failed to new kv encoder (%s) : %s", p.dbInfo.Name, err.Error())
			return nil
		}

		p.encoders = append(p.encoders, encoder)
		size = 1
	}

	encoder := p.encoders[size-1]
	p.encoders = p.encoders[:size-1]
	return encoder
}

func (p *kvEncoderPool) Recycle(encoder *kv.TableKVEncoder) {
	if encoder != nil {
		p.mux.Lock()
		p.encoders = append(p.encoders, encoder)
		p.mux.Unlock()
	}
}

func (p *kvEncoderPool) Clear() {
	p.mux.Lock()
	defer p.mux.Unlock()

	for _, encoder := range p.encoders {
		encoder.Close()
	}
	p.encoders = p.encoders[:0]
}

////////////////////////////////////////////////////////////////

type TableRestore struct {
	wg  sync.WaitGroup
	mux sync.Mutex
	ctx context.Context

	cfg         *config.Config
	dbInfo      *TidbDBInfo
	tableInfo   *TidbTableInfo
	tableMeta   *mydump.MDTableMeta
	encoders    *kvEncoderPool
	deliversMgr *kv.KVDeliverKeeper

	regions          []*mydump.TableRegion
	tasks            []*regionRestoreTask
	handledRegions   map[int]*regionStat
	localChecksum    *verify.KVChecksum
	tableMaxRowID    int64
	tableRows        uint64
	postProcessError chan error
}

type regionStat struct {
	maxRowID int64
	rows     uint64
	checksum *verify.KVChecksum
}

func NewTableRestore(
	ctx context.Context,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	tableMeta *mydump.MDTableMeta,
	cfg *config.Config,
	deliverMgr *kv.KVDeliverKeeper,
) *TableRestore {

	encoders := newKvEncoderPool(dbInfo, tableInfo, tableMeta, cfg.TiDB.SQLMode)
	encoders.init(cfg.App.WorkerPoolSize)

	tr := &TableRestore{
		ctx:            ctx,
		cfg:            cfg,
		dbInfo:         dbInfo,
		tableInfo:      tableInfo,
		tableMeta:      tableMeta,
		encoders:       encoders,
		deliversMgr:    deliverMgr,
		handledRegions: make(map[int]*regionStat),
	}
	tr.loadRegions()

	return tr
}

func (tr *TableRestore) Close() {
	// TODO : flush table meta right now ~
	tr.encoders.Clear()
	common.AppLogger.Infof("[%s] restore done", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name))
}

func (tr *TableRestore) loadRegions() {
	timer := time.Now()
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	common.AppLogger.Infof("[%s] load regions", table)

	founder := mydump.NewRegionFounder(tr.cfg.Mydumper.MinRegionSize)
	regions := founder.MakeTableRegions(tr.tableMeta)

	tasks := make([]*regionRestoreTask, 0, len(regions))
	for _, region := range regions {
		executor := NewRegionRestoreExectuor(tr.cfg, tr.dbInfo, tr.tableInfo, tr.tableMeta)
		task := newRegionRestoreTask(region, executor, tr.encoders, tr.deliversMgr, tr.onRegionFinished)
		tasks = append(tasks, task)
		common.AppLogger.Debugf("[%s] region - %s", table, region.Name())
	}

	tr.regions = regions
	tr.tasks = tasks

	common.AppLogger.Infof("[%s] load %d regions takes %v", table, len(regions), time.Since(timer))
	return
}

func (tr *TableRestore) onRegionFinished(ctx context.Context, id int, maxRowID int64, rows uint64, checksum *verify.KVChecksum) error {
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	tr.mux.Lock()
	defer tr.mux.Unlock()

	tr.handledRegions[id] = &regionStat{
		maxRowID: maxRowID,
		rows:     rows,
		checksum: checksum,
	}

	total := len(tr.regions)
	handled := len(tr.handledRegions)
	common.AppLogger.Infof("[%s] handled region count = %d (%s)", table, handled, common.Percent(handled, total))

	return nil
}

func (tr *TableRestore) restoreTableMeta(ctx context.Context) error {
	timer := time.Now()
	dsn := tr.cfg.TiDB
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		// let it failed and record it to log.
		common.AppLogger.Warnf("connect db failed %v, the next operation is: ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d; you should do it manually", err, tr.tableMeta.DB, tr.tableMeta.Name, tr.tableMaxRowID)
		return nil
	}
	defer db.Close()

	err = AlterAutoIncrement(ctx, db, tr.tableMeta.DB, tr.tableMeta.Name, tr.tableMaxRowID)
	if err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] alter table set auto_id takes %v", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name), time.Since(timer))
	return nil
}

func (tr *TableRestore) importKV() error {
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	common.AppLogger.Infof("[%s] flush kv deliver ...", table)

	start := time.Now()
	defer func() {
		metrics.MarkTiming(fmt.Sprintf("[%s]_kv_flush", table), start)
		common.AppLogger.Infof("[%s] kv deliver all flushed !", table)
	}()

	// FIXME: flush is an asynchronous operation, what if flush failed?
	if err := tr.deliversMgr.Flush(table); err != nil {
		common.AppLogger.Errorf("[%s] falied to flush kvs : %s", table, err.Error())
		return errors.Trace(err)
	}

	return nil
}

// do checksum for each table.
func (tr *TableRestore) checksum(ctx context.Context) error {
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	if !tr.cfg.PostRestore.Checksum {
		common.AppLogger.Infof("[%s] Skip checksum.", table)
		return nil
	}

	remoteChecksum, err := DoChecksum(ctx, tr.cfg.TiDB, table)
	if err != nil {
		return errors.Trace(err)
	}

	localChecksum := tr.localChecksum
	if localChecksum == nil {
		common.AppLogger.Warnf("[%s] no local checksum, remote checksum is %+v", table, remoteChecksum)
		return nil
	}
	if remoteChecksum.Checksum != localChecksum.Sum() || remoteChecksum.TotalKVs != localChecksum.SumKVS() || remoteChecksum.TotalBytes != localChecksum.SumSize() {
		common.AppLogger.Errorf("[%s] checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
			table, remoteChecksum.Checksum, localChecksum.Sum(), remoteChecksum.TotalKVs, localChecksum.SumKVS(), remoteChecksum.TotalBytes, localChecksum.SumSize())
		return nil
	}

	common.AppLogger.Infof("[%s] checksum pass", table)
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
	common.QueryRowWithRetry(ctx, db, query, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes)
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

type RegionRestoreExectuor struct {
	cfg *config.Config

	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
}

func NewRegionRestoreExectuor(
	cfg *config.Config,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	tableMeta *mydump.MDTableMeta) *RegionRestoreExectuor {

	exc := &RegionRestoreExectuor{
		cfg:       cfg,
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
	}

	return exc
}

func (exc *RegionRestoreExectuor) Run(
	ctx context.Context,
	region *mydump.TableRegion,
	kvEncoder *kv.TableKVEncoder,
	kvDeliver kv.KVDeliver) (nextRowID int64, affectedRows uint64, checksum *verify.KVChecksum, err error) {

	/*
		Flows :
			1. read mydump file
			2. sql -> kvs
			3. load kvs data (into kv deliver server)
			4. flush kvs data (into tikv node)
	*/
	reader, err := mydump.NewRegionReader(region.File, region.Offset, region.Size)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	defer reader.Close()

	table := common.UniqueTable(exc.tableMeta.DB, exc.tableMeta.Name)
	readMark := fmt.Sprintf("[%s]_read_file", table)
	encodeMark := fmt.Sprintf("[%s]_sql_2_kv", table)
	deliverMark := fmt.Sprintf("[%s]_deliver_write", table)

	rows := uint64(0)
	checksum = verify.NewKVChecksum(0)
	/*
		TODO :
			So far, since checksum can not recompute on the same key-value pair,
			otherwise this would leads to an incorrect checksum value finally.
			So it's important to guarantee that do checksum on kvs correctly
			no matter what happens during process of restore ( eg. safe point / error retry ... )
	*/

	for {
		select {
		case <-ctx.Done():
			return kvEncoder.NextRowID(), rows, checksum, errCtxAborted
		default:
		}

		start := time.Now()
		sqls, err := reader.Read(defReadBlockSize)
		if errors.Cause(err) == io.EOF {
			break
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
				return kvEncoder.NextRowID(), rows, checksum, errors.Trace(err)
			}

			// kv -> deliver ( -> tikv )
			start = time.Now()
			err = kvDeliver.Put(kvs)
			metrics.MarkTiming(deliverMark, start)

			if err != nil {
				// TODO : retry ~
				common.AppLogger.Errorf("kv deliver failed = %s\n", err.Error())
				return kvEncoder.NextRowID(), rows, checksum, errors.Trace(err)
			}

			checksum.Update(kvs)
			rows += affectedRows
		}
		// TODO .. record progress on this region
	}

	// TODO :
	//		It's really necessary to statistic total num of kv pairs for debug tracing !!!

	return kvEncoder.NextRowID(), rows, checksum, nil
}
