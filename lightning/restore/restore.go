package restore

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	tidbcfg "github.com/pingcap/tidb/config"
	kvec "github.com/pingcap/tidb/util/kvencoder"
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

type RestoreControlloer struct {
	mux sync.RWMutex
	wg  sync.WaitGroup

	cfg    *config.Config
	dbMeta *mydump.MDDatabaseMeta
	dbInfo *TidbDBInfo

	localChecksums map[string]*verify.KVChecksum

	// statDB         *sql.DB
	// statDbms       *ProgressDBMS
	// tablesProgress map[string]*TableProgress
}

func NewRestoreControlloer(dbMeta *mydump.MDDatabaseMeta, cfg *config.Config) *RestoreControlloer {
	// store := cfg.ProgressStore
	// statDB := ConnectDB(store.Host, store.Port, store.User, store.Psw)
	// statDbms := NewProgressDBMS(statDB, store.Database)

	return &RestoreControlloer{
		cfg:            cfg,
		dbMeta:         dbMeta,
		localChecksums: make(map[string]*verify.KVChecksum),
		// statDB: statDB,
		// statDbms: statDbms,
	}
}

func (rc *RestoreControlloer) Close() {
	// rc.statDB.Close()
}

func (rc *RestoreControlloer) Run(ctx context.Context) {
	opts := []func(context.Context) error{
		rc.restoreSchema,
		// rc.recoverProgress,
		rc.restoreTables,
		rc.compact,
		rc.checksum,
		rc.analyze,
	}

	for _, process := range opts {
		err := process(ctx)
		if errors.Cause(err) == errCtxAborted {
			break
		}
		if err != nil {
			log.Errorf("run cause error : %s", errors.ErrorStack(err))
			break // ps : not continue
		}
	}

	// show metrics
	statistic := metrics.DumpTiming()
	log.Infof("Timing statistic :\n%s", statistic)

	return
}

func (rc *RestoreControlloer) restoreSchema(ctx context.Context) error {
	tidbMgr, err := NewTiDBManager(rc.cfg.TiDB)
	if err != nil {
		return errors.Trace(err)
	}
	defer tidbMgr.Close()

	database := rc.dbMeta.Name

	if !rc.cfg.Mydumper.NoSchema {
		timer := time.Now()
		log.Infof("restore table schema for `%s`", rc.dbMeta.Name)
		tablesSchema := make(map[string]string)
		for tbl, tblMeta := range rc.dbMeta.Tables {
			tablesSchema[tbl] = tblMeta.GetSchema()
		}
		err = tidbMgr.InitSchema(ctx, database, tablesSchema)
		if err != nil {
			return errors.Errorf("db schema failed to init : %v", err)
		}
		log.Infof("restore table schema for `%s` takes %v", rc.dbMeta.Name, time.Since(timer))
	}
	dbInfo, err := tidbMgr.LoadSchemaInfo(ctx, database)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dbInfo = dbInfo
	return nil
}

func (rc *RestoreControlloer) restoreTables(ctx context.Context) error {
	timer := time.Now()
	// tables' restoring mission
	tablesRestoring := make([]*TableRestore, 0, len(rc.dbMeta.Tables))
	defer func() {
		for _, tr := range tablesRestoring {
			tr.Close()
		}
	}()

	dbInfo := rc.dbInfo
	for tbl, tableMeta := range rc.dbMeta.Tables {
		tableInfo, ok := dbInfo.Tables[tbl]
		if !ok {
			return errors.Errorf("table info %s not found", tbl)
		}

		tablesRestoring = append(tablesRestoring, NewTableRestore(
			ctx, dbInfo, tableInfo, tableMeta, rc.cfg, rc.localChecksums,
		))
	}

	// TODO .. sort table by table ?

	// tables' region restore task
	tasks := make([]*regionRestoreTask, 0, len(tablesRestoring))
	for _, tr := range tablesRestoring {
		tasks = append(tasks, tr.tasks...)
	}

	workers := NewRestoreWorkerPool(rc.cfg.App.WorkerPoolSize)

	go func() {
		ticker := time.NewTicker(time.Minute * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				statistic := metrics.DumpTiming()
				log.Infof("Timing statistic :\n%s", statistic)
			}
		}
	}()

	skipTables := make(map[string]struct{})

	var wg sync.WaitGroup
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

		worker := workers.Apply()
		wg.Add(1)
		go func(w *RestoreWorker, t *regionRestoreTask) {
			defer workers.Recycle(w)
			defer wg.Done()
			table := common.UniqueTable(t.region.DB, t.region.Table)
			if _, ok := skipTables[table]; ok {
				log.Infof("something wrong with table %s before, so skip region %s", table, t.region.Name())
				return
			}
			err := t.Run(ctx)
			if err != nil {
				log.Errorf("table %s region %s run task error %s", table, t.region.Name(), errors.ErrorStack(err))
				skipTables[table] = struct{}{}
			}

		}(worker, task)
	}
	wg.Wait() // TODO ... ctx abroted
	log.Infof("restore table data takes %v", time.Since(timer))

	return nil
}

// do compaction for the whole data.
func (rc *RestoreControlloer) compact(ctx context.Context) error {
	if !rc.cfg.PostRestore.Compact {
		log.Info("Skip compaction.")
		return nil
	}

	cli, err := kv.NewKVDeliverClient(ctx, uuid.Nil, rc.cfg.TikvImporter.Addr, rc.cfg.TiDB.PdAddr, "")
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	if err := cli.Compact([]byte{}, []byte{}); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// do checksum for each table.
func (rc *RestoreControlloer) checksum(ctx context.Context) error {
	if !rc.cfg.PostRestore.Checksum {
		log.Info("Skip checksum.")
		return nil
	}

	tables := rc.getTables()
	remoteChecksums, err := DoChecksum(ctx, rc.cfg.TiDB, tables)
	if err != nil {
		return errors.Trace(err)
	}

	for _, remoteChecksum := range remoteChecksums {
		table := common.UniqueTable(remoteChecksum.Schema, remoteChecksum.Table)
		localChecksum, ok := rc.localChecksums[table]
		if !ok {
			log.Warnf("[%s] no local checksum, remote checksum is %v", table, remoteChecksum)
			continue
		}

		if remoteChecksum.Checksum != localChecksum.Sum() || remoteChecksum.TotalKVs != localChecksum.SumKVS() || remoteChecksum.TotalBytes != localChecksum.SumSize() {
			log.Errorf("[%s] checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)",
				table, remoteChecksum.Checksum, localChecksum.Sum(), remoteChecksum.TotalKVs, localChecksum.SumKVS(), remoteChecksum.TotalBytes, localChecksum.SumSize())
			continue
		}

		log.Infof("[%s] checksum pass", table)
	}

	return nil
}

// analyze will analyze table for all tables.
func (rc *RestoreControlloer) analyze(ctx context.Context) error {
	if !rc.cfg.PostRestore.Analyze {
		log.Info("Skip analyze table.")
		return nil
	}

	tables := rc.getTables()
	err := analyzeTable(ctx, rc.cfg.TiDB, tables)
	return errors.Trace(err)
}

// getTables returns a table list, which table format is `db`.`table`.
func (rc *RestoreControlloer) getTables() []string {
	tables := make([]string, 0, len(rc.dbMeta.Tables))
	dbInfo := rc.dbInfo
	for tbl := range rc.dbMeta.Tables {
		// FIXME: it seems a little bit of redundance. Simplify it in the future.  @chendahui
		_, ok := dbInfo.Tables[tbl]
		if !ok {
			log.Warnf("table info not found : %s", tbl)
			continue
		}
		tables = append(tables, common.UniqueTable(dbInfo.Name, tbl))
	}
	return tables
}

func analyzeTable(ctx context.Context, dsn config.DBStore, tables []string) error {
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		log.Errorf("connect db failed %v, the next operation is: ANALYZE TABLE. You should do it one by one manually", err)
		return errors.Trace(err)
	}
	defer db.Close()

	// speed up executing analyze table temporarily
	setSessionVarInt(ctx, db, "tidb_build_stats_concurrency", 16)
	setSessionVarInt(ctx, db, "tidb_distsql_scan_concurrency", dsn.DistSQLScanConcurrency)

	for _, table := range tables {
		timer := time.Now()
		log.Infof("[%s] analyze", table)
		query := fmt.Sprintf("ANALYZE TABLE %s", table)
		err := common.ExecWithRetry(ctx, db, []string{query})
		if err != nil {
			log.Errorf("%s error %s", query, errors.ErrorStack(err))
			continue
		}
		log.Infof("[%s] analyze takes %v", table, time.Since(timer))
	}

	return nil
}

////////////////////////////////////////////////////////////////

func setSessionVarInt(ctx context.Context, db *sql.DB, name string, value int) {
	stmt := fmt.Sprintf("set session %s = %d", name, value)
	if err := common.ExecWithRetry(ctx, db, []string{stmt}); err != nil {
		log.Warnf("failed to set variable @%s to %d: %s", name, value, err.Error())
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
	log.Infof("[%s] restore region [%s]", table, region.Name())

	t.status = statRunning
	maxRowID, rows, checksum, err := t.run(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[%s] restore region [%s] takes %v", table, region.Name(), time.Since(timer))
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
			log.Errorf("failed to new kv encoder (%s) : %s", p.dbInfo.Name, err.Error())
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

	regions        []*mydump.TableRegion
	id2regions     map[int]*mydump.TableRegion
	tasks          []*regionRestoreTask
	handledRegions map[int]*regionStat
	localChecksums map[string]*verify.KVChecksum
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
	localChecksums map[string]*verify.KVChecksum) *TableRestore {

	encoders := newKvEncoderPool(dbInfo, tableInfo, tableMeta, cfg.TiDB.SQLMode)
	encoders.init(cfg.App.WorkerPoolSize)

	tr := &TableRestore{
		ctx:            ctx,
		cfg:            cfg,
		dbInfo:         dbInfo,
		tableInfo:      tableInfo,
		tableMeta:      tableMeta,
		encoders:       encoders,
		deliversMgr:    kv.NewKVDeliverKeeper(cfg.TikvImporter.Addr, cfg.TiDB.PdAddr),
		handledRegions: make(map[int]*regionStat),
		localChecksums: localChecksums,
	}

	timer := time.Now()
	tr.loadRegions()
	log.Infof("[%s] load regions takes %v", common.UniqueTable(tableMeta.DB, tableMeta.Name), time.Since(timer))

	return tr
}

func (tr *TableRestore) Close() {
	// TODO : flush table meta right now ~
	tr.encoders.Clear()
	log.Infof("[%s] closed", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name))
}

func (tr *TableRestore) loadRegions() {
	log.Infof("[%s] load regions", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name))

	founder := mydump.NewRegionFounder(tr.cfg.Mydumper.MinRegionSize)
	regions := founder.MakeTableRegions(tr.tableMeta)

	id2regions := make(map[int]*mydump.TableRegion)
	for _, region := range regions {
		log.Infof("[%s] region - %s", common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name), region.Name())
		id2regions[region.ID] = region
	}

	tasks := make([]*regionRestoreTask, 0, len(regions))
	for _, region := range regions {
		executor := NewRegionRestoreExectuor(tr.cfg, tr.dbInfo, tr.tableInfo, tr.tableMeta)
		task := newRegionRestoreTask(region, executor, tr.encoders, tr.deliversMgr, tr.onRegionFinished)
		tasks = append(tasks, task)
	}

	tr.regions = regions
	tr.id2regions = id2regions
	tr.tasks = tasks
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
	log.Infof("[%s] handled region count = %d (%s)", table, handled, common.Percent(handled, total))
	if handled == len(tr.tasks) {
		err := tr.onFinished(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (tr *TableRestore) onFinished(ctx context.Context) error {
	// flush all kvs into TiKV
	if err := tr.importKV(); err != nil {
		return errors.Trace(err)
	}

	// generate meta kv
	var (
		tableMaxRowID int64
		tableRows     uint64
		checksum      = verify.NewKVChecksum(0)
	)
	for _, regStat := range tr.handledRegions {
		tableRows += regStat.rows
		if regStat.maxRowID > tableMaxRowID {
			tableMaxRowID = regStat.maxRowID
		}
		checksum.Add(regStat.checksum)
	}
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	log.Infof("[%s] local checksum %s", table, checksum)
	tr.localChecksums[table] = checksum

	if err := tr.restoreTableMeta(ctx, tableMaxRowID); err != nil {
		return errors.Trace(err)
	}

	log.Infof("[%s] has imported %d rows", table, tableRows)
	return nil
}

func (tr *TableRestore) restoreTableMeta(ctx context.Context, rowID int64) error {
	dsn := tr.cfg.TiDB
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		// let it failed and record it to log.
		log.Errorf("connect db failed %v, the next operation is: ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d; you should do it manually", err, tr.tableMeta.DB, tr.tableMeta.Name, rowID)
		return errors.Trace(err)
	}
	defer db.Close()

	return errors.Trace(AlterAutoIncrement(ctx, db, tr.tableMeta.DB, tr.tableMeta.Name, rowID))
}

func (tr *TableRestore) importKV() error {
	table := common.UniqueTable(tr.tableMeta.DB, tr.tableMeta.Name)
	log.Infof("[%s] flush kv deliver ...", table)

	kvDeliver := tr.deliversMgr

	start := time.Now()
	defer func() {
		kvDeliver.Close()
		metrics.MarkTiming(fmt.Sprintf("[%s]_kv_flush", table), start)
		log.Infof("[%s] kv deliver all flushed !", table)
	}()

	if err := kvDeliver.Flush(); err != nil {
		log.Errorf("[%s] falied to flush kvs : %s", table, err.Error())
		return errors.Trace(err)
	}

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
func DoChecksum(ctx context.Context, dsn config.DBStore, tables []string) ([]*RemoteChecksum, error) {
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
			log.Errorf("update tikv_gc_life_time error %s", errors.ErrorStack(err))
		}
	}()

	// speed up executing checksum table temporarily
	setSessionVarInt(ctx, db, "tidb_checksum_table_concurrency", 16)
	setSessionVarInt(ctx, db, "tidb_distsql_scan_concurrency", dsn.DistSQLScanConcurrency)

	// ADMIN CHECKSUM TABLE <table>,<table>  example.
	// 	mysql> admin checksum table test.t;
	// +---------+------------+---------------------+-----------+-------------+
	// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
	// +---------+------------+---------------------+-----------+-------------+
	// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
	// +---------+------------+---------------------+-----------+-------------+

	checksums := make([]*RemoteChecksum, 0, len(tables))
	// do table checksum one by one instead of doing all at once to make tikv server comfortable
	for _, table := range tables {
		timer := time.Now()
		cs := RemoteChecksum{}
		log.Infof("[%s] doing remote checksum", table)
		query := fmt.Sprintf("ADMIN CHECKSUM TABLE %s", table)
		common.QueryRowWithRetry(ctx, db, query, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		checksums = append(checksums, &cs)
		log.Infof("[%s] do checksum takes %v", table, time.Since(timer))
	}

	return checksums, nil
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
			log.Debugf("len(kvs) %d, len(sql) %d", len(kvs), len(stmt))
			if err != nil {
				log.Errorf("kv encode failed = %s\n", err.Error())
				return kvEncoder.NextRowID(), rows, checksum, errors.Trace(err)
			}

			// kv -> deliver ( -> tikv )
			start = time.Now()
			err = kvDeliver.Put(kvs)
			metrics.MarkTiming(deliverMark, start)

			if err != nil {
				// TODO : retry ~
				log.Errorf("kv deliver failed = %s\n", err.Error())
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
