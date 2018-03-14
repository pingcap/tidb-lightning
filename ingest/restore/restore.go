package restore

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/ingest/common"
	"github.com/pingcap/tidb-lightning/ingest/config"
	"github.com/pingcap/tidb-lightning/ingest/kv"
	"github.com/pingcap/tidb-lightning/ingest/mydump"
	tidbcfg "github.com/pingcap/tidb/config"
)

var (
	errCtxAborted = errors.New("context aborted error")
	metrics       = common.NewMetrics()
	concurrency   = 1
)

func init() {
	concurrency = runtime.NumCPU() // TODO ... config

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

	// statDB         *sql.DB
	// statDbms       *ProgressDBMS
	// tablesProgress map[string]*TableProgress
}

func NewRestoreControlloer(dbMeta *mydump.MDDatabaseMeta, cfg *config.Config) *RestoreControlloer {
	// store := cfg.ProgressStore
	// statDB := ConnectDB(store.Host, store.Port, store.User, store.Psw)
	// statDbms := NewProgressDBMS(statDB, store.Database)

	return &RestoreControlloer{
		cfg:    cfg,
		dbMeta: dbMeta,
		// statDB: statDB,
		// statDbms: statDbms,
	}
}

func (rc *RestoreControlloer) Close() {
	// rc.statDB.Close()
}

func (rc *RestoreControlloer) Run(ctx context.Context) {
	log.Info("restore controlloer start running !")

	opts := []func(context.Context) error{
		rc.restoreSchema,
		// rc.recoverProgress,
		rc.restoreTables,
	}

	for _, process := range opts {
		err := process(ctx)
		if err == errCtxAborted {
			break
		}
		if err != nil {
			log.Errorf("run cause error : %s", err.Error())
			break // ps : not continue
		}
	}

	// show metrics
	statistic := metrics.DumpTiming()
	log.Warnf("Timing statistic :\n%s", statistic)

	return
}

func (rc *RestoreControlloer) restoreSchema(ctx context.Context) error {
	log.Info("Restore db/table schema ~")

	tidbMgr, err := NewTiDBManager(rc.cfg.PdAddr)
	if err != nil {
		log.Errorf("create tidb manager failed : %v", err)
		return err
	}
	defer tidbMgr.Close()

	database := rc.dbMeta.Name
	tablesSchema := make(map[string]string)
	for tbl, tblMeta := range rc.dbMeta.Tables {
		tablesSchema[tbl] = tblMeta.GetSchema()
	}

	err = tidbMgr.InitSchema(database, tablesSchema)
	if err != nil {
		return errors.Errorf("db schema failed to init : %v", err)
	}
	// TODO : check tables' schema
	rc.dbInfo = tidbMgr.SyncSchema(database)

	return nil
}

func (rc *RestoreControlloer) restoreTables(ctx context.Context) error {
	log.Info("start to restore tables ~")

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
			log.Warnf("table info not found : %s", tbl)
			continue
		}

		tablesRestoring = append(tablesRestoring, NewTableRestore(
			ctx, dbInfo, tableInfo, tableMeta, rc.cfg,
		))
	}

	// TODO .. sort table by table ?

	// tables' region restore task
	tasks := make([]*regionRestoreTask, 0, len(tablesRestoring))
	for _, tr := range tablesRestoring {
		tasks = append(tasks, tr.tasks...)
	}

	workers := NewRestoreWorkerPool(concurrency)

	go func() {
		ticker := time.NewTicker(time.Minute * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				statistic := metrics.DumpTiming()
				log.Warnf("Timing statistic :\n%s", statistic)
			}
		}
	}()

	var wg sync.WaitGroup
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

		worker := workers.Apply()
		wg.Add(1)
		log.Warnf("region allowed to run >>>>>> [%s]", task.region.Name())
		go func(w *RestoreWorker, t *regionRestoreTask) {
			defer wg.Done()
			defer workers.Recycle(w)
			t.Run(ctx)
		}(worker, task)
	}
	wg.Wait() // TODO ... ctx abroted

	return nil
}

////////////////////////////////////////////////////////////////

// TODO ... find another way to caculate
func adjustUUID(uuid string, length int) string {
	size := len(uuid)
	if size > length {
		uuid = uuid[size-length:]
	} else if size < length {
		uuid = uuid + strings.Repeat("+", length-size)
	}
	return uuid
}

func makeKVDeliver(
	ctx context.Context,
	cfg *config.Config,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo) (kv.KVDeliver, error) {

	uuid := adjustUUID(fmt.Sprintf("%s_%s", dbInfo.Name, tableInfo.Name), 16)
	return kv.NewKVDeliverClient(ctx, uuid, cfg.KvIngest.Backend)
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

type restoreCallback func(regionID int, maxRowID int64, rows uint64, checksum *KVChecksum, err error)

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

func (t *regionRestoreTask) Run(ctx context.Context) {
	region := t.region
	log.Infof("Start restore region : [%s] ...", t.region.Name())

	t.status = statRunning
	maxRowID, rows, checksum, err := t.run(ctx)
	if err != nil {
		log.Errorf("Table region (%s) restore failed : %s", region.Name(), err.Error())
	}

	log.Infof("Finished restore region : [%s]", region.Name())
	t.callback(region.ID, maxRowID, rows, checksum, err)
	t.status = statFinished

	return
}

func (t *regionRestoreTask) run(ctx context.Context) (int64, uint64, *KVChecksum, error) {
	kvEncoder := t.encoders.Apply()
	defer t.encoders.Recycle(kvEncoder)

	kvDeliver := t.delivers.AcquireClient(t.executor.dbInfo.Name, t.executor.tableInfo.Name)
	defer t.delivers.RecycleClient(kvDeliver)

	return t.executor.Run(ctx, t.region, kvEncoder, kvDeliver)
}

////////////////////////////////////////////////////////////////

type kvEncoderPool struct {
	mux       sync.Mutex
	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *mydump.MDTableMeta
	encoders  []*kv.TableKVEncoder
}

func newKvEncoderPool(
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	tableMeta *mydump.MDTableMeta) *kvEncoderPool {
	return &kvEncoderPool{
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
		encoders:  []*kv.TableKVEncoder{},
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
				p.dbInfo.Name, p.dbInfo.ID,
				p.tableInfo.Name, p.tableInfo.ID,
				p.tableInfo.Columns, p.tableMeta.GetSchema())
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
			p.dbInfo.Name, p.dbInfo.ID,
			p.tableInfo.Name, p.tableInfo.ID,
			p.tableInfo.Columns, p.tableMeta.GetSchema())
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
}

type regionStat struct {
	maxRowID int64
	rows     uint64
	checksum *KVChecksum
}

func NewTableRestore(
	ctx context.Context,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	tableMeta *mydump.MDTableMeta,
	cfg *config.Config) *TableRestore {

	tr := &TableRestore{
		ctx:            ctx,
		cfg:            cfg,
		dbInfo:         dbInfo,
		tableInfo:      tableInfo,
		tableMeta:      tableMeta,
		encoders:       newKvEncoderPool(dbInfo, tableInfo, tableMeta).init(concurrency),
		deliversMgr:    kv.NewKVDeliverKeeper(cfg.KvIngest.Backend),
		handledRegions: make(map[int]*regionStat),
	}

	s := time.Now()
	tr.loadRegions()
	log.Infof("[%s] load regions cost = %.2f sec", tableInfo.Name, time.Since(s).Seconds())

	return tr
}

func (tr *TableRestore) Close() {
	// TODO : flush table meta right now ~
	tr.encoders.Clear()
	log.Infof("[%s] closed !", tr.tableMeta.Name)
}

func (tr *TableRestore) loadRegions() {
	log.Infof("[%s] load regions !", tr.tableMeta.Name)

	// TODO : regionProgress & !regionProgress.Finished()

	preAllocateRowsID := !tr.tableInfo.WithIntegerPrimaryKey()
	founder := mydump.NewRegionFounder(tr.cfg.Mydumper.MinRegionSize)
	regions := founder.MakeTableRegions(tr.tableMeta, preAllocateRowsID)

	table := tr.tableMeta.Name
	for _, region := range regions {
		log.Warnf("[%s] region - %s", table, region.Name())
	}

	id2regions := make(map[int]*mydump.TableRegion)
	for _, region := range regions {
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

func (tr *TableRestore) onRegionFinished(id int, maxRowID int64, rows uint64, checksum *KVChecksum, err error) {
	table := tr.tableInfo.Name
	tr.mux.Lock()
	defer tr.mux.Unlock()

	region := tr.id2regions[id]
	if err != nil {
		log.Errorf("[%s] region (%s) restore failed : %s",
			table, region.Name(), err.Error())
		return
	}

	tr.handledRegions[id] = &regionStat{
		maxRowID: maxRowID,
		rows:     rows,
		checksum: checksum,
	}

	total := len(tr.regions)
	handled := len(tr.handledRegions)
	log.Infof("[%s] handled region count = %d (%s)", table, handled, common.Percent(handled, total))
	if handled == len(tr.tasks) {
		tr.onFinished()
	}

	return
}

func (tr *TableRestore) makeKVDeliver() (kv.KVDeliver, error) {
	return makeKVDeliver(tr.ctx, tr.cfg, tr.dbInfo, tr.tableInfo)
}

func (tr *TableRestore) onFinished() {
	// generate meta kv
	var (
		tableMaxRowID int64       = 0
		tableRows     uint64      = 0
		checksum      *KVChecksum = NewKVChecksum(0)
	)
	for _, regStat := range tr.handledRegions {
		tableRows += regStat.rows
		if regStat.maxRowID > tableMaxRowID {
			tableMaxRowID = regStat.maxRowID
		}
		checksum.Add(regStat.checksum)
	}

	tr.restoreTableMeta(tableMaxRowID)

	// flush all kvs into TiKV ~
	tr.ingestKV()

	// verify table data
	tr.verifyTable(tableRows, checksum)

	return
}

func (tr *TableRestore) restoreTableMeta(rowID int64) error {
	encoder := tr.encoders.Apply()
	defer tr.encoders.Recycle(encoder)

	table := tr.tableInfo.Name
	log.Infof("[%s] restore table meta (row_id = %d)", table, rowID)

	kvDeliver := tr.deliversMgr.AcquireClient(tr.dbInfo.Name, table)
	defer tr.deliversMgr.RecycleClient(kvDeliver)

	kvs, err := encoder.BuildMetaKvs(rowID)
	if err != nil {
		log.Errorf("failed to generate meta key (row_id = %d) : %s", table, rowID, err.Error())
		return errors.Trace(err)
	}

	if err = kvDeliver.Put(kvs); err != nil {
		log.Errorf("[%s] meta key deliver failed : %s", table, err.Error())
		return errors.Trace(err)
	}

	return nil
}

func (tr *TableRestore) ingestKV() error {
	table := tr.tableInfo.Name
	log.Infof("[%s] to flush kv deliver ...", table)

	// kvDeliver, _ := tr.makeKVDeliver()
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

	if err := kvDeliver.Compact(); err != nil {
		log.Errorf("[%s] falied to compact kvs : %s", table, err.Error())
		return errors.Trace(err)
	}

	return nil
}

func (tr *TableRestore) verifyTable(rows uint64) error {
	table := tr.tableInfo.Name
	log.Infof("[%s] verifying table ...", table)

	start := time.Now()
	defer func() {
		metrics.MarkTiming(fmt.Sprintf("[%s]_verify", table), start)
		log.Infof("[%s] finish verification", table)
	}()

	// total num
	if err := tr.verifyQuantity(rows); err != nil {
		log.Errorf("[%s] verify quantity failed : %s", table, err.Error())
		return err
	}
	log.Infof("[%s] owns %d rows integrallty !", table, rows)

	// admin check table
	if tr.cfg.Verify.RunCheckTable {
		if err := tr.excCheckTable(); err != nil {
			log.Errorf("[%s] verify check table failed : %s", table, err.Error())
			return err
		}
	}

	// admin checksum table
	if tr.cfg.Verify.RunChecksumTable {
		log.Infof("[%s] to verify checksum (=%d) ...", table, checksum.Sum())
		if err := tr.verifyChecksum(checksum); err != nil {
			log.Errorf("[%s] verfiy checksum failed : %s", table, err.Error())
			return err
		}
	}

	return nil
}

func (tr *TableRestore) verifyChecksum(expect *KVChecksum) error {
	dsn := tr.cfg.TiDB
	db := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	defer db.Close()

	// ps : checksum command usually take long time to execute,
	//		so here need to expand the gcLifeTime for single transaction.
	oriGCLifeTime, gcErr := ObtainGCLifeTime(db)
	if gcErr == nil {
		gcErr = UpdateGCLifeTime(db, "100h")
	}
	defer func() {
		if gcErr == nil {
			UpdateGCLifeTime(db, oriGCLifeTime)
		}
	}()

	// ps : speed up executing checksum temporarily
	db.Exec("set session tidb_checksum_table_concurrency = 32")

	var checksum, kvs, bytes uint64
	var flag string
	r := db.QueryRow(
		fmt.Sprintf("ADMIN CHECKSUM TABLE %s.%s", tr.tableMeta.DB, tr.tableInfo.Name))
	if err := r.Scan(&flag, &flag, &checksum, &kvs, &bytes); err != nil {
		return err
	}

	if checksum != expect.Sum() || kvs != expect.SumKVS() || bytes != expect.SumSize() {
		return errors.Errorf("checksum mismatch (%d vs %d) (kvs : %d vs %d) (size : %d vs %d)",
			checksum, expect.Sum(), kvs, expect.SumKVS(), bytes, expect.SumSize())
	}

	return nil
}

func (tr *TableRestore) verifyQuantity(expectRows uint64) error {
	dsn := tr.cfg.TiDB
	db := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	defer db.Close()

	rows := uint64(0)
	r := db.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", tr.tableMeta.DB, tr.tableInfo.Name))
	if err := r.Scan(&rows); err != nil {
		return err
	}

	if rows != expectRows {
		return errors.Errorf("[verify] Rows num not equal %d (expect = %d)", rows, expectRows)
	}

	return nil
}

func (tr *TableRestore) excCheckTable() error {
	log.Infof("Verify by execute `admin check table` : %s", tr.tableMeta.Name)

	dsn := tr.cfg.TiDB
	db := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	defer db.Close()

	// verify datas completion via command "admin check table"
	_, err := db.Exec(
		fmt.Sprintf("ADMIN CHECK TABLE %s.%s", tr.tableMeta.DB, tr.tableMeta.Name))
	return err
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
	kvDeliver kv.KVDeliver) (int64, uint64, *KVChecksum, error) {

	/*
		Flows :
			1. read mydump file
			2. sql -> kvs
			3. load kvs data (into kv deliver server)
			4. flush kvs data (into tikv node)
	*/
	reader, _ := mydump.NewRegionReader(region.File, region.Offset, region.Size)
	defer reader.Close()

	table := exc.tableInfo.Name
	readMark := fmt.Sprintf("[%s]_read_file", table)
	encodeMark := fmt.Sprintf("[%s]_sql_2_kv", table)
	deliverMark := fmt.Sprintf("[%s]_deliver_write", table)

	rows := uint64(0)
	checksum := NewKVChecksum(0)

	if region.BeginRowID >= 0 {
		kvEncoder.RebaseRowID(region.BeginRowID)
	}
	for {
		select {
		case <-ctx.Done():
			return kvEncoder.NextRowID(), rows, checksum, errCtxAborted
		default:
		}

		start := time.Now()
		sqls, err := reader.Read(defReadBlockSize)
		if err == io.EOF {
			break
		}
		metrics.MarkTiming(readMark, start)

		for _, stmt := range sqls {
			// sql -> kv
			start = time.Now()
			kvs, affectedRows, err := kvEncoder.Sql2KV(stmt)
			metrics.MarkTiming(encodeMark, start)

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
