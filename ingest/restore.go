package ingest

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"database/sql"
	"path/filepath"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"

	kvec "github.com/pingcap/tidb/util/kvencoder"
)

const (
	kvFlushLimit int = 100000

	defBlockSize int64 = 1024 * 32 // TODO ... config
)

type RestoreControlloer struct {
	mux      sync.RWMutex
	wg       sync.WaitGroup
	ctx      context.Context
	shutdown context.CancelFunc

	cfg     *Config
	dbMeta  *MDDatabaseMeta
	dbInfo  *TidbDBInfo
	tidbMgr *TiDBManager

	statDB         *sql.DB
	statDbms       *ProgressDBMS
	tablesProgress map[string]*TableProgress
}

func NewRestoreControlloer(dbMeta *MDDatabaseMeta, cfg *Config) *RestoreControlloer {
	ctx, shutdown := context.WithCancel(context.Background())

	tidbMgr, err := NewTiDBManager(cfg.PdAddr)
	if err != nil {
		log.Errorf("init tidb manager failed : %v", err)
		return nil
	}

	store := cfg.ProgressStore
	statDB := ConnectDB(store.Host, store.Port, store.User, store.Psw)
	statDbms := NewProgressDBMS(statDB, store.Database)

	rc := &RestoreControlloer{
		ctx:      ctx,
		shutdown: shutdown,
		cfg:      cfg,
		//
		dbMeta:  dbMeta,
		tidbMgr: tidbMgr,
		// progress mark
		statDB:   statDB,
		statDbms: statDbms,
	}
	rc.start()
	return rc
}

func (rc *RestoreControlloer) Close() {
	rc.shutdown()
	rc.wg.Wait()

	rc.tidbMgr.Close()
	rc.statDB.Close()
}

func (rc *RestoreControlloer) start() {
	rc.wg.Add(1)
	go func() {
		rc.run()
		rc.wg.Done()
		log.Info("restore controlloer end !")
	}()
}

func (rc *RestoreControlloer) run() {
	log.Info("restore controlloer start running !")

	// TODO : handle errors from each step

	rc.restoreSchema()
	rc.recoverProgress()
	rc.restoreTablesData()
	rc.verify(rc.ctx)

	return
}

func (rc *RestoreControlloer) recoverProgress() {
	log.Info("Try to recover progress from store.")

	database := rc.dbMeta.Name
	tableCount := len(rc.dbMeta.Tables)
	tables := make([]string, 0, tableCount)
	for tbl, _ := range rc.dbMeta.Tables {
		tables = append(tables, tbl)
	}

	tablesProgress := rc.statDbms.LoadAllProgress(database, tables)
	if len(tablesProgress) == 0 {
		rc.refreshProgress()
		return
	}

	if len(tablesProgress) != tableCount {
		// TODO ..
		log.Warnf(" .... ")
	}

	// TODO ...

	return
}

func (rc *RestoreControlloer) refreshProgress() {
	log.Info("Refresh all progress !")

	tablesProgress := make(map[string]*TableProgress)
	database := rc.dbMeta.Name

	for table, tblMeta := range rc.dbMeta.Tables {
		parts := make(map[string]int64)
		for _, file := range tblMeta.DataFiles {
			fileName := filepath.Base(file)
			fileSize := GetFileSize(file)
			parts[fileName] = fileSize
		}

		tblPrg := NewTableProgress(database, table, parts)
		tablesProgress[table] = tblPrg
	}

	rc.statDbms.Setup(tablesProgress)
	rc.tablesProgress = tablesProgress

	return
}

func (rc *RestoreControlloer) restoreSchema() {
	log.Info("Restore db/table scheam ~")

	database := rc.dbMeta.Name
	tablesSchema := make(map[string]string)
	for tbl, tblMeta := range rc.dbMeta.Tables {
		tablesSchema[tbl] = tblMeta.GetSchema()
	}

	err := rc.tidbMgr.InitSchema(database, tablesSchema)
	if err != nil {
		log.Errorf("restore schema failed : %v", err)
	}

	rc.dbInfo = rc.syncSchema()

	return
}

func (rc *RestoreControlloer) syncSchema() *TidbDBInfo {
	database := rc.dbMeta.Name
	for i := 0; i < 100; i++ { // TODO ... max wait time ~
		done := true
		dbInfo := rc.tidbMgr.LoadSchemaInfo(database)
		for _, tblInfo := range dbInfo.Tables {
			if !tblInfo.Available {
				done = false
				break
			}
		}
		if !done {
			log.Warnf("Not all tables ready yet")
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}

	dbInfo := rc.tidbMgr.LoadSchemaInfo(database)
	log.Infof("Database (%s) ID ====> %d", database, dbInfo.ID)
	for tbl, tblInfo := range dbInfo.Tables {
		log.Infof("Table (%s) ID ====> %d", tbl, tblInfo.ID)
	}
	time.Sleep(time.Second * 5)

	return dbInfo
}

func (rc *RestoreControlloer) restoreTablesData() {
	var wg sync.WaitGroup

	for table, tableMeta := range rc.dbMeta.Tables {
		tableInfo, exists := rc.dbInfo.Tables[table]
		if !exists {
			log.Errorf("Incredible ! Table info not found : %s", table)
			continue
		}

		progress, ok := rc.tablesProgress[table]
		if !ok {
			log.Errorf("table [%s] corresponding progress not found !", table)
			continue
		}

		tblExc, err := NewTableRestoreExecutor(tableMeta, rc.dbInfo, tableInfo, progress, rc.cfg)
		if err != nil {
			log.Errorf("Table (%s) executor init failed  : %s", table, err.Error())
			continue
		}

		wg.Add(1)
		go func(exc *TableRestoreExecutor) {
			defer wg.Done()
			if err := exc.Run(rc.ctx); err != nil {
				log.Errorf("Table (%s) executor run cause error : %s",
					exc.tableMeta.Name, err.Error())
			}
		}(tblExc)
	}

	wg.Wait()
	return
}

func (rc *RestoreControlloer) verify(ctx context.Context) {
	// TODO
	log.Warnf("Please add tables' data verification !")
}

////////////////////////////////////////////////////////////////

type TableRestoreExecutor struct {
	cfg *Config

	/*
		Progress :
			- baseRowID / MaxID ????
			- curTable
			- curTableOffset
	*/

	dbInfo    *TidbDBInfo
	tableInfo *TidbTableInfo
	tableMeta *MDTableMeta
	progress  *TableProgress

	uuid      string
	kvEncoder *TableKVEncoder
	kvDeliver *KVDeliver
}

func NewTableRestoreExecutor(
	tableMeta *MDTableMeta,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	progress *TableProgress,
	cfg *Config) (*TableRestoreExecutor, error) {

	uuid := adjustUUID(fmt.Sprintf("%s_%s", dbInfo.Name, tableInfo.Name), 16)
	kvDeliver, err := NewKVDeliver(uuid, cfg.KvDeliverAddr, cfg.PdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableSchema := tableMeta.GetSchema()
	kvEncoder := NewTableKVEncoder(
		dbInfo.Name, dbInfo.ID, tableInfo.Name, tableInfo.ID, tableSchema, 0)

	exc := &TableRestoreExecutor{
		cfg:       cfg,
		dbInfo:    dbInfo,
		tableInfo: tableInfo,
		tableMeta: tableMeta,
		progress:  progress,

		uuid:      uuid,
		kvEncoder: kvEncoder,
		kvDeliver: kvDeliver,
	}

	return exc, nil
}

func (exc *TableRestoreExecutor) Close() {
	if err := exc.kvDeliver.Close(); err != nil {
		log.Errorf("kv deliver close error : %s", err.Error())
	}
}

func adjustUUID(uuid string, length int) string {
	size := len(uuid)
	if size > length {
		uuid = uuid[size-length:]
	} else if size < length {
		uuid = uuid + strings.Repeat("+", length-size)
	}
	return uuid
}

func (exc *TableRestoreExecutor) Run(ctx context.Context) error {
	exc.execute(ctx)
	// TODO : sum up rows id & set meta key !
	exc.verifyTables(ctx)
	return nil
}

func (exc *TableRestoreExecutor) execute(ctx context.Context) {
	log.Infof("Table resotre executor [%s] start ~", exc.tableMeta.Name)

	// ps : As we only ingest table data row by row ,
	//	so it's very important to keep source ql files in order  ~
	dataFiles := exc.tableMeta.DataFiles
	sort.Strings(dataFiles)

	// convert
	tableProgres := exc.progress
	kvEncoer := exc.kvEncoder
	kvDeliver := exc.kvDeliver
	defer func() {
		if err := kvDeliver.Flush(ctx); err != nil {
			log.Errorf("kv deliver flush error = %v", err)
		} else {
			// ps : so far we flush all after all table's data loaed !
			tableProgres.SetComplete()
		}
	}()

	for _, file := range dataFiles {
		select {
		case <-ctx.Done():
			// TODO ... record progress
			return
		default:
		}

		fileName := filepath.Base(file)
		fileProgress, ok := tableProgres.FilesPrgoress[fileName]
		if !ok {
			log.Errorf("table file [%s] corresponding progress not found !", fileName)
			continue
		}

		if fileProgress.Finished() {
			_, maxRowID := fileProgress.Locate(StageFlushed)
			kvEncoer.RebaseRowID(maxRowID)
			continue
		}

		// TODO : offset recover ~
		err := exc.restoreFile(ctx, file, fileProgress, kvEncoer, kvDeliver)
		if err != nil {
			log.Errorf("[%s] file restore cause error : %s", file, err.Error())
			return
		}
	}

	return
}

func (exc *TableRestoreExecutor) restoreFile(
	ctx context.Context,
	file string,
	progress *TableFileProgress,
	kvEncoder *TableKVEncoder,
	kvDeliver *KVDeliver) error {

	// recover from progress
	offset, rowID := progress.Locate(StageLoaded)
	if offset > 0 {
		kvEncoder.RebaseRowID(rowID)
	}

	// 	Flows :
	//		1. read mydump file
	//		2. sql -> kvs
	//		3. load kvs data (into kv deliver server)
	//		4. flush kvs data (into tikv node)

	reader, _ := NewMDDataReader(file, offset)
	defer reader.Close()

	var sqlData []byte
	var err error
	for {
		select {
		case <-ctx.Done():
			// TODO ... record progress
			return nil
		default:
		}

		sqlData, err = reader.Read(defBlockSize)
		if err == io.EOF {
			log.Infof("file [%s] restore finsh !", file)
			break
		}

		// TODO : optimize strings operation
		sql := string(sqlData)
		parts := strings.Split(sql, ";")
		for _, subSql := range parts {
			subSql = strings.TrimSpace(subSql)
			if len(subSql) == 0 {
				continue
			}

			kvs, _ := kvEncoder.Sql2KV(subSql)
			// table := exc.tableMeta.Name
			// countKey(table, kvs, rows)

			if err = kvDeliver.Put(ctx, kvs); err != nil {
				// TODO : retry ~
				log.Errorf("deliver kv failed = %s\n", err.Error())
			}
		}

		offset := reader.Tell()
		maxRowID := kvEncoder.NextRowID()
		progress.Update(StageLoaded, offset, maxRowID)
	}

	return nil
}

func (exc *TableRestoreExecutor) verifyTables(ctx context.Context) {
	// TODO : execute sql -- "admin check table" & "count(*)""
	log.Warnf("Please add tables verify !")
}

func countKey(table string, kvs []kvec.KvPair, rows uint64) {
	stats := make(map[byte]int)
	for _, p := range kvs {
		switch p.Key[0] {
		case 'm':
			stats[p.Key[0]]++
		case 't':
			stats[p.Key[10]]++
		default:
			stats[p.Key[0]]++
		}
	}

	log.Warnf("(%s) sql rows = %d", table, rows)
	for k, cnt := range stats {
		key := string([]byte{k})
		log.Warnf("(%s) [%s] = %d", table, key, cnt)
	}

	return
}

//////////////////////////////////////////////////

type autoFlushKvDeliver struct {
	kvDeliver      *KVDeliver
	batchSizeLimit int
	sizeCounter    int
}

func newAutoFlushKvDeliver(kvDeliver *KVDeliver, batchSizeLimit int) *autoFlushKvDeliver {
	return &autoFlushKvDeliver{
		kvDeliver:      kvDeliver,
		batchSizeLimit: batchSizeLimit,
		sizeCounter:    0,
	}
}

func (f *autoFlushKvDeliver) Put(ctx context.Context, kvs []kvec.KvPair) error {
	err := f.kvDeliver.Put(ctx, kvs)

	f.sizeCounter += len(kvs)
	if f.sizeCounter >= f.batchSizeLimit {
		f.doFlush(ctx)
		f.sizeCounter = 0
	}

	return err
}

func (f *autoFlushKvDeliver) Flush(ctx context.Context) error {
	log.Warnf("Call flush !!!")

	err := f.kvDeliver.Flush(ctx)
	f.sizeCounter = 0
	return err
}

func (f *autoFlushKvDeliver) doFlush(ctx context.Context) {
	log.Warnf("Auto do flush !!!")

	err := f.kvDeliver.Flush(ctx) // TODO : retry ~
	if err != nil {
		log.Errorf("real kv deliver flush error : %s\n", err.Error())
	}
}
