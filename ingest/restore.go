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
)

const (
	defBlockSize int64 = 1024 * 32 // TODO ... config
)

var (
	errCtxAborted = errors.New("context aborted error")
	metrics       = NewMetrics()
)

type RestoreControlloer struct {
	mux sync.RWMutex
	wg  sync.WaitGroup

	cfg     *Config
	dbMeta  *MDDatabaseMeta
	dbInfo  *TidbDBInfo
	tidbMgr *TiDBManager

	statDB         *sql.DB
	statDbms       *ProgressDBMS
	tablesProgress map[string]*TableProgress
}

func NewRestoreControlloer(dbMeta *MDDatabaseMeta, cfg *Config) *RestoreControlloer {
	store := cfg.ProgressStore
	statDB := ConnectDB(store.Host, store.Port, store.User, store.Psw)
	statDbms := NewProgressDBMS(statDB, store.Database)

	return &RestoreControlloer{
		cfg:      cfg,
		dbMeta:   dbMeta,
		statDB:   statDB,
		statDbms: statDbms,
	}
}

func (rc *RestoreControlloer) Close() {
	rc.statDB.Close()
}

func (rc *RestoreControlloer) Run(ctx context.Context) {
	log.Info("restore controlloer start running !")

	opts := []func(context.Context) error{
		rc.restoreSchema,
		rc.recoverProgress,
		rc.restoreTables,
	}

	for _, process := range opts {
		err := process(ctx)
		if err == errCtxAborted {
			break
		}
		if err != nil {
			log.Errorf("")
		}
	}

	// show metrics
	log.Warnf("Timing statistic :\n%s", metrics.DumpTiming())

	return
}

func (rc *RestoreControlloer) recoverProgress(ctx context.Context) error {
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
		return nil
	}

	if len(tablesProgress) != tableCount {
		// TODO ..
		log.Warnf(" .... ")
	}

	// TODO ...

	return nil
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

func (rc *RestoreControlloer) restoreSchema(ctx context.Context) error {
	log.Info("Restore db/table scheam ~")
	// tidbMgr := rc.tidbMgr

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
		log.Errorf("restore schema failed : %v", err)
		return err
	}

	rc.dbInfo = tidbMgr.SyncSchema(database)

	return nil
}

func (rc *RestoreControlloer) restoreTables(ctx context.Context) error {
	var wg sync.WaitGroup
	for table, tableMeta := range rc.dbMeta.Tables {
		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

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

		tblExc, err := NewTableRestoreExecutor(
			ctx, tableMeta, rc.dbInfo, tableInfo, progress, rc.cfg)
		if err != nil {
			log.Errorf("Table (%s) executor init failed  : %s", table, err.Error())
			continue
		}

		wg.Add(1)
		go func(exc *TableRestoreExecutor) {
			defer wg.Done()
			err := exc.Run()
			if err != nil && err != errCtxAborted {
				log.Errorf("Table executor (%s) running causes error : %s",
					exc.tableMeta.Name, err.Error())
			}
		}(tblExc)
	}
	wg.Wait()

	return nil
}

// func (rc *RestoreControlloer) verify(ctx context.Context) error {
// 	log.Warnf("Please add tables' data verification !")
//
// 	return nil
// }

/*
	Handle 1 table restoring execution
*/
type TableRestoreExecutor struct {
	ctx context.Context
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
	ctx context.Context,
	tableMeta *MDTableMeta,
	dbInfo *TidbDBInfo,
	tableInfo *TidbTableInfo,
	progress *TableProgress,
	cfg *Config) (*TableRestoreExecutor, error) {

	uuid := adjustUUID(fmt.Sprintf("%s_%s", dbInfo.Name, tableInfo.Name), 16)
	kvDeliver, err := NewKVDeliver(ctx, uuid, cfg.KvDeliverAddr, cfg.PdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableSchema := tableMeta.GetSchema()
	kvEncoder := NewTableKVEncoder(
		dbInfo.Name, dbInfo.ID, tableInfo.Name, tableInfo.ID, tableSchema, 0)

	exc := &TableRestoreExecutor{
		ctx:       ctx,
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

func adjustUUID(uuid string, length int) string {
	size := len(uuid)
	if size > length {
		uuid = uuid[size-length:]
	} else if size < length {
		uuid = uuid + strings.Repeat("+", length-size)
	}
	return uuid
}

func (exc *TableRestoreExecutor) Close() {
	if err := exc.kvDeliver.Close(); err != nil {
		log.Errorf("kv deliver close error : %s", err.Error())
	}
}

func (exc *TableRestoreExecutor) Run() error {
	ctx := exc.ctx
	err := exc.restore(ctx)

	if err == nil {
		log.Infof("Start to verify table [%s]", exc.tableInfo.Name)

		s := time.Now()
		err = exc.verify(ctx)
		mark := fmt.Sprintf("[%s]_verify", exc.tableInfo.Name)
		metrics.TimeCostNS(mark, time.Since(s).Nanoseconds())
	}

	return err
}

func (exc *TableRestoreExecutor) restore(ctx context.Context) error {
	log.Infof("[%s] Start table resotre", exc.tableMeta.Name)
	table := exc.tableMeta.Name
	progress := exc.progress

	// ps : As we only ingest table data row by row ,
	//		so it's very important to keep source ql files in order  ~
	dataFiles := exc.tableMeta.DataFiles
	sort.Strings(dataFiles)

	// 1. restore row datas
	if err := exc.restoreDataFiles(ctx, dataFiles, progress); err != nil {
		return err
	}

	if !exc.checkAllLoaded(dataFiles, progress) {
		err := errors.Errorf("[%s] Table data loaded incompleted !", table)
		log.Errorf(err.Error())
		return err
	}

	// 2. restore meta data of table
	if err := exc.restoreMeta(); err != nil {
		log.Errorf("[%s] table meta restore failed !", table, err.Error())
		return err
	}

	// 3. flush table restoring
	if err := exc.flush(); err != nil {
		log.Errorf("[%s] table restore flush error = %s", table, err.Error())
		return err
	}

	progress.SetComplete()
	return nil
}

func (exc *TableRestoreExecutor) restoreDataFiles(
	ctx context.Context,
	dataFiles []string,
	tableProgress *TableProgress) error {

	kvEncoer := exc.kvEncoder
	kvDeliver := exc.kvDeliver

	// convert
	s := time.Now()
	for _, file := range dataFiles {
		select {
		case <-ctx.Done():
			// TODO ... record progress
			return errCtxAborted
		default:
		}

		fileName := filepath.Base(file)
		fileProgress, ok := tableProgress.FilesPrgoress[fileName]
		if !ok {
			// TODO ... return error ?
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
			if err != errCtxAborted {
				log.Errorf("[%s] file restore cause error : %s", file, err.Error())
			}
			return err
		}
	}
	mark := fmt.Sprintf("[%s]_restore_row_datas", exc.tableInfo.Name)
	metrics.TimeCostNS(mark, time.Since(s).Nanoseconds())

	return nil
}

func (exc *TableRestoreExecutor) restoreFile(
	ctx context.Context,
	file string,
	progress *TableFileProgress,
	kvEncoder *TableKVEncoder,
	kvDeliver *KVDeliver) error {

	// recover from progress
	offset, maxRowID := progress.Locate(StageLoaded)
	if offset > 0 {
		kvEncoder.RebaseRowID(maxRowID)
	}

	// 	Flows :
	//		1. read mydump file
	//		2. sql -> kvs
	//		3. load kvs data (into kv deliver server)
	//		4. flush kvs data (into tikv node)
	reader, _ := NewMDDataReader(file, offset)
	defer reader.Close()

	var (
		table       string = exc.tableInfo.Name
		markRead    string = fmt.Sprintf("[%s]_read_file", table)
		mark2KV     string = fmt.Sprintf("[%s]_sql_2_kv", table)
		markDeliver string = fmt.Sprintf("[%s]_kv_deliver", table)
		start       time.Time
	)

	for blocks := 0; ; blocks++ {
		select {
		case <-ctx.Done():
			return errCtxAborted
		default:
		}

		start = time.Now()
		sqls, err := reader.Read(defBlockSize)
		if err == io.EOF {
			log.Infof("file [%s] restore finish !", file)
			break
		}
		metrics.TimeCostNS(markRead, time.Since(start).Nanoseconds())

		for _, stmt := range sqls {
			// sql -> kv
			start = time.Now()
			kvs, _ := kvEncoder.Sql2KV(string(stmt))
			metrics.TimeCostNS(mark2KV, time.Since(start).Nanoseconds())

			// kv -> deliver ( -> tikv )
			start = time.Now()
			if err = kvDeliver.Put(kvs); err != nil {
				// TODO : retry ~
				log.Errorf("deliver kv failed = %s\n", err.Error())
			}
			metrics.TimeCostNS(markDeliver, time.Since(start).Nanoseconds())
		}

		if 0 == blocks%1000 {
			// PS : so far, choose not to update in real time but batch ~
			offset = reader.Tell()
			maxRowID = kvEncoder.NextRowID()
			progress.Update(StageLoaded, offset, maxRowID)
		}
	}

	offset = reader.Tell()
	maxRowID = kvEncoder.NextRowID()
	progress.Update(StageLoaded, offset, maxRowID)

	return nil
}

func (exc *TableRestoreExecutor) restoreMeta() error {
	// var rowID int64
	// for file, progress := range exc.progress.FilesPrgoress {
	// 	// TODO ...
	// }

	return nil
}

func (exc *TableRestoreExecutor) flush() error {
	table := exc.tableInfo.Name
	start := time.Now()

	log.Infof("[%s] table data restoring start to flush !", table)
	defer func() {
		log.Infof(" [%s] finished flushing table restoring data (cost = %.2f sec) !",
			table, time.Since(start).Seconds())
	}()

	s := time.Now()
	err := exc.kvDeliver.Flush()
	mark := fmt.Sprintf("[%s]_flush_all", exc.tableInfo.Name)
	metrics.TimeCostNS(mark, time.Since(s).Nanoseconds())
	return err
}

func (exc *TableRestoreExecutor) checkAllLoaded(dataFiles []string, progress *TableProgress) bool {
	finished := true
	for _, file := range dataFiles {
		fileName := filepath.Base(file)
		fileProgress, ok := progress.FilesPrgoress[fileName]
		if !ok {
			log.Warnf("[%s] miss file progress.", fileName)
			finished = false
			break
		}
		if !isFileAllLoaded(file, fileProgress) {
			log.Warnf("[%s] table file not finished.", fileName)
			finished = false
			break
		}
	}

	return finished
}

func isFileAllLoaded(file string, progress *TableFileProgress) bool {
	fileSize := GetFileSize(file)
	stage := progress.CheckStage(fileSize - 1)
	return stage == StageLoaded || stage == StageFlushed
}

func (exc *TableRestoreExecutor) verify(ctx context.Context) error {
	dsn := exc.cfg.TiDB
	tidb := ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	defer tidb.Close()

	// TODO : compare executed rows == count(*)
	tidb.Exec("USE " + exc.tableMeta.DB)
	_, err := tidb.Exec("ADMIN CHECK TABLE " + exc.tableMeta.Name)

	return err
}
