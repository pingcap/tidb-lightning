package restore

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/pingcap/br/pkg/checksum"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"

	. "github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
)

const (
	preUpdateServiceSafePointFactor = 3
)

var (
	serviceSafePointTTL int64 = 10 * 60 // 10 min in seconds
)

// RemoteChecksum represents a checksum result got from tidb.
type RemoteChecksum struct {
	Schema     string
	Table      string
	Checksum   uint64
	TotalKVs   uint64
	TotalBytes uint64
}

type ChecksumManager interface {
	Checksum(ctx context.Context, tableInfo *TidbTableInfo) (*RemoteChecksum, error)
}

func newChecksumManager(rc *RestoreController) (ChecksumManager, error) {
	// if we don't need checksum, just return nil
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB || rc.cfg.PostRestore.Checksum == config.OpLevelOff {
		return nil, nil
	}

	pdAddr := rc.cfg.TiDB.PdAddr
	pdVersion, err := common.FetchPDVersion(rc.tls, pdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for v4.0.0 or upper, we can use the gc ttl api
	var manager ChecksumManager
	if pdVersion.Major >= 4 {
		tlsOpt := rc.tls.ToPDSecurityOption()
		pdCli, err := pd.NewClient([]string{pdAddr}, tlsOpt)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if tlsOpt.CAPath != "" {
			conf := tidbcfg.GetGlobalConfig()
			conf.Security.ClusterSSLCA = tlsOpt.CAPath
			conf.Security.ClusterSSLCert = tlsOpt.CertPath
			conf.Security.ClusterSSLKey = tlsOpt.KeyPath
			tidbcfg.StoreGlobalConfig(conf)
		}
		store, err := tikv.Driver{}.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
		if err != nil {
			return nil, errors.Trace(err)
		}

		manager = newTiKVChecksumManager(store.(tikv.Storage).GetClient(), pdCli)
	} else {
		manager = newTiDBChecksumExecutor(rc.tidbMgr.db)
	}

	return manager, nil
}

// fetch checksum for tidb sql client
type tidbChecksumExecutor struct {
	db      *sql.DB
	manager *gcLifeTimeManager
}

func newTiDBChecksumExecutor(db *sql.DB) *tidbChecksumExecutor {
	return &tidbChecksumExecutor{
		db:      db,
		manager: newGCLifeTimeManager(),
	}
}

func (e *tidbChecksumExecutor) Checksum(ctx context.Context, tableInfo *TidbTableInfo) (*RemoteChecksum, error) {
	var err error
	if err = e.manager.addOneJob(ctx, e.db); err != nil {
		return nil, err
	}

	// set it back finally
	defer e.manager.removeOneJob(ctx, e.db)

	tableName := common.UniqueTable(tableInfo.DB, tableInfo.Name)

	task := log.With(zap.String("table", tableName)).Begin(zap.InfoLevel, "remote checksum")

	// ADMIN CHECKSUM TABLE <table>,<table>  example.
	// 	mysql> admin checksum table test.t;
	// +---------+------------+---------------------+-----------+-------------+
	// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
	// +---------+------------+---------------------+-----------+-------------+
	// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
	// +---------+------------+---------------------+-----------+-------------+

	cs := RemoteChecksum{}
	err = common.SQLWithRetry{DB: e.db, Logger: task.Logger}.QueryRow(ctx, "compute remote checksum",
		"ADMIN CHECKSUM TABLE "+tableName, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes,
	)
	dur := task.End(zap.ErrorLevel, err)
	metric.ChecksumSecondsHistogram.Observe(dur.Seconds())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &cs, nil
}

// DoChecksum do checksum for tables.
// table should be in <db>.<table>, format.  e.g. foo.bar
func DoChecksum(ctx context.Context, db *sql.DB, table *TidbTableInfo) (*RemoteChecksum, error) {
	var err error
	manager, ok := ctx.Value(&checksumManagerKey).(ChecksumManager)
	if !ok {
		return nil, errors.New("No gcLifeTimeManager found in context, check context initialization")
	}

	task := log.With(zap.String("table", table.Name)).Begin(zap.InfoLevel, "remote checksum")

	cs, err := manager.Checksum(ctx, table)
	dur := task.End(zap.ErrorLevel, err)
	metric.ChecksumSecondsHistogram.Observe(dur.Seconds())

	return cs, err
}

type gcLifeTimeManager struct {
	runningJobsLock sync.Mutex
	runningJobs     int
	oriGCLifeTime   string
}

func newGCLifeTimeManager() *gcLifeTimeManager {
	// Default values of three member are enough to initialize this struct
	return &gcLifeTimeManager{}
}

// Pre- and post-condition:
// if m.runningJobs == 0, GC life time has not been increased.
// if m.runningJobs > 0, GC life time has been increased.
// m.runningJobs won't be negative(overflow) since index concurrency is relatively small
func (m *gcLifeTimeManager) addOneJob(ctx context.Context, db *sql.DB) error {
	m.runningJobsLock.Lock()
	defer m.runningJobsLock.Unlock()

	if m.runningJobs == 0 {
		oriGCLifeTime, err := ObtainGCLifeTime(ctx, db)
		if err != nil {
			return err
		}
		m.oriGCLifeTime = oriGCLifeTime
		err = increaseGCLifeTime(ctx, m, db)
		if err != nil {
			return err
		}
	}
	m.runningJobs += 1
	return nil
}

// Pre- and post-condition:
// if m.runningJobs == 0, GC life time has been tried to recovered. If this try fails, a warning will be printed.
// if m.runningJobs > 0, GC life time has not been recovered.
// m.runningJobs won't minus to negative since removeOneJob follows a successful addOneJob.
func (m *gcLifeTimeManager) removeOneJob(ctx context.Context, db *sql.DB) {
	m.runningJobsLock.Lock()
	defer m.runningJobsLock.Unlock()

	m.runningJobs -= 1
	if m.runningJobs == 0 {
		err := UpdateGCLifeTime(ctx, db, m.oriGCLifeTime)
		if err != nil {
			query := fmt.Sprintf(
				"UPDATE mysql.tidb SET VARIABLE_VALUE = '%s' WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
				m.oriGCLifeTime,
			)
			log.L().Warn("revert GC lifetime failed, please reset the GC lifetime manually after Lightning completed",
				zap.String("query", query),
				log.ShortError(err),
			)
		}
	}
}

func increaseGCLifeTime(ctx context.Context, manager *gcLifeTimeManager, db *sql.DB) (err error) {
	// checksum command usually takes a long time to execute,
	// so here need to increase the gcLifeTime for single transaction.
	var increaseGCLifeTime bool
	if manager.oriGCLifeTime != "" {
		ori, err := time.ParseDuration(manager.oriGCLifeTime)
		if err != nil {
			return errors.Trace(err)
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
			return err
		}
	}

	failpoint.Inject("IncreaseGCUpdateDuration", nil)

	return nil
}

type tikvChecksumManager struct {
	client  kv.Client
	manager gcTTLManager
}

// newTiKVChecksumManager return a new tikv checksum manager
func newTiKVChecksumManager(client kv.Client, pdClient pd.Client) *tikvChecksumManager {
	return &tikvChecksumManager{
		client: client,
		manager: gcTTLManager{
			pdClient: pdClient,
		},
	}
}

func (e *tikvChecksumManager) checksumDB(ctx context.Context, tableInfo *TidbTableInfo) (*RemoteChecksum, error) {
	builder := checksum.NewExecutorBuilder(tableInfo.Core, oracle.ComposeTS(time.Now().Unix()*1000, 0))
	executor, err := builder.Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	res, err := executor.Execute(ctx, e.client, func() {})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &RemoteChecksum{
		Schema:     tableInfo.DB,
		Table:      tableInfo.Name,
		Checksum:   res.Checksum,
		TotalBytes: res.TotalBytes,
		TotalKVs:   res.TotalKvs,
	}, nil
}

func (e *tikvChecksumManager) Checksum(ctx context.Context, tableInfo *TidbTableInfo) (*RemoteChecksum, error) {
	tbl := common.UniqueTable(tableInfo.DB, tableInfo.Name)
	err := e.manager.addOneJob(ctx, tbl, oracle.ComposeTS(time.Now().Unix()*1000, 0))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return e.checksumDB(ctx, tableInfo)
}

type tableChecksumTS struct {
	table    string
	gcSafeTS uint64
}

// following function are for implement `heap.Interface`

func (m *gcTTLManager) Len() int {
	return len(m.tableGCSafeTS)
}

func (m *gcTTLManager) Less(i, j int) bool {
	return m.tableGCSafeTS[i].gcSafeTS < m.tableGCSafeTS[j].gcSafeTS
}

func (m *gcTTLManager) Swap(i, j int) {
	m.tableGCSafeTS[i], m.tableGCSafeTS[j] = m.tableGCSafeTS[j], m.tableGCSafeTS[i]
}

func (m *gcTTLManager) Push(x interface{}) {
	m.tableGCSafeTS = append(m.tableGCSafeTS, x.(*tableChecksumTS))
}

func (m *gcTTLManager) Pop() interface{} {
	i := m.tableGCSafeTS[len(m.tableGCSafeTS)-1]
	m.tableGCSafeTS = m.tableGCSafeTS[:len(m.tableGCSafeTS)-1]
	return i
}

type gcTTLManager struct {
	lock     sync.Mutex
	pdClient pd.Client
	// tableGCSafeTS is a binary heap that stored active checksum jobs GC safe point ts
	tableGCSafeTS []*tableChecksumTS
	currentTs     uint64
	serviceID     string
	// 0 for not start, otherwise started
	started uint32
}

func newGCTTLManager(pdClient pd.Client) gcTTLManager {
	return gcTTLManager{
		pdClient:  pdClient,
		serviceID: fmt.Sprintf("lightning-%s", uuid.New()),
	}
}

func (m *gcTTLManager) addOneJob(ctx context.Context, table string, ts uint64) error {
	// start gc ttl loop if not started yet.
	if atomic.CompareAndSwapUint32(&m.started, 0, 1) {
		m.start(ctx)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	var curTs uint64
	if len(m.tableGCSafeTS) > 0 {
		curTs = m.tableGCSafeTS[0].gcSafeTS
	}
	m.Push(&tableChecksumTS{table: table, gcSafeTS: ts})
	heap.Fix(m, len(m.tableGCSafeTS)-1)
	m.currentTs = m.tableGCSafeTS[0].gcSafeTS
	if curTs == 0 || m.currentTs < curTs {
		return m.doUpdateGCTTL(ctx, m.currentTs)
	}
	return nil
}

func (m *gcTTLManager) removeOneJob(table string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	idx := -1
	for i := 0; i < len(m.tableGCSafeTS); i++ {
		if m.tableGCSafeTS[i].table == table {
			idx = i
			break
		}
	}

	if idx >= 0 {
		l := len(m.tableGCSafeTS)
		m.tableGCSafeTS[idx] = m.tableGCSafeTS[l-1]
		m.tableGCSafeTS = m.tableGCSafeTS[:l-1]
		if l > 1 && idx < l-1 {
			heap.Fix(m, idx)
		}
	}

	var newTs uint64
	if len(m.tableGCSafeTS) > 0 {
		newTs = m.tableGCSafeTS[0].gcSafeTS
	}
	m.currentTs = newTs
}

func (m *gcTTLManager) updateGCTTL(ctx context.Context) error {
	m.lock.Lock()
	currentTs := m.currentTs
	m.lock.Unlock()
	return m.doUpdateGCTTL(ctx, currentTs)
}

func (m *gcTTLManager) doUpdateGCTTL(ctx context.Context, ts uint64) error {
	log.L().Debug("update PD safePoint limit with TTL",
		zap.Uint64("currnet_ts", ts))
	var err error
	if ts > 0 {
		_, err = m.pdClient.UpdateServiceGCSafePoint(ctx,
			m.serviceID, serviceSafePointTTL, ts)
	}
	return err
}

func (m *gcTTLManager) start(ctx context.Context) {
	// It would be OK since TTL won't be zero, so gapTime should > `0.
	updateGapTime := time.Duration(serviceSafePointTTL) * time.Second / preUpdateServiceSafePointFactor

	updateTick := time.NewTicker(updateGapTime)

	updateGCTTL := func() {
		if err := m.updateGCTTL(ctx); err != nil {
			log.L().Warn("failed to update service safe point, checksum may fail if gc triggered", zap.Error(err))
		}
	}

	// trigger a service gc ttl at start
	updateGCTTL()
	go func() {
		defer updateTick.Stop()
		for {
			select {
			case <-ctx.Done():
				log.L().Info("service safe point keeper exited")
				return
			case <-updateTick.C:
				updateGCTTL()
			}
		}
	}()
}
