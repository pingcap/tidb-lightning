package ingest

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/pingcap/tidb/tablecodec"

	"github.com/juju/errors"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/ingest/config"
	"github.com/pingcap/tidb-lightning/ingest/kv"
	applog "github.com/pingcap/tidb-lightning/ingest/log"
	"github.com/pingcap/tidb-lightning/ingest/mydump"
	"github.com/pingcap/tidb-lightning/ingest/restore"
)

type mainloop struct {
	cfg      *config.Config
	ctx      context.Context
	shutdown context.CancelFunc

	wg sync.WaitGroup
}

func initEnv(cfg *config.Config) error {
	if err := applog.InitLogger(&cfg.App.LogConfig); err != nil {
		return errors.Trace(err)
	}

	kv.ConfigDeliverTxnBatchSize(cfg.ImportServer.BatchSize)

	if cfg.App.ProfilePort > 0 {
		go func() { // TODO : config to enable it in debug mode
			log.Info(http.ListenAndServe(fmt.Sprintf(":%d", cfg.App.ProfilePort), nil))
		}()
	}

	return nil
}

func NewMainLoop(cfg *config.Config) *mainloop {
	initEnv(cfg)
	log.Infof("cfg %+v", cfg)

	ctx, shutdown := context.WithCancel(context.Background())

	return &mainloop{
		cfg:      cfg,
		ctx:      ctx,
		shutdown: shutdown,
	}
}

func (m *mainloop) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	if m.cfg.DoCompact != "" {
		tables := strings.Split(m.cfg.DoCompact, ",")
		err := m.doCompact(tables)
		if err != nil {
			log.Errorf("compact error %s", errors.ErrorStack(err))
		}
		return
	}

	if m.cfg.DoChecksum != "" {
		tables := strings.Split(m.cfg.DoChecksum, ",")
		err := m.doChecksum(tables)
		if err != nil {
			log.Errorf("checksum error %s", errors.ErrorStack(err))
		}
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
	}()
	m.wg.Wait()
}

func (m *mainloop) run() {
	mdl, err := mydump.NewMyDumpLoader(m.cfg)
	if err != nil {
		log.Errorf("failed to load mydumper source : %s", err.Error())
		return
	}

	dbMeta := mdl.GetDatabase()
	procedure := restore.NewRestoreControlloer(dbMeta, m.cfg)
	defer procedure.Close()

	procedure.Run(m.ctx)
	return
}

func (m *mainloop) doCompact(tables []string) error {
	cli, err := kv.NewKVDeliverClient(context.Background(), uuid.Nil, m.cfg.ImportServer.Addr, m.cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	tidbMgr, err := restore.NewTiDBManager(m.cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer tidbMgr.Close()

	for _, table := range tables {
		log.Infof("begin compaction for table %s", table)

		// table must contains only one dot or we don't know how to split it.
		if strings.Count(table, ".") != 1 {
			log.Warnf("tables %s contains not dot or more than one dot which is not allowed", table)
			continue
		}

		split := strings.Split(table, ".")
		tableInfo, err := tidbMgr.GetTableByName(split[0], split[1])
		if err != nil {
			return errors.Trace(err)
		}

		start := tablecodec.GenTablePrefix(tableInfo.ID)
		end := tablecodec.GenTablePrefix(tableInfo.ID + 1)
		if err := cli.Compact(start, end); err != nil {
			return errors.Trace(err)
		}
		log.Infof("finished compaction for table %s", table)
	}

	log.Info("compact done")
	return nil
}

func (m *mainloop) doChecksum(tables []string) error {
	results, err := restore.DoChecksum(m.cfg.TiDB, tables)
	if err != nil {
		return errors.Trace(err)
	}

	for _, result := range results {
		log.Infof("table %s.%s remote(from tidb) checksum %d,  total_kvs, total_bytes %d",
			result.Schema, result.Table, result.Checksum, result.TotalKVs, result.TotalBytes)
	}
	return nil
}

func (m *mainloop) Stop() {
	m.shutdown()
	m.wg.Wait()
}
