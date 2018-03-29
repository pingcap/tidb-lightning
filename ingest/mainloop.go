package ingest

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"

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

	if m.cfg.DoCompact {
		m.doCompact()
		return
	}

	if m.cfg.DoChecksum != "" {
		tables := strings.Split(m.cfg.DoChecksum, ",")
		m.doChecksum(tables)
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

func (m *mainloop) doCompact() {
	cli, err := kv.NewKVDeliverClient(context.Background(), uuid.Nil, m.cfg.ImportServer.Addr, m.cfg.TiDB.PdAddr)
	if err != nil {
		log.Errorf(errors.ErrorStack(err))
		return
	}
	defer cli.Close()

	if err := cli.Compact(); err != nil {
		log.Errorf("compact error %s", errors.ErrorStack(err))
		return
	}
	log.Info("compact done")
	return
}

func (m *mainloop) doChecksum(tables []string) {
	results, err := restore.DoChecksum(m.cfg.TiDB, tables)
	if err != nil {
		log.Errorf("do checksum for tables %+v, error %s", tables, errors.ErrorStack(err))
		return
	}

	for _, result := range results {
		log.Infof("table %s.%s remote(from tidb) checksum %d,  total_kvs, total_bytes %d",
			result.Schema, result.Table, result.Checksum, result.TotalKVs, result.TotalBytes)
	}
}

func (m *mainloop) Stop() {
	m.shutdown()
	m.wg.Wait()
}
