package lightning

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/juju/errors"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	applog "github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
)

type Lightning struct {
	cfg      *config.Config
	ctx      context.Context
	shutdown context.CancelFunc

	wg sync.WaitGroup
}

func initEnv(cfg *config.Config) error {
	if err := applog.InitLogger(&cfg.App.LogConfig, cfg.TiDB.LogLevel); err != nil {
		return errors.Trace(err)
	}

	kv.ConfigDeliverTxnBatchSize(cfg.TikvImporter.BatchSize)

	if cfg.App.ProfilePort > 0 {
		go func() { // TODO : config to enable it in debug mode
			log.Info(http.ListenAndServe(fmt.Sprintf(":%d", cfg.App.ProfilePort), nil))
		}()
	}

	return nil
}

func New(cfg *config.Config) *Lightning {
	initEnv(cfg)
	log.Infof("cfg %+v", cfg)

	ctx, shutdown := context.WithCancel(context.Background())

	return &Lightning{
		cfg:      cfg,
		ctx:      ctx,
		shutdown: shutdown,
	}
}

func (l *Lightning) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	if l.cfg.DoCompact {
		err := l.doCompact()
		if err != nil {
			log.Errorf("compact error %s", errors.ErrorStack(err))
		}
		return
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.run()
	}()
	l.wg.Wait()
}

func (l *Lightning) run() {
	mdl, err := mydump.NewMyDumpLoader(l.cfg)
	if err != nil {
		log.Errorf("failed to load mydumper source : %s", errors.ErrorStack(err))
		return
	}

	dbMeta := mdl.GetDatabase()
	procedure := restore.NewRestoreControlloer(dbMeta, l.cfg)
	defer procedure.Close()

	procedure.Run(l.ctx)
	return
}

func (l *Lightning) doCompact() error {
	cli, err := kv.NewKVDeliverClient(context.Background(), uuid.Nil, l.cfg.TikvImporter.Addr, l.cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	start := time.Now()
	if err := cli.Compact([]byte{}, []byte{}); err != nil {
		return errors.Trace(err)
	}

	fmt.Println("compact takes", time.Since(start))
	return nil
}

func (l *Lightning) Stop() {
	l.shutdown()
	l.wg.Wait()
}
