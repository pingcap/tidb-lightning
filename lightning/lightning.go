package lightning

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"

	"github.com/juju/errors"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/common"
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

	ctx, shutdown := context.WithCancel(context.Background())

	return &Lightning{
		cfg:      cfg,
		ctx:      ctx,
		shutdown: shutdown,
	}
}

func (l *Lightning) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	common.PrintInfo("lightning", func() {
		log.Infof("cfg %s", l.cfg)
	})

	if l.cfg.DoCompact {
		err := l.doCompact()
		if err != nil {
			log.Fatalf("compact error %s", errors.ErrorStack(err))
		}
		return
	}

	if mode := l.cfg.SwitchMode; mode != "" {
		var err error
		switch mode {
		case config.ImportMode:
			err = l.switchMode(sstpb.SwitchMode_Import)
		case config.NormalMode:
			err = l.switchMode(sstpb.SwitchMode_Normal)
		default:
			log.Fatalf("invalid switch mode %s, must use %s or %s", mode, config.ImportMode, config.NormalMode)
		}
		if err != nil {
			log.Fatalf("switch mode error %v", errors.ErrorStack(err))
		}
		log.Infof("switch mode to %s", mode)
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
	cli, err := kv.NewKVDeliverClient(context.Background(), uuid.Nil, l.cfg.TikvImporter.Addr, l.cfg.TiDB.PdAddr, "")
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	start := time.Now()
	if err := cli.Compact([]byte{}, []byte{}, -1); err != nil {
		return errors.Trace(err)
	}

	fmt.Println("compact takes", time.Since(start))
	return nil
}

func (l *Lightning) switchMode(mode sstpb.SwitchMode) error {
	cli, err := kv.NewKVDeliverClient(context.Background(), uuid.Nil, l.cfg.TikvImporter.Addr, l.cfg.TiDB.PdAddr, "")
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	return errors.Trace(cli.Switch(mode))
}

func (l *Lightning) Stop() {
	l.shutdown()
	l.wg.Wait()
}
