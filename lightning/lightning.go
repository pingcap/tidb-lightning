package lightning

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"

	"github.com/juju/errors"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
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
	if err := common.InitLogger(&cfg.App.LogConfig, cfg.TiDB.LogLevel); err != nil {
		return errors.Trace(err)
	}

	kv.ConfigDeliverTxnBatchSize(cfg.TikvImporter.BatchSize)

	if cfg.App.ProfilePort > 0 {
		go func() {
			http.Handle("/metrics", prometheus.Handler())
			common.AppLogger.Info(http.ListenAndServe(fmt.Sprintf(":%d", cfg.App.ProfilePort), nil))
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
		common.AppLogger.Infof("cfg %s", l.cfg)
	})

	if l.handleCommandFlagsAndExits() {
		return
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.run()
	}()
	l.wg.Wait()
}

func (l *Lightning) handleCommandFlagsAndExits() (exits bool) {
	if l.cfg.DoCompact {
		err := l.doCompact()
		if err != nil {
			common.AppLogger.Fatalf("compact error %s", errors.ErrorStack(err))
		}
		return true
	}

	if mode := l.cfg.SwitchMode; mode != "" {
		var err error
		switch mode {
		case config.ImportMode:
			err = l.switchMode(sstpb.SwitchMode_Import)
		case config.NormalMode:
			err = l.switchMode(sstpb.SwitchMode_Normal)
		default:
			common.AppLogger.Fatalf("invalid mode %s, must use %s or %s", mode, config.ImportMode, config.NormalMode)
		}
		if err != nil {
			common.AppLogger.Fatalf("switch mode error %v", errors.ErrorStack(err))
		}
		common.AppLogger.Infof("switch mode to %s", mode)
		return true
	}
	return false
}

func (l *Lightning) run() {
	mdl, err := mydump.NewMyDumpLoader(l.cfg)
	if err != nil {
		common.AppLogger.Errorf("failed to load mydumper source : %s", errors.ErrorStack(err))
		return
	}

	dbMetas := mdl.GetDatabases()
	procedure := restore.NewRestoreControlloer(l.ctx, dbMetas, l.cfg)
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

	if err := cli.Compact(restore.FullLevelCompact); err != nil {
		return errors.Trace(err)
	}

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
