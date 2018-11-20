package lightning

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

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

func (l *Lightning) Run() error {
	runtime.GOMAXPROCS(runtime.NumCPU())
	common.PrintInfo("lightning", func() {
		common.AppLogger.Infof("cfg %s", l.cfg)
	})

	if l.handleCommandFlagsAndExits() {
		return nil
	}
	if l.cfg.App.ProfileCPU {
		f, err := os.Create("cpuprofile")
		if err != nil {
			common.AppLogger.Fatalf("Cannot create CPU profile", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	l.wg.Add(1)
	var err error
	go func() {
		defer l.wg.Done()
		err = l.run()
	}()
	l.wg.Wait()
	return errors.Trace(err)
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

func (l *Lightning) run() error {
	mdl, err := mydump.NewMyDumpLoader(l.cfg)
	if err != nil {
		common.AppLogger.Errorf("failed to load mydumper source : %s", errors.ErrorStack(err))
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	procedure, err := restore.NewRestoreController(l.ctx, dbMetas, l.cfg)
	if err != nil {
		common.AppLogger.Errorf("failed to restore : %s", errors.ErrorStack(err))
		return errors.Trace(err)
	}
	defer procedure.Close()

	err = procedure.Run(l.ctx)
	procedure.Wait()
	return errors.Trace(err)
}

func (l *Lightning) doCompact() error {
	ctx := context.Background()

	importer, err := kv.NewImporter(ctx, l.cfg.TikvImporter.Addr, l.cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer importer.Close()

	if err := importer.Compact(ctx, restore.FullLevelCompact); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (l *Lightning) switchMode(mode sstpb.SwitchMode) error {
	ctx := context.Background()

	importer, err := kv.NewImporter(ctx, l.cfg.TikvImporter.Addr, l.cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer importer.Close()

	if err := importer.SwitchMode(ctx, mode); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (l *Lightning) Stop() {
	l.shutdown()
	l.wg.Wait()
}
