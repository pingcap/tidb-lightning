package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"

	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb/plan"
)

func setGlobalVars() {
	// hardcode it
	plan.PreparedPlanCacheEnabled = true
	plan.PreparedPlanCacheCapacity = 10
}

func main() {
	setGlobalVars()

	cfg, err := config.LoadConfig(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		common.AppLogger.Fatalf("parse cmd flags error: %s", err)
	}

	app := lightning.New(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		common.AppLogger.Infof("Got signal %v to exit.", sig)
		app.Close()
	}()

	if err := app.Run(); err != nil {
		common.AppLogger.Errorf("running err %v", errors.ErrorStack(err))
	}
	app.Close()
	common.AppLogger.Info("tidb lightning exit.")
	return
}
