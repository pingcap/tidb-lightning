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

func onExitSignal() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	common.AppLogger.Infof("Got signal %d to exit.", sig)
}

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
	app.Run()

	// TODO : onExitSignal() --> mainloop.Stop()

	common.AppLogger.Info("tidb lightning exit.")
	return
}
