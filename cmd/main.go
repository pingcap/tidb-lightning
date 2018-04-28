package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/config"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	log "github.com/sirupsen/logrus"
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
	log.Infof("Got signal %d to exit.", sig)
}

func setGlobalVars(localCfg *config.Config) {
	// hardcode it
	plan.PreparedPlanCacheEnabled = true
	plan.PreparedPlanCacheCapacity = 10

	cfg := tidbcfg.GetGlobalConfig()
	cfg.Log.SlowThreshold = 3000

	kv.ImportingTxnMembufCap = int(localCfg.DataSource.ReadBlockSize * 4)
	// TODO : calculate predicted ratio, bwtween sql and kvs' size, base on specified DDL
}

func main() {
	cfg, err := config.LoadConfig(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags error: %s", err)
	}

	setGlobalVars(cfg)

	app := lightning.New(cfg)
	app.Run()

	// TODO : onExitSignal() --> mainloop.Stop()

	log.Info("tidb lightning exit.")
	return
}
