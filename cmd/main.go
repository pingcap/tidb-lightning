package main

import (
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/pingcap/tidb-lightning/ingest"
	"github.com/pingcap/tidb-lightning/ingest/config"
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

func main() {
	cfg, err := config.LoadConfig(os.Args[1:])
	if err != nil {
		log.Errorf("load config failed: %s", errors.ErrorStack(err))
		return
	}

	mainloop := ingest.NewMainLoop(cfg)
	mainloop.Run()

	// TODO : onExitSignal() --> mainloop.Stop()

	log.Info("tidb ingest exit.")
	return
}
