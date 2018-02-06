package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"

	_ "github.com/juju/errors"
	"github.com/ngaut/log"

	"github.com/pingcap/tidb-lightning/ingest"
	"github.com/pingcap/tidb-lightning/ingest/common"
	"github.com/pingcap/tidb-lightning/ingest/config"
)

var (
	cfgFile = flag.String("c", "tidb-lighting.toml", "tidb-lighting configuration file")
)

func initEnv(cfg *config.Config) error {
	common.EnsureDir(cfg.Dir)
	// initLogger(cfg.Dir)

	if len(cfg.ProfilePort) > 0 {
		go func() { // TODO : config to enable it in debug mode
			log.Info(http.ListenAndServe(":"+cfg.ProfilePort, nil))
		}()
	}

	return nil
}

func initLogger(dir string) error {
	logDir := path.Join(dir, "log")
	logFile := path.Join(logDir, "ingest.log")
	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		return err
	}

	log.SetRotateByDay()
	log.SetHighlighting(false)
	log.SetLevel(log.LOG_LEVEL_WARN)
	if err := log.SetOutputByName(logFile); err != nil {
		return err
	}

	return nil
}

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
	flag.Parse()

	cfg, err := config.LoadConfig(*cfgFile)
	if err != nil {
		log.Errorf("load config failed (%s) : %s", *cfgFile, err.Error())
		return
	}

	initEnv(cfg)

	mainloop := ingest.Mainloop(cfg)
	mainloop.Run()

	// TODO : onExitSignal() --> mainloop.Stop()

	log.Info("tidb ingest exit.")
	return
}
