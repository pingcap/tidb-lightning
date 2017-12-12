package main

import (
	"flag"
	"os"
	"os/signal"
	"path"
	"syscall"

	_ "github.com/juju/errors"
	"github.com/ngaut/log"

	"github.com/pingcap/tidb-lightning/ingest"
)

var (
	cfgFile = flag.String("c", "tidb-lighting.toml", "tidb-lighting configuration file")
)

func initEnv(cfg *ingest.Config) error {
	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return err
	}

	/*
		if err := initLogger(cfg.Dir); err != nil {
			return errors.Trace(err)
		}
	*/

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

	cfg, err := ingest.LoadConfig(*cfgFile)
	if err != nil {
		log.Errorf("load config failed (%s) : %s", *cfgFile, err.Error())
		return
	}

	initEnv(cfg)

	mainloop := ingest.Mainloop(cfg)
	mainloop.Run()

	// onExitSignal()
	// mainloop.Stop()

	log.Info("tidb ingest exit.")
	return
}
