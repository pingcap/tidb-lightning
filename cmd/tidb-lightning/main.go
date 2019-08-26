// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Must(config.LoadGlobalConfig(os.Args[1:], nil))
	app := lightning.New(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		app.Stop()
	}()

	logger := log.L()

	err := app.GoServe()
	if err != nil {
		logger.Error("failed to start HTTP server", zap.Error(err))
		return
	}

	if cfg.App.ServerMode {
		err = app.RunServer()
	} else {
		err = app.RunOnce()
	}
	if err != nil {
		logger.Error("tidb lightning encountered error", zap.Error(err))
	} else {
		logger.Info("tidb lightning exit")
	}

	syncErr := logger.Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}

	if err != nil {
		os.Exit(1)
	}
}
