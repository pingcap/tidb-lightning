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
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	plan "github.com/pingcap/tidb/planner/core"
	"go.uber.org/zap"
)

func setGlobalVars() {
	// hardcode it
	plan.SetPreparedPlanCache(true)
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
		fmt.Println("Failed to parse command flags: ", err)
		os.Exit(1)
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
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		app.Stop()
	}()

	err = app.Run()
	if err != nil {
		log.L().Error("tidb lightning encountered error", zap.Error(err))
		os.Exit(1)
	}

	log.L().Info("tidb lightning exit")
}
