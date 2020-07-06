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
	"path/filepath"
	"runtime/debug"
	"syscall"

	"github.com/pingcap/errors"

	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Must(config.LoadGlobalConfig(os.Args[1:], nil))
	fmt.Fprintf(os.Stdout, "Verbose debug logs will be written to %s.\n\n", cfg.App.Config.File)
	err := checkSystemRequirement(cfg)
	if err != nil {
		log.L().Error("check system requirements failed", zap.Error(err))
		fmt.Fprintln(os.Stderr, "check system requirements failed:", err)
		return
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

	logger := log.L()

	// Lightning allocates too many transient objects and heap size is small,
	// so garbage collections happen too frequently and lots of time is spent in GC component.
	//
	// In a test of loading the table `order_line.csv` of 14k TPCC.
	// The time need of `encode kv data and write` step reduce from 52m4s to 37m30s when change
	// GOGC from 100 to 500, the total time needed reduce near 15m too.
	// The cost of this is the memory of lightnin at runtime grow from about 200M to 700M, but it's acceptable.
	// So we set the gc percentage as 500 default to reduce the GC frequency instead of 100.
	//
	// Local mode need much more memory than importer/tidb mode, if the gc percentage is too high,
	// lightning memory usage will also be high.
	if cfg.TikvImporter.Backend != config.BackendLocal {
		gogc := os.Getenv("GOGC")
		if gogc == "" {
			old := debug.SetGCPercent(500)
			log.L().Debug("set gc percentage", zap.Int("old", old), zap.Int("new", 500))
		}
	}

	err = app.GoServe()
	if err != nil {
		logger.Error("failed to start HTTP server", zap.Error(err))
		fmt.Fprintln(os.Stderr, "failed to start HTTP server:", err)
		return
	}

	if cfg.App.ServerMode {
		err = app.RunServer()
	} else {
		err = app.RunOnce()
	}
	if err != nil {
		logger.Error("tidb lightning encountered error stack info", zap.Error(err))
		logger.Error("tidb lightning encountered error", log.ShortError(err))
		fmt.Fprintln(os.Stderr, "tidb lightning encountered error: ", err)
	} else {
		logger.Info("tidb lightning exit")
		fmt.Fprintln(os.Stdout, "tidb lightning exit")
	}

	syncErr := logger.Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}

	if err != nil {
		os.Exit(1)
	}
}

func checkSystemRequirement(cfg *config.GlobalConfig) error {
	if cfg.TikvImporter.Backend == config.BackendLocal {
		// in local mode, we need to read&write a lot of L0 sst files, so we need to check
		// system max open files limit
		var totalSize uint64
		err := filepath.Walk(cfg.Mydumper.SourceDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && info.Size() > 0 {
				totalSize += uint64(info.Size())
			}
			return err
		})
		if err != nil {
			return errors.Trace(err)
		}

		// we estimate each sst as 50MB, this is a fairly coarse predict, the actual fd needed is
		// depend on chunk&import concurrency and each data&index key-value size
		estimateMaxFiles := totalSize / (50 * 1024 * 1024)
		var rLimit syscall.Rlimit
		err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			return errors.Trace(err)
		}
		if rLimit.Cur >= estimateMaxFiles {
			return nil
		}
		if rLimit.Max < estimateMaxFiles {
			// If the process is not started by privileged user, this will fail.
			rLimit.Max = estimateMaxFiles
		}
		prevLimit := rLimit.Cur
		rLimit.Cur = estimateMaxFiles
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("the maximum number of open file descriptors is too small, got %d, expect greater or equal to %d", prevLimit, estimateMaxFiles))
		}
	}

	return nil
}
