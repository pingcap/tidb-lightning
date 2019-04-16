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

package lightning

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
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
	if err := log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel); err != nil {
		return errors.Trace(err)
	}

	if cfg.App.ProfilePort > 0 {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.App.ProfilePort), nil)
			log.L().Info("stopped HTTP server", log.ShortError(err))
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
		log.L().Info("cfg", zap.Stringer("cfg", l.cfg))
	})

	l.wg.Add(1)
	var err error
	go func() {
		defer l.wg.Done()
		err = l.run()
	}()
	l.wg.Wait()
	return errors.Trace(err)
}

func (l *Lightning) run() error {
	loadTask := log.L().Begin(zap.InfoLevel, "load data source")
	mdl, err := mydump.NewMyDumpLoader(l.cfg)
	loadTask.End(zap.ErrorLevel, err)
	if err != nil {
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	procedure, err := restore.NewRestoreController(l.ctx, dbMetas, l.cfg)
	if err != nil {
		log.L().Error("restore failed", log.ShortError(err))
		return errors.Trace(err)
	}
	defer procedure.Close()

	err = procedure.Run(l.ctx)
	procedure.Wait()
	return errors.Trace(err)
}

func (l *Lightning) Stop() {
	l.shutdown()
	l.wg.Wait()
}
