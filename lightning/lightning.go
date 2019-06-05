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
	"io/ioutil"
	"net/http"
	"os"
	"runtime"

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
	globalCfg *config.GlobalConfig
	taskCfgs  chan *config.Config
	ctx       context.Context
	shutdown  context.CancelFunc
	server    http.Server
}

func initEnv(cfg *config.GlobalConfig) error {
	return log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel)
}

func New(globalCfg *config.GlobalConfig) *Lightning {
	if err := initEnv(globalCfg); err != nil {
		fmt.Println("Failed to initialize environment:", err)
		os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	ctx, shutdown := context.WithCancel(context.Background())
	return &Lightning{
		globalCfg: globalCfg,
		ctx:       ctx,
		shutdown:  shutdown,
	}
}

func (l *Lightning) Serve() {
	if len(l.globalCfg.App.StatusAddr) == 0 {
		return
	}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/tasks", func(w http.ResponseWriter, req *http.Request) {
		l.handleNewTask(w, req)
	})

	l.server.Addr = l.globalCfg.App.StatusAddr
	err := l.server.ListenAndServe()
	log.L().Info("stopped HTTP server", log.ShortError(err))
}

// Run Lightning using the global config as the same as the task config.
func (l *Lightning) RunOnce() error {
	cfg := config.NewConfig()
	if err := cfg.LoadFromGlobal(l.globalCfg); err != nil {
		return err
	}
	if err := cfg.Adjust(); err != nil {
		return err
	}
	return l.run(cfg)
}

func (l *Lightning) RunServer() error {
	var finalErr error
	l.taskCfgs = make(chan *config.Config, 64)

	log.L().Info("Lightning server is running, post to /tasks to start an import task")

	for {
		select {
		case <-l.ctx.Done():
			return l.ctx.Err()
		case task := <-l.taskCfgs:
			if task == nil {
				return finalErr
			}
			err := l.run(task)
			if err != nil {
				finalErr = err
			}
		}
	}
}

func (l *Lightning) run(taskCfg *config.Config) error {
	common.PrintInfo("lightning", func() {
		log.L().Info("cfg", zap.Stringer("cfg", taskCfg))
	})

	loadTask := log.L().Begin(zap.InfoLevel, "load data source")
	mdl, err := mydump.NewMyDumpLoader(taskCfg)
	loadTask.End(zap.ErrorLevel, err)
	if err != nil {
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	procedure, err := restore.NewRestoreController(l.ctx, dbMetas, taskCfg)
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
	if err := l.server.Shutdown(l.ctx); err != nil {
		log.L().Warn("failed to shutdown HTTP server", log.ShortError(err))
	}
	l.shutdown()
}

func (l *Lightning) handleNewTask(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	cfgs := l.taskCfgs
	if cfgs == nil {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "server-mode not enabled", http.StatusNotImplemented)
		return
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot read request: %v", err), http.StatusBadRequest)
		return
	}
	log.L().Debug("received task config", zap.ByteString("content", data))

	cfg := config.NewConfig()
	if err = cfg.LoadFromGlobal(l.globalCfg); err != nil {
		http.Error(w, fmt.Sprintf("cannot restore from global config: %v", err), http.StatusInternalServerError)
		return
	}
	if err = cfg.LoadFromTOML(data); err != nil {
		http.Error(w, fmt.Sprintf("cannot parse task (must be TOML): %v", err), http.StatusBadRequest)
		return
	}
	if err = cfg.Adjust(); err != nil {
		http.Error(w, fmt.Sprintf("invalid task configuration: %v", err), http.StatusBadRequest)
		return
	}

	select {
	case cfgs <- cfg:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("task queued"))
	default:
		http.Error(w, "task queue overflowed, please try again later", http.StatusServiceUnavailable)
	}
}
