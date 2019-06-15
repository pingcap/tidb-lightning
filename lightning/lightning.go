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
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/pingcap/tidb-lightning/lightning/web"
)

type Lightning struct {
	globalCfg  *config.GlobalConfig
	taskCfgs   *config.ConfigList
	ctx        context.Context
	shutdown   context.CancelFunc
	server     http.Server
	serverAddr net.Addr
}

func initEnv(cfg *config.GlobalConfig) error {
	return log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel)
}

func New(globalCfg *config.GlobalConfig) *Lightning {
	if err := initEnv(globalCfg); err != nil {
		fmt.Println("Failed to initialize environment:", err)
		os.Exit(1)
	}

	ctx, shutdown := context.WithCancel(context.Background())
	return &Lightning{
		globalCfg: globalCfg,
		ctx:       ctx,
		shutdown:  shutdown,
	}
}

func (l *Lightning) GoServe() error {
	if len(l.globalCfg.App.StatusAddr) == 0 {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/progress/task", handleProgressTask)
	mux.HandleFunc("/progress/table", handleProgressTable)

	listener, err := net.Listen("tcp", l.globalCfg.App.StatusAddr)
	if err != nil {
		return err
	}
	l.serverAddr = listener.Addr()
	l.server.Handler = mux

	go func() {
		err := l.server.Serve(listener)
		log.L().Info("stopped HTTP server", log.ShortError(err))
	}()
	return nil
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
	l.taskCfgs = config.NewConfigList()
	log.L().Info(
		"Lightning server is running, post to /tasks to start an import task",
		zap.Stringer("address", l.serverAddr),
	)

	for {
		task, err := l.taskCfgs.Pop(l.ctx)
		if err != nil {
			return err
		}
		err = l.run(task)
		if err != nil {
			log.L().Error("tidb lightning encountered error", zap.Error(err))
		}
	}
}

var taskCfgRecorderKey struct{}

func (l *Lightning) run(taskCfg *config.Config) (err error) {
	failpoint.Inject("SkipRunTask", func() error {
		if recorder, ok := l.ctx.Value(&taskCfgRecorderKey).(chan *config.Config); ok {
			recorder <- taskCfg
		}
		return nil
	})

	common.PrintInfo("lightning", func() {
		log.L().Info("cfg", zap.Stringer("cfg", taskCfg))
	})

	web.BroadcastStartTask()
	defer func() {
		web.BroadcastEndTask(err)
	}()

	loadTask := log.L().Begin(zap.InfoLevel, "load data source")
	var mdl *mydump.MDLoader
	mdl, err = mydump.NewMyDumpLoader(taskCfg)
	loadTask.End(zap.ErrorLevel, err)
	if err != nil {
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	web.BroadcastInitProgress(dbMetas)

	var procedure *restore.RestoreController
	procedure, err = restore.NewRestoreController(l.ctx, dbMetas, taskCfg)
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

func (l *Lightning) handleTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		l.handleGetTask(w)
	case http.MethodPost:
		l.handlePostTask(w, req)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"only GET and POST are allowed"}`))
	}
}

func (l *Lightning) handleGetTask(w http.ResponseWriter) {
	var response struct {
		Enabled   bool    `json:"enabled"`
		QueuedIDs []int64 `json:"queue"`
	}

	response.Enabled = l.taskCfgs != nil
	if response.Enabled {
		response.QueuedIDs = l.taskCfgs.AllIDs()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (l *Lightning) handlePostTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	type errorResponse struct {
		Error string `json:"error"`
	}
	type taskResponse struct {
		ID int64 `json:"id"`
	}

	if l.taskCfgs == nil {
		w.WriteHeader(http.StatusNotImplemented)
		json.NewEncoder(w).Encode(errorResponse{Error: "server-mode not enabled"})
		return
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("cannot read request: %v", err)})
		return
	}
	log.L().Debug("received task config", zap.ByteString("content", data))

	cfg := config.NewConfig()
	if err = cfg.LoadFromGlobal(l.globalCfg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("cannot restore from global config: %v", err)})
		return
	}
	if err = cfg.LoadFromTOML(data); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("cannot parse task (must be TOML): %v", err)})
		return
	}
	if err = cfg.Adjust(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("invalid task configuration: %v", err)})
		return
	}

	l.taskCfgs.Push(cfg)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(taskResponse{ID: cfg.TaskID})
}

func writeBytesCompressed(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(http.StatusOK)
	gw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	gw.Write(b)
	gw.Close()
}

func handleProgressTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	res, err := web.MarshalTaskProgress()
	if err == nil {
		writeBytesCompressed(w, res)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
	}
}

func handleProgressTable(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tableName := req.URL.Query().Get("t")
	res, err := web.MarshalTableCheckpoints(tableName)
	if err == nil {
		writeBytesCompressed(w, res)
	} else {
		if errors.IsNotFound(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(err.Error())
	}
}
