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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shurcooL/httpgzip"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/pingcap/tidb-lightning/lightning/web"
)

type Lightning struct {
	globalCfg *config.GlobalConfig
	globalTLS *common.TLS
	// taskCfgs is the list of task configurations enqueued in the server mode
	taskCfgs   *config.ConfigList
	ctx        context.Context
	shutdown   context.CancelFunc
	server     http.Server
	serverAddr net.Addr

	cancelLock sync.Mutex
	curTask    *config.Config
	cancel     context.CancelFunc
}

func initEnv(cfg *config.GlobalConfig) error {
	return log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel)
}

func New(globalCfg *config.GlobalConfig) *Lightning {
	if err := initEnv(globalCfg); err != nil {
		fmt.Println("Failed to initialize environment:", err)
		os.Exit(1)
	}

	tls, err := common.NewTLS(globalCfg.Security.CAPath, globalCfg.Security.CertPath, globalCfg.Security.KeyPath, globalCfg.App.StatusAddr)
	if err != nil {
		log.L().Fatal("failed to load TLS certificates", zap.Error(err))
	}

	ctx, shutdown := context.WithCancel(context.Background())
	return &Lightning{
		globalCfg: globalCfg,
		globalTLS: tls,
		ctx:       ctx,
		shutdown:  shutdown,
	}
}

func (l *Lightning) GoServe() error {
	if len(l.globalCfg.App.StatusAddr) == 0 {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.RedirectHandler("/web/", http.StatusFound))
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	handleTasks := http.StripPrefix("/tasks", http.HandlerFunc(l.handleTask))
	mux.Handle("/tasks", handleTasks)
	mux.Handle("/tasks/", handleTasks)
	mux.HandleFunc("/progress/task", handleProgressTask)
	mux.HandleFunc("/progress/table", handleProgressTable)
	mux.HandleFunc("/pause", handlePause)
	mux.HandleFunc("/resume", handleResume)

	mux.Handle("/web/", http.StripPrefix("/web", httpgzip.FileServer(web.Res, httpgzip.FileServerOptions{
		IndexHTML: true,
		ServeError: func(w http.ResponseWriter, req *http.Request, err error) {
			if os.IsNotExist(err) && !strings.Contains(req.URL.Path, ".") {
				http.Redirect(w, req, "/web/", http.StatusFound)
			} else {
				httpgzip.NonSpecific(w, req, err)
			}
		},
	})))

	listener, err := net.Listen("tcp", l.globalCfg.App.StatusAddr)
	if err != nil {
		return err
	}
	l.serverAddr = listener.Addr()
	l.server.Handler = mux
	listener = l.globalTLS.WrapListener(listener)

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
	cfg.TaskID = time.Now().UnixNano()
	failpoint.Inject("SetTaskID", func(val failpoint.Value) {
		cfg.TaskID = int64(val.(int))
	})
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
			restore.DeliverPauser.Pause() // force pause the progress on error
			log.L().Error("tidb lightning encountered error", zap.Error(err))
		}
	}
}

var taskCfgRecorderKey struct{}

func (l *Lightning) run(taskCfg *config.Config) (err error) {
	common.PrintInfo("lightning", func() {
		log.L().Info("cfg", zap.Stringer("cfg", taskCfg))
		fmt.Fprintln(os.Stdout, "cfg", zap.Stringer("cfg", taskCfg))
	})

	ctx, cancel := context.WithCancel(l.ctx)
	l.cancelLock.Lock()
	l.cancel = cancel
	l.curTask = taskCfg
	l.cancelLock.Unlock()
	web.BroadcastStartTask()

	defer func() {
		cancel()
		l.cancelLock.Lock()
		l.cancel = nil
		l.cancelLock.Unlock()
		web.BroadcastEndTask(err)
	}()

	failpoint.Inject("SkipRunTask", func() error {
		if recorder, ok := l.ctx.Value(&taskCfgRecorderKey).(chan *config.Config); ok {
			select {
			case recorder <- taskCfg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

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
	procedure, err = restore.NewRestoreController(ctx, dbMetas, taskCfg)
	if err != nil {
		log.L().Error("restore failed", log.ShortError(err))
		return errors.Trace(err)
	}
	defer procedure.Close()

	err = procedure.Run(ctx)
	procedure.Wait()
	return errors.Trace(err)
}

func (l *Lightning) Stop() {
	if err := l.server.Shutdown(l.ctx); err != nil {
		log.L().Warn("failed to shutdown HTTP server", log.ShortError(err))
	}
	l.shutdown()
}

func writeJSONError(w http.ResponseWriter, code int, prefix string, err error) {
	type errorResponse struct {
		Error string `json:"error"`
	}

	w.WriteHeader(code)

	if err != nil {
		prefix += ": " + err.Error()
	}
	json.NewEncoder(w).Encode(errorResponse{Error: prefix})
}

func parseTaskID(req *http.Request) (int64, string, error) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	taskIDString := path
	verb := ""
	if i := strings.IndexByte(path, '/'); i >= 0 {
		taskIDString = path[:i]
		verb = path[i+1:]
	}

	taskID, err := strconv.ParseInt(taskIDString, 10, 64)
	if err != nil {
		return 0, "", err
	}

	return taskID, verb, nil
}

func (l *Lightning) handleTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		taskID, _, err := parseTaskID(req)
		if e, ok := err.(*strconv.NumError); ok && e.Num == "" {
			l.handleGetTask(w)
		} else if err == nil {
			l.handleGetOneTask(w, req, taskID)
		} else {
			writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		}
	case http.MethodPost:
		l.handlePostTask(w, req)
	case http.MethodDelete:
		l.handleDeleteOneTask(w, req)
	case http.MethodPatch:
		l.handlePatchOneTask(w, req)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost+", "+http.MethodDelete+", "+http.MethodPatch)
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET, POST, DELETE and PATCH are allowed", nil)
	}
}

func (l *Lightning) handleGetTask(w http.ResponseWriter) {
	var response struct {
		Current   *int64  `json:"current"`
		QueuedIDs []int64 `json:"queue"`
	}

	if l.taskCfgs != nil {
		response.QueuedIDs = l.taskCfgs.AllIDs()
	}

	l.cancelLock.Lock()
	if l.cancel != nil && l.curTask != nil {
		response.Current = new(int64)
		*response.Current = l.curTask.TaskID
	}
	l.cancelLock.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (l *Lightning) handleGetOneTask(w http.ResponseWriter, req *http.Request, taskID int64) {
	var task *config.Config

	l.cancelLock.Lock()
	if l.curTask != nil && l.curTask.TaskID == taskID {
		task = l.curTask
	}
	l.cancelLock.Unlock()

	if task == nil && l.taskCfgs != nil {
		task, _ = l.taskCfgs.Get(taskID)
	}

	if task == nil {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
		return
	}

	json, err := json.Marshal(task)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to serialize task", err)
		return
	}

	writeBytesCompressed(w, req, json)
}

func (l *Lightning) handlePostTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	if l.taskCfgs == nil {
		// l.taskCfgs is non-nil only if Lightning is started with RunServer().
		// Without the server mode this pointer is default to be nil.
		writeJSONError(w, http.StatusNotImplemented, "server-mode not enabled", nil)
		return
	}

	type taskResponse struct {
		ID int64 `json:"id"`
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "cannot read request", err)
		return
	}
	log.L().Debug("received task config", zap.ByteString("content", data))

	cfg := config.NewConfig()
	if err = cfg.LoadFromGlobal(l.globalCfg); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "cannot restore from global config", err)
		return
	}
	if err = cfg.LoadFromTOML(data); err != nil {
		writeJSONError(w, http.StatusBadRequest, "cannot parse task (must be TOML)", err)
		return
	}
	if err = cfg.Adjust(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task configuration", err)
		return
	}

	l.taskCfgs.Push(cfg)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(taskResponse{ID: cfg.TaskID})
}

func (l *Lightning) handleDeleteOneTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	taskID, _, err := parseTaskID(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		return
	}

	var cancel context.CancelFunc
	cancelSuccess := false

	l.cancelLock.Lock()
	if l.cancel != nil && l.curTask != nil && l.curTask.TaskID == taskID {
		cancel = l.cancel
		l.cancel = nil
	}
	l.cancelLock.Unlock()

	if cancel != nil {
		cancel()
		cancelSuccess = true
	} else if l.taskCfgs != nil {
		cancelSuccess = l.taskCfgs.Remove(taskID)
	}

	log.L().Info("canceled task", zap.Int64("taskID", taskID), zap.Bool("success", cancelSuccess))

	if cancelSuccess {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	} else {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
	}
}

func (l *Lightning) handlePatchOneTask(w http.ResponseWriter, req *http.Request) {
	if l.taskCfgs == nil {
		writeJSONError(w, http.StatusNotImplemented, "server-mode not enabled", nil)
		return
	}

	taskID, verb, err := parseTaskID(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		return
	}

	moveSuccess := false
	switch verb {
	case "front":
		moveSuccess = l.taskCfgs.MoveToFront(taskID)
	case "back":
		moveSuccess = l.taskCfgs.MoveToBack(taskID)
	default:
		writeJSONError(w, http.StatusBadRequest, "unknown patch action", nil)
		return
	}

	if moveSuccess {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	} else {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
	}
}

func writeBytesCompressed(w http.ResponseWriter, req *http.Request, b []byte) {
	if !strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		w.Write(b)
		return
	}

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
		writeBytesCompressed(w, req, res)
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
		writeBytesCompressed(w, req, res)
	} else {
		if errors.IsNotFound(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(err.Error())
	}
}

func handlePause(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"paused":%v}`, restore.DeliverPauser.IsPaused())

	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		restore.DeliverPauser.Pause()
		log.L().Info("progress paused")
		w.Write([]byte("{}"))

	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut)
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET and PUT are allowed", nil)
	}
}

func handleResume(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		restore.DeliverPauser.Resume()
		log.L().Info("progress resumed")
		w.Write([]byte("{}"))

	default:
		w.Header().Set("Allow", http.MethodPut)
		writeJSONError(w, http.StatusMethodNotAllowed, "only PUT is allowed", nil)
	}
}
