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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

type lightningSuite struct{}

var _ = Suite(&lightningSuite{})

func TestLightning(t *testing.T) {
	TestingT(t)
}

func (s *lightningSuite) TestInitEnv(c *C) {
	cfg := &config.GlobalConfig{
		App: config.GlobalLightning{StatusAddr: ":45678"},
	}
	err := initEnv(cfg)
	c.Assert(err, IsNil)
	cfg.App.StatusAddr = ""
	cfg.App.Config.File = "."
	err = initEnv(cfg)
	c.Assert(err, ErrorMatches, "can't use directory as log file name")
}

func (s *lightningSuite) TestRun(c *C) {
	cfg := config.NewGlobalConfig()
	cfg.TiDB.Host = "test.invalid"
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "test.invalid:2379"
	cfg.Mydumper.SourceDir = "not-exists"
	lightning := New(cfg)
	err := lightning.RunOnce()
	c.Assert(err, ErrorMatches, ".*mydumper dir does not exist")

	err = lightning.run(&config.Config{
		Mydumper: config.MydumperRuntime{SourceDir: "."},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "invalid",
		},
	})
	c.Assert(err, ErrorMatches, "Unknown checkpoint driver invalid")

	err = lightning.run(&config.Config{
		Mydumper: config.MydumperRuntime{SourceDir: "."},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "file",
			DSN:    "any-file",
		},
	})
	c.Assert(err, NotNil)
}

var _ = Suite(&lightningServerSuite{})

type lightningServerSuite struct {
	lightning *Lightning
	taskCfgCh chan *config.Config
}

func (s *lightningServerSuite) SetUpTest(c *C) {
	cfg := config.NewGlobalConfig()
	cfg.TiDB.Host = "test.invalid"
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "test.invalid:2379"
	cfg.App.ServerMode = true
	cfg.App.StatusAddr = "127.0.0.1:0"

	s.lightning = New(cfg)
	s.taskCfgCh = make(chan *config.Config)
	s.lightning.ctx = context.WithValue(s.lightning.ctx, &taskCfgRecorderKey, s.taskCfgCh)
	s.lightning.GoServe()

	failpoint.Enable("github.com/pingcap/tidb-lightning/lightning/SkipRunTask", "return")
}

func (s *lightningServerSuite) TearDownTest(c *C) {
	failpoint.Disable("github.com/pingcap/tidb-lightning/lightning/SkipRunTask")
	s.lightning.Stop()
}

func (s *lightningServerSuite) TestRunServer(c *C) {
	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	resp, err := http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	var data map[string]string
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Equals, "server-mode not enabled")
	resp.Body.Close()

	go s.lightning.RunServer()

	req, err := http.NewRequest(http.MethodPut, url, nil)
	c.Assert(err, IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusMethodNotAllowed)
	c.Assert(resp.Header.Get("Allow"), Matches, ".*"+http.MethodPost+".*")
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Matches, "cannot parse task.*")
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("[mydumper.csv]\nseparator = 'fooo'"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Matches, "invalid task configuration:.*")
	resp.Body.Close()

	for i := 0; i < 20; i++ {
		resp, err = http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'demo-path-%d'
			[mydumper.csv]
			separator = '/'
		`, i)))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		var result map[string]int
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(result, HasKey, "id")

		select {
		case taskCfg := <-s.taskCfgCh:
			c.Assert(taskCfg.TiDB.Host, Equals, "test.invalid")
			c.Assert(taskCfg.Mydumper.SourceDir, Equals, fmt.Sprintf("demo-path-%d", i))
			c.Assert(taskCfg.Mydumper.CSV.Separator, Equals, "/")
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("task is not queued after 500ms (i = %d)", i)
		}
	}
}

func (s *lightningServerSuite) TestGetDeleteTask(c *C) {
	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	type getAllResultType struct {
		Current int64
		Queue   []int64
	}

	getAllTasks := func() (result getAllResultType) {
		resp, err := http.Get(url)
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		return
	}

	postTask := func(i int) int64 {
		resp, err := http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'demo-path-%d'
		`, i)))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		var result struct{ ID int64 }
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		return result.ID
	}

	go s.lightning.RunServer()

	// Check `GET /tasks` without any active tasks

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: 0,
		Queue:   []int64{},
	})

	first := postTask(1)
	second := postTask(2)
	third := postTask(3)

	c.Assert(first, Not(Equals), 123456)
	c.Assert(second, Not(Equals), 123456)
	c.Assert(third, Not(Equals), 123456)

	// Check `GET /tasks` returns all tasks currently running

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: first,
		Queue:   []int64{second, third},
	})

	// Check `GET /tasks/abcdef` returns error

	resp, err := http.Get(url + "/abcdef")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `GET /tasks/123456` returns not found

	resp, err = http.Get(url + "/123456")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// Check `GET /tasks/1` returns the desired cfg

	var resCfg config.Config

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, second))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resCfg.Mydumper.SourceDir, Equals, "demo-path-2")

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, first))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resCfg.Mydumper.SourceDir, Equals, "demo-path-1")

	// Check `DELETE /tasks` returns error.

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	c.Assert(err, IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/` returns error.

	req.URL.Path = "/tasks/"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/(not a number)` returns error.

	req.URL.Path = "/tasks/abcdef"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/123456` returns not found

	req.URL.Path = "/tasks/123456"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// Cancel a queued task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", second)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp.Body.Close()

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: first,
		Queue:   []int64{third},
	})

	// Cancel a running task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", first)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp.Body.Close()

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: third,
		Queue:   []int64{},
	})
}
