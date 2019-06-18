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

func (s *lightningSuite) TestRunServer(c *C) {
	cfg := config.NewGlobalConfig()
	cfg.TiDB.Host = "test.invalid"
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "test.invalid:2379"
	cfg.App.ServerMode = true
	cfg.App.StatusAddr = "127.0.0.1:45678"

	lightning := New(cfg)
	go lightning.Serve()
	defer lightning.Stop()

	url := "http://127.0.0.1:45678/tasks"

	// Wait a bit until the server is really listening.
	time.Sleep(500 * time.Millisecond)

	req, err := http.NewRequest(http.MethodPut, url, nil)
	c.Assert(err, IsNil)
	resp, err := http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusMethodNotAllowed)
	c.Assert(resp.Header.Get("Allow"), Equals, http.MethodGet+", "+http.MethodPost)
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	var data map[string]string
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Equals, "server-mode not enabled")
	resp.Body.Close()

	go lightning.RunServer()

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

	taskCfgCh := make(chan *config.Config)
	lightning.ctx = context.WithValue(lightning.ctx, &taskCfgRecorderKey, taskCfgCh)
	failpoint.Enable("github.com/pingcap/tidb-lightning/lightning/SkipRunTask", "return")
	defer failpoint.Disable("github.com/pingcap/tidb-lightning/lightning/SkipRunTask")

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
		c.Assert(err, IsNil)
		c.Assert(result, HasKey, "id")
		resp.Body.Close()

		select {
		case taskCfg := <-taskCfgCh:
			c.Assert(taskCfg.TiDB.Host, Equals, "test.invalid")
			c.Assert(taskCfg.Mydumper.SourceDir, Equals, fmt.Sprintf("demo-path-%d", i))
			c.Assert(taskCfg.Mydumper.CSV.Separator, Equals, "/")
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("task is not queued after 500ms (i = %d)", i)
		}
	}
}
