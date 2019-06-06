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
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"
	"fmt"
	"context"

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

	resp, err := http.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusMethodNotAllowed)
	c.Assert(resp.Header.Get("Allow"), Equals, http.MethodPost)
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(data, BytesEquals, []byte("server-mode not enabled\n"))
	resp.Body.Close()

	go lightning.RunServer()

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	data, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(data), Matches, "cannot parse task.*\n")
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("[mydumper.csv]\nseparator = 'fooo'"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	data, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(data), Matches, "invalid task configuration:.*\n")
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

		select {
		case taskCfg := <-taskCfgCh:
			c.Assert(taskCfg.TiDB.Host, Equals, "test.invalid")
			c.Assert(taskCfg.Mydumper.SourceDir, Equals, fmt.Sprintf("demo-path-%d", i))
			c.Assert(taskCfg.Mydumper.CSV.Separator, Equals, "/")
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("task is not queued after 500ms (i = %d)", i)
		}
	}

	// Check that queuing too many tasks will cause the server to reject it.
	for i := 0; i < 80; i++ {
		_, err = http.Post(url, "application/toml", strings.NewReader(""))
		c.Assert(err, IsNil)
	}
	resp, err = http.Post(url, "application/toml", strings.NewReader(""))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusServiceUnavailable)
}

