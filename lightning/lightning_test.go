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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

type lightningSuite struct{}

var _ = Suite(&lightningSuite{})

func TestLightning(t *testing.T) {
	TestingT(t)
}

func (s *lightningSuite) TestInitEnv(c *C) {
	cfg := &config.Config{
		App: config.Lightning{ProfilePort: 45678},
	}
	err := initEnv(cfg)
	c.Assert(err, IsNil)
	cfg.App.ProfilePort = 0
	cfg.App.Config.File = "."
	err = initEnv(cfg)
	c.Assert(err, ErrorMatches, "can't use directory as log file name")
}

func (s *lightningSuite) TestRun(c *C) {
	cfg := &config.Config{}
	cfg.Mydumper.SourceDir = "not-exists"
	lightning := New(cfg)
	err := lightning.Run()
	c.Assert(err, ErrorMatches, ".*mydumper dir does not exist")

	cfg.Mydumper.SourceDir = "."
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = "invalid"
	lightning = New(cfg)
	err = lightning.Run()
	c.Assert(err, ErrorMatches, "Unknown checkpoint driver invalid")

	cfg.Mydumper.SourceDir = "."
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = "file"
	cfg.Checkpoint.DSN = "any-file"
	lightning = New(cfg)
	err = lightning.Run()
	c.Assert(err, ErrorMatches, ".*connect: can't assign requested address")
}
