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

package log_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

func TestLog(t *testing.T) {
	TestingT(t)
}

type logSuite struct{}

var _ = Suite(&logSuite{})

func (s *logSuite) TestConfigAdjust(c *C) {
	cfg := &log.Config{}
	cfg.Adjust()
	c.Assert(cfg.Level, Equals, "info")

	cfg.File = "."
	err := log.InitLogger(cfg, "info")
	log.L().Named("xx")
	c.Assert(err, ErrorMatches, "can't use directory as log file name")
}
