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

package backend

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	tidbkv "github.com/pingcap/tidb/kv"
)

type kvSuite struct{}

var _ = Suite(&kvSuite{})

func TestKV(t *testing.T) {
	TestingT(t)
}

func (s *kvSuite) TestSetOption(c *C) {
	session := newSession(&SessionOptions{SQLMode: mysql.ModeNone, Timestamp: 1234567890})
	txn, err := session.Txn(true)
	c.Assert(err, IsNil)
	txn.SetOption(tidbkv.Priority, tidbkv.PriorityHigh)
}
