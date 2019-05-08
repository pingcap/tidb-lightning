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

package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/kvencoder"
)

var _ = Suite(&splitKVSuite{})

type splitKVSuite struct{}

func (s *splitKVSuite) TestSplitKV(c *C) {
	pairs := []kvenc.KvPair{
		{
			Key: []byte{1, 2, 3},
			Val: []byte{4, 5, 6},
		},
		{
			Key: []byte{7, 8},
			Val: []byte{9, 0},
		},
		{
			Key: []byte{1, 2, 3, 4},
			Val: []byte{5, 6, 7, 8},
		},
		{
			Key: []byte{9, 0},
			Val: []byte{1, 2},
		},
	}

	splitBy10 := splitIntoDeliveryStreams(pairs, 10)
	c.Assert(splitBy10, DeepEquals, [][]kvenc.KvPair{pairs[0:2], pairs[2:3], pairs[3:4]})

	splitBy12 := splitIntoDeliveryStreams(pairs, 12)
	c.Assert(splitBy12, DeepEquals, [][]kvenc.KvPair{pairs[0:2], pairs[2:4]})

	splitBy1000 := splitIntoDeliveryStreams(pairs, 1000)
	c.Assert(splitBy1000, DeepEquals, [][]kvenc.KvPair{pairs[0:4]})

	splitBy1 := splitIntoDeliveryStreams(pairs, 1)
	c.Assert(splitBy1, DeepEquals, [][]kvenc.KvPair{pairs[0:1], pairs[1:2], pairs[2:3], pairs[3:4]})
}
