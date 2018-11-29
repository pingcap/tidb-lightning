package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/kvencoder"
)

var _ = Suite(&splitKVSuite{})

type splitKVSuite struct{}

func (s *splitKVSuite) TestSplitKV(c *C) {
	pairs := []kvenc.KvPair{
		kvenc.KvPair{
			Key: []byte{1, 2, 3},
			Val: []byte{4, 5, 6},
		},
		kvenc.KvPair{
			Key: []byte{7, 8},
			Val: []byte{9, 0},
		},
		kvenc.KvPair{
			Key: []byte{1, 2, 3, 4},
			Val: []byte{5, 6, 7, 8},
		},
		kvenc.KvPair{
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
