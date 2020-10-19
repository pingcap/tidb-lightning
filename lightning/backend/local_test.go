package backend

import (
	"bytes"

	. "github.com/pingcap/check"
)

type localSuite struct{}

var _ = Suite(&localSuite{})

func (s *localSuite) TestNextKey(c *C) {
	c.Assert(nextKey([]byte{}), DeepEquals, []byte{})

	cases := [][]byte{
		[]byte{0},
		[]byte{255},
		[]byte{1, 255},
	}
	for _, b := range cases {
		next := nextKey(b)
		c.Assert(next, DeepEquals, append(b, 0))
	}

	// in the old logic, this should return []byte{} which is not the actually smallest eky
	next := nextKey([]byte{1, 255})
	c.Assert(bytes.Compare(next, []byte{2}), Equals, -1)

	// another test case, nextkey()'s return should be smaller than key with a prefix of the origin key
	next = nextKey([]byte{1, 255})
	c.Assert(bytes.Compare(next, []byte{1, 255, 0, 1, 2}), Equals, -1)
}
