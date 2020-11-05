package backend

import (
	"bytes"
	"time"

	"github.com/google/btree"
	. "github.com/pingcap/check"
)

type localSuite struct{}

var _ = Suite(&localSuite{})

func (s *localSuite) TestNextKey(c *C) {
	c.Assert(nextKey([]byte{}), DeepEquals, []byte{})

	cases := [][]byte{
		{0},
		{255},
		{1, 255},
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

func checkTreeNodes(c *C, rt *rangeLockTree, ranges []Range) {
	idx := 0
	rt.BTree.Ascend(func(i btree.Item) bool {
		n := i.(*LockedRange)
		c.Assert(n.Range, DeepEquals, ranges[idx])
		idx++
		return true
	})
	c.Assert(idx, Equals, len(ranges))
}

func (s *localSuite) TestRangeTree(c *C) {
	rt := &rangeLockTree{BTree: btree.New(2)}

	finishChan := make(chan struct{})
	asyncPut := func(r Range) {
		go func(r Range) {
			rt.put(r)
			finishChan <- struct{}{}
		}(r)
	}

	checkNonFinish := func() {
		select {
		case <-finishChan:
			c.Fatal("")
		case <-time.After(10 * time.Millisecond):
		}
	}

	r1 := Range{start: []byte{0}, end: []byte{5}}
	rt.put(r1)
	checkTreeNodes(c, rt, []Range{r1})

	r2 := Range{start: []byte{10}, end: []byte{15}}
	rt.put(r2)
	checkTreeNodes(c, rt, []Range{r1, r2})

	// insert a range with overlap
	r3 := Range{start: []byte{3}, end: []byte{12}}
	asyncPut(r3)
	checkNonFinish()
	checkTreeNodes(c, rt, []Range{r1, r2})

	// add another no-overlap range, shouldn't be blocked
	r4 := Range{start: []byte{20}, end: []byte{25}}
	rt.put(r4)
	checkTreeNodes(c, rt, []Range{r1, r2, r4})

	r5 := Range{start: []byte{11}, end: []byte{20}}
	asyncPut(r5)
	checkNonFinish()
	checkTreeNodes(c, rt, []Range{r1, r2, r4})

	// remove r2, r5 should be inserted now
	rt.remove(r2)
	<-finishChan

	checkTreeNodes(c, rt, []Range{r1, r5, r4})
	rt.remove(r1)
	checkNonFinish()
	checkTreeNodes(c, rt, []Range{r5, r4})

	rt.remove(r4)
	checkNonFinish()
	checkTreeNodes(c, rt, []Range{r5})

	rt.remove(r5)
	<-finishChan
	checkTreeNodes(c, rt, []Range{r3})

	rt.remove(r3)
	checkTreeNodes(c, rt, []Range{})
}
