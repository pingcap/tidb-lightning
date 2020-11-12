package backend

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/btree"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/hack"
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

// The first half of this test is same as the test in tikv:
// https://github.com/tikv/tikv/blob/dbfe7730dd0fddb34cb8c3a7f8a079a1349d2d41/components/engine_rocks/src/properties.rs#L572
func (s *localSuite) TestRangeProperties(c *C) {
	type testCase struct {
		key   []byte
		vLen  int
		count int
	}
	cases := []testCase{
		// handle "a": size(size = 1, offset = 1),keys(1,1)
		{[]byte("a"), 0, 1},
		{[]byte("b"), defaultPropSizeIndexDistance / 8, 1},
		{[]byte("c"), defaultPropSizeIndexDistance / 4, 1},
		{[]byte("d"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("e"), defaultPropSizeIndexDistance / 8, 1},
		// handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
		{[]byte("f"), defaultPropSizeIndexDistance / 4, 1},
		{[]byte("g"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("h"), defaultPropSizeIndexDistance / 8, 1},
		{[]byte("i"), defaultPropSizeIndexDistance / 4, 1},
		// handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),keys(4,5)
		{[]byte("j"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("k"), defaultPropSizeIndexDistance / 2, 1},
		// handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
		{[]byte("l"), 0, defaultPropKeysIndexDistance / 2},
		{[]byte("m"), 0, defaultPropKeysIndexDistance / 2},
		//handle "m": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset = 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
		{[]byte("n"), 1, defaultPropKeysIndexDistance},
		//handle "n": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset = 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
		{[]byte("o"), 1, 1},
		// handle　"o": keys = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
	}

	collector := newRangePropertiesCollector()
	for _, p := range cases {
		v := make([]byte, p.vLen)
		for i := 0; i < p.count; i++ {
			_ = collector.Add(pebble.InternalKey{UserKey: p.key}, v)
		}
	}

	userProperties := make(map[string]string, 1)
	_ = collector.Finish(userProperties)

	props, err := decodeRangeProperties(hack.Slice(userProperties[propRangeIndex]))
	c.Assert(err, IsNil)

	// Smallest key in props.
	c.Assert(props[0].Key, DeepEquals, cases[0].key)
	// Largest key in props.
	c.Assert(props[len(props)-1].Key, DeepEquals, cases[len(cases)-1].key)
	c.Assert(len(props), Equals, 7)

	a := props.get([]byte("a"))
	c.Assert(a.Size, Equals, uint64(1))
	e := props.get([]byte("e"))
	c.Assert(e.Size, Equals, uint64(defaultPropSizeIndexDistance+5))
	i := props.get([]byte("i"))
	c.Assert(i.Size, Equals, uint64(defaultPropSizeIndexDistance/8*17+9))
	k := props.get([]byte("k"))
	c.Assert(k.Size, Equals, uint64(defaultPropSizeIndexDistance/8*25+11))
	m := props.get([]byte("m"))
	c.Assert(m.Keys, Equals, uint64(defaultPropKeysIndexDistance+11))
	n := props.get([]byte("n"))
	c.Assert(n.Keys, Equals, uint64(defaultPropKeysIndexDistance*2+11))
	o := props.get([]byte("o"))
	c.Assert(o.Keys, Equals, uint64(defaultPropKeysIndexDistance*2+12))

	props2 := rangeProperties([]rangeProperty{
		{[]byte("b"), rangeOffsets{defaultPropSizeIndexDistance + 10, defaultPropKeysIndexDistance / 2}},
		{[]byte("h"), rangeOffsets{defaultPropSizeIndexDistance * 3 / 2, defaultPropKeysIndexDistance * 3 / 2}},
		{[]byte("k"), rangeOffsets{defaultPropSizeIndexDistance * 3, defaultPropKeysIndexDistance * 7 / 4}},
		{[]byte("mm"), rangeOffsets{defaultPropSizeIndexDistance * 5, defaultPropKeysIndexDistance * 2}},
		{[]byte("q"), rangeOffsets{defaultPropSizeIndexDistance * 7, defaultPropKeysIndexDistance*9/4 + 10}},
		{[]byte("y"), rangeOffsets{defaultPropSizeIndexDistance*7 + 100, defaultPropKeysIndexDistance*9/4 + 1010}},
	})

	sizeProps := newSizeProperties()
	sizeProps.addAll(props)
	sizeProps.addAll(props2)

	res := []*rangeProperty{
		{[]byte("a"), rangeOffsets{1, 1}},
		{[]byte("b"), rangeOffsets{defaultPropSizeIndexDistance + 10, defaultPropKeysIndexDistance / 2}},
		{[]byte("e"), rangeOffsets{defaultPropSizeIndexDistance + 4, 4}},
		{[]byte("h"), rangeOffsets{defaultPropSizeIndexDistance/2 - 10, defaultPropKeysIndexDistance}},
		{[]byte("i"), rangeOffsets{defaultPropSizeIndexDistance*9/8 + 4, 4}},
		{[]byte("k"), rangeOffsets{defaultPropSizeIndexDistance*5/2 + 2, defaultPropKeysIndexDistance/4 + 2}},
		{[]byte("m"), rangeOffsets{defaultPropKeysIndexDistance, defaultPropKeysIndexDistance}},
		{[]byte("mm"), rangeOffsets{defaultPropSizeIndexDistance * 2, defaultPropKeysIndexDistance / 4}},
		{[]byte("n"), rangeOffsets{defaultPropKeysIndexDistance * 2, defaultPropKeysIndexDistance}},
		{[]byte("o"), rangeOffsets{2, 1}},
		{[]byte("q"), rangeOffsets{defaultPropSizeIndexDistance * 2, defaultPropKeysIndexDistance/4 + 10}},
		{[]byte("y"), rangeOffsets{100, 1000}},
	}

	c.Assert(sizeProps.indexHandles.Len(), Equals, 12)
	idx := 0
	sizeProps.iter(func(p *rangeProperty) bool {
		c.Assert(p, DeepEquals, res[idx])
		idx++
		return true
	})

	fullRange := Range{start: []byte("a"), end: []byte("z")}
	ranges := splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance*5/2)

	c.Assert(ranges, DeepEquals, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("k")},
		{start: []byte("k"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	})

	ranges = splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)
	c.Assert(ranges, DeepEquals, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("h")},
		{start: []byte("h"), end: []byte("k")},
		{start: []byte("k"), end: []byte("m")},
		{start: []byte("m"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("n")},
		{start: []byte("n"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	})
}

func (s *localSuite) TestRangePropertiesWithPebble(c *C) {
	dir := c.MkDir()

	sizeDistance := uint64(500)
	keysDistance := uint64(20)
	opt := &pebble.Options{
		MemTableSize:             LocalMemoryTableSize,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		MaxOpenFiles:             10000,
		DisableWAL:               true,
		ReadOnly:                 false,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			func() pebble.TablePropertyCollector {
				return &RangePropertiesCollector{
					props:               make([]rangeProperty, 0, 1024),
					propSizeIdxDistance: sizeDistance,
					propKeysIdxDistance: keysDistance,
				}
			},
		},
	}
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	c.Assert(err, IsNil)
	defer db.Close()

	// local collector
	collector := &RangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: sizeDistance,
		propKeysIdxDistance: keysDistance,
	}
	writeOpt := &pebble.WriteOptions{Sync: false}
	value := make([]byte, 100)
	for i := 0; i < 10; i++ {
		wb := db.NewBatch()
		for j := 0; j < 100; j++ {
			key := make([]byte, 8)
			valueLen := rand.Intn(50)
			binary.BigEndian.PutUint64(key, uint64(i*100+j))
			err = wb.Set(key, value[:valueLen], writeOpt)
			c.Assert(err, IsNil)
			err = collector.Add(pebble.InternalKey{UserKey: key}, value[:valueLen])
			c.Assert(err, IsNil)
		}
		c.Assert(wb.Commit(writeOpt), IsNil)
	}
	// flush one sst
	c.Assert(db.Flush(), IsNil)

	props := make(map[string]string, 1)
	c.Assert(collector.Finish(props), IsNil)

	sstMetas, err := db.SSTables(pebble.WithProperties())
	c.Assert(err, IsNil)
	for i, level := range sstMetas {
		if i == 0 {
			c.Assert(len(level), Equals, 1)
		} else {
			c.Assert(len(level), Equals, 0)
		}
	}

	c.Assert(sstMetas[0][0].Properties.UserProperties, DeepEquals, props)
}
