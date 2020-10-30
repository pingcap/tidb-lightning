package backend

import (
	"bytes"

	"github.com/cockroachdb/pebble"
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

	props, err := decodeRangeProperties(hack.Slice(userProperties[PROP_RANGE_INDEX]))
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
