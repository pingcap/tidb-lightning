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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/br/pkg/restore"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"

	"github.com/pingcap/tidb-lightning/lightning/common"
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

	// test recode key
	// key with int handle
	for _, handleId := range []int64{1, 255, math.MaxInt32} {
		key := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(handleId))
		c.Assert(nextKey(key), DeepEquals, []byte(tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(handleId+1))))
	}

	testDatums := [][]types.Datum{
		{types.NewIntDatum(1), types.NewIntDatum(2)},
		{types.NewIntDatum(255), types.NewIntDatum(256)},
		{types.NewIntDatum(math.MaxInt32), types.NewIntDatum(math.MaxInt32 + 1)},
		{types.NewStringDatum("test"), types.NewStringDatum("test\000")},
		{types.NewStringDatum("test\255"), types.NewStringDatum("test\255\000")},
	}

	stmtCtx := new(stmtctx.StatementContext)
	for _, datums := range testDatums {
		keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(123), datums[0])
		c.Assert(err, IsNil)
		h, err := kv.NewCommonHandle(keyBytes)
		c.Assert(err, IsNil)
		key := tablecodec.EncodeRowKeyWithHandle(1, h)
		nextKeyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(123), datums[1])
		c.Assert(err, IsNil)
		nextHdl, err := kv.NewCommonHandle(nextKeyBytes)
		c.Assert(err, IsNil)
		expectNextKey := []byte(tablecodec.EncodeRowKeyWithHandle(1, nextHdl))
		c.Assert(nextKey(key), DeepEquals, expectNextKey)
	}

	// dIAAAAAAAAD/PV9pgAAAAAD/AAABA4AAAAD/AAAAAQOAAAD/AAAAAAEAAAD8
	// a index key with: table: 61, index: 1, int64: 1, int64: 1
	a := []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 61, 95, 105, 128, 0, 0, 0, 0, 255, 0, 0, 1, 3, 128, 0, 0, 0, 255, 0, 0, 0, 1, 3, 128, 0, 0, 255, 0, 0, 0, 0, 1, 0, 0, 0, 252}
	c.Assert(nextKey(a), DeepEquals, append(a, 0))
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

func testLocalWriter(c *C, needSort bool, partitialSort bool) {
	dir := c.MkDir()
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	c.Assert(err, IsNil)
	tmpPath := filepath.Join(dir, "tmp")
	err = os.Mkdir(tmpPath, 0755)
	c.Assert(err, IsNil)
	meta := localFileMeta{}
	_, engineUUID := MakeUUID("ww", 0)
	f := LocalFile{localFileMeta: meta, db: db, Uuid: engineUUID}
	w := openLocalWriter(&f, tmpPath, 1024*1024)

	ctx := context.Background()
	//kvs := make(kvPairs, 1000)
	var kvs kvPairs
	value := make([]byte, 128)
	for i := 0; i < 16; i++ {
		binary.BigEndian.PutUint64(value[i*8:], uint64(i))
	}
	var keys [][]byte
	for i := 1; i <= 20000; i++ {
		var kv common.KvPair
		kv.Key = make([]byte, 16)
		kv.Val = make([]byte, 128)
		copy(kv.Val, value)
		key := rand.Intn(1000)
		binary.BigEndian.PutUint64(kv.Key, uint64(key))
		binary.BigEndian.PutUint64(kv.Key[8:], uint64(i))
		kvs = append(kvs, kv)
		keys = append(keys, kv.Key)
	}
	var rows1 kvPairs
	var rows2 kvPairs
	var rows3 kvPairs
	rows4 := kvs[:12000]
	if partitialSort {
		sort.Slice(rows4, func(i, j int) bool {
			return bytes.Compare(rows4[i].Key, rows4[j].Key) < 0
		})
		rows1 = rows4[:6000]
		rows3 = rows4[6000:]
		rows2 = kvs[12000:]
	} else {
		if needSort {
			sort.Slice(kvs, func(i, j int) bool {
				return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
			})
		}
		rows1 = kvs[:6000]
		rows2 = kvs[6000:12000]
		rows3 = kvs[12000:]
	}
	err = w.AppendRows(ctx, "", []string{}, 1, rows1)
	c.Assert(err, IsNil)
	err = w.AppendRows(ctx, "", []string{}, 1, rows2)
	c.Assert(err, IsNil)
	err = w.AppendRows(ctx, "", []string{}, 1, rows3)
	c.Assert(err, IsNil)
	err = w.Close()
	c.Assert(err, IsNil)
	err = db.Flush()
	c.Assert(err, IsNil)
	o := &pebble.IterOptions{}
	it := db.NewIter(o)

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	c.Assert(int(f.Length.Load()), Equals, 20000)
	c.Assert(int(f.TotalSize.Load()), Equals, 144*20000)
	valid := it.SeekGE(keys[0])
	c.Assert(valid, IsTrue)
	for _, k := range keys {
		c.Assert(it.Key(), DeepEquals, k)
		it.Next()
	}
}

func (s *localSuite) TestLocalWriterWithSort(c *C) {
	testLocalWriter(c, false, false)
}

func (s *localSuite) TestLocalWriterWithIngest(c *C) {
	testLocalWriter(c, true, false)
}

func (s *localSuite) TestLocalWriterWithIngestUnsort(c *C) {
	testLocalWriter(c, true, true)
}

type mockSplitClient struct {
	restore.SplitClient
}

func (c *mockSplitClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
	return &restore.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: key,
		},
	}, nil
}

func (s *localSuite) TestIsIngestRetryable(c *C) {
	local := &local{
		splitCli: &mockSplitClient{},
	}

	resp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				Leader: &metapb.Peer{Id: 2},
			},
		},
	}
	ctx := context.Background()
	region := &restore.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: []byte{1},
			EndKey:   []byte{3},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		},
	}
	meta := &sst.SSTMeta{
		Range: &sst.Range{
			Start: []byte{1},
			End:   []byte{2},
		},
	}
	retryType, newRegion, err := local.isIngestRetryable(ctx, resp, region, meta)
	c.Assert(retryType, Equals, retryWrite)
	c.Assert(newRegion.Leader.Id, Equals, uint64(2))
	c.Assert(err, NotNil)

	resp.Error = &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:       1,
					StartKey: []byte{1},
					EndKey:   []byte{3},
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: 1,
						Version: 2,
					},
					Peers: []*metapb.Peer{{Id: 1}},
				},
			},
		},
	}
	retryType, newRegion, err = local.isIngestRetryable(ctx, resp, region, meta)
	c.Assert(retryType, Equals, retryWrite)
	c.Assert(newRegion.Region.RegionEpoch.Version, Equals, uint64(2))
	c.Assert(err, NotNil)

	resp.Error = &errorpb.Error{Message: "raft: proposal dropped"}
	retryType, newRegion, err = local.isIngestRetryable(ctx, resp, region, meta)
	c.Assert(retryType, Equals, retryWrite)

	resp.Error = &errorpb.Error{Message: "unknown error"}
	retryType, newRegion, err = local.isIngestRetryable(ctx, resp, region, meta)
	c.Assert(retryType, Equals, retryNone)
	c.Assert(err, ErrorMatches, "non-retryable error: unknown error")
}
