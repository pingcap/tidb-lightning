package backend

import (
	"bytes"
	"context"
	"sync"

	"github.com/pingcap/br/pkg/restore"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
)

type testClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*restore.RegionInfo
	regionsInfo  *core.RegionsInfo // For now it's only used in ScanRegions
	nextRegionID uint64
	splitCount   int
}

func newTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*restore.RegionInfo,
	nextRegionID uint64,
) *testClient {
	regionsInfo := core.NewRegionsInfo()
	for _, regionInfo := range regions {
		regionsInfo.AddRegion(core.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &testClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
	}
}

func (c *testClient) GetAllRegions() map[uint64]*restore.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *testClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testClient) GetRegionByID(ctx context.Context, regionID uint64) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testClient) SplitRegion(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	key []byte,
) (*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *restore.RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &restore.RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.regionsInfo.SetRegion(core.NewRegionInfo(newRegion.Region, newRegion.Leader))
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	c.regionsInfo.SetRegion(core.NewRegionInfo(target.Region, target.Leader))
	return newRegion, nil
}

func (c *testClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) (*restore.RegionInfo, []*restore.RegionInfo, error) {
	c.splitCount++
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*restore.RegionInfo, 0)
	var region *restore.RegionInfo
	for _, key := range keys {
		var target *restore.RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &restore.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.regionsInfo.SetRegion(core.NewRegionInfo(newRegion.Region, newRegion.Leader))
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.regions[target.Region.Id] = target
		region = target
		newRegions = append(newRegions, newRegion)
	}
	if region != nil {
		c.regionsInfo.SetRegion(core.NewRegionInfo(region.Region, region.Leader))
	}

	return region, newRegions, nil
}

func (c *testClient) BatchSplitRegions(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) ([]*restore.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *testClient) ScatterRegion(ctx context.Context, regionInfo *restore.RegionInfo) error {
	return nil
}

func (c *testClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *testClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*restore.RegionInfo, error) {
	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*restore.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &restore.RegionInfo{
			Region: info.GetMeta(),
			Leader: info.GetLeader(),
		})
	}
	return regions, nil
}

func (c *testClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r placement.Rule, err error) {
	return
}

func (c *testClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	return nil
}

func (c *testClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *testClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient() *testClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*restore.RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &restore.RegionInfo{
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestClient(stores, regions, 6)
}

func checkRegionRanges(c *C, regions []*restore.RegionInfo, keys [][]byte) {
	c.Assert(len(regions)+1, Equals, len(keys))
	for i, r := range regions {
		_, regionStart, _ := codec.DecodeBytes(r.Region.StartKey, []byte{})
		_, regionEnd, _ := codec.DecodeBytes(r.Region.EndKey, []byte{})
		c.Assert(regionStart, DeepEquals, keys[i])
		c.Assert(regionEnd, DeepEquals, keys[i+1])
	}
}

func (s *localSuite) TestBatchSplitAndIngestRegion(c *C) {
	oldLimit := maxBatchSplitKeys
	maxBatchSplitKeys = 4
	defer func() {
		maxBatchSplitKeys = oldLimit
	}()

	client := initTestClient()
	local := &local{
		splitCli: client,
	}
	ctx := context.Background()

	// current region ranges: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
	rangeStart := codec.EncodeBytes([]byte{}, []byte("b"))
	rangeEnd := codec.EncodeBytes([]byte{}, []byte("c"))
	regions, err := paginateScanRegion(ctx, client, rangeStart, rangeEnd, 1)
	c.Assert(err, IsNil)
	// regions is: [aay, bba), [bba, bbh), [bbh, cca)
	checkRegionRanges(c, regions, [][]byte{[]byte("aay"), []byte("bba"), []byte("bbh"), []byte("cca")})

	// generate:  ranges [b, ba), [ba, bb), [bb, bc), ... [by, bz)
	ranges := make([]Range, 0)
	start := []byte{'b'}
	for i := byte('a'); i <= 'z'; i++ {
		end := []byte{'b', i}
		ranges = append(ranges, Range{start: start, end: end})
		start = end
	}

	//
	err = local.SplitAndScatterRegionByRanges(ctx, ranges)
	c.Assert(err, IsNil)

	// so with a batch split size of 4, there will be 7 time batch split
	// 1. region: [aay, bba), keys: [b, ba, bb]
	// 2. region: [bbh, cca), keys: [bc, bd, be, bf]
	// 3. region: [bf, cca), keys: [bg, bh, bi, bj]
	// 4. region: [bj, cca), keys: [bk, bl, bm, bn]
	// 5. region: [bn, cca), keys: [bo, bp, bq, br]
	// 6. region: [br, cca), keys: [bs, bt, bu, bv]
	// 7. region: [bv, cca), keys: [bw, bx, by, bz]
	c.Assert(client.splitCount, Equals, 7)

	// check split ranges
	regions, err = paginateScanRegion(ctx, client, rangeStart, rangeEnd, 1)
	c.Assert(err, IsNil)
	result := [][]byte{[]byte("b"), []byte("ba"), []byte("bb"), []byte("bba"), []byte("bbh"), []byte("bc"),
		[]byte("bd"), []byte("be"), []byte("bf"), []byte("bg"), []byte("bh"), []byte("bi"), []byte("bj"),
		[]byte("bk"), []byte("bl"), []byte("bm"), []byte("bn"), []byte("bo"), []byte("bp"), []byte("bq"),
		[]byte("br"), []byte("bs"), []byte("bt"), []byte("bu"), []byte("bv"), []byte("bw"), []byte("bx"),
		[]byte("by"), []byte("bz"), []byte("cca")}
	checkRegionRanges(c, regions, result)
}
