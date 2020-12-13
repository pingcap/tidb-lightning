// Copyright 2020 PingCAP, Inc.
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
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
	"time"

	split "github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	SplitRetryTimes       = 8
	retrySplitMaxWaitTime = 4 * time.Second
)

var (
	// the max keys count in a batch to split one region
	maxBatchSplitKeys = 4096
)

// TODO remove this file and use br internal functions
// This File include region split & scatter operation just like br.
// we can simply call br function, but we need to change some function signature of br

func (local *local) SplitAndScatterRegionByRanges(ctx context.Context, ranges []Range) error {
	if len(ranges) == 0 {
		return nil
	}

	minKey := codec.EncodeBytes([]byte{}, ranges[0].start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].end)

	log.L().Info("split and scatter region",
		zap.Binary("minKey", minKey),
		zap.Binary("maxKey", maxKey),
	)

	var err error
	scatterRegions := make([]*split.RegionInfo, 0)
	var retryKeys [][]byte
	waitTime := 1 * time.Second
	for i := 0; i < SplitRetryTimes; i++ {
		if i > 0 {
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			waitTime *= 2
			if waitTime > retrySplitMaxWaitTime {
				waitTime = retrySplitMaxWaitTime
			}
		}
		var regions []*split.RegionInfo
		regions, err = paginateScanRegion(ctx, local.splitCli, minKey, maxKey, 128)
		if err != nil {
			log.L().Warn("paginate scan region failed", zap.Binary("minKey", minKey), zap.Binary("maxKey", maxKey),
				log.ShortError(err), zap.Int("retry", i))
			continue
		}

		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}

		var splitKeyMap map[uint64][][]byte
		if len(retryKeys) > 0 {
			splitKeyMap = getSplitKeys(retryKeys, regions)
			retryKeys = retryKeys[:0]
		} else {
			splitKeyMap = getSplitKeysByRanges(ranges, regions)
		}
		for regionID, keys := range splitKeyMap {
			var newRegions []*split.RegionInfo
			region := regionMap[regionID]
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i], keys[j]) < 0
			})
			splitRegion := region
			for j := 0; j < (len(keys)+maxBatchSplitKeys-1)/maxBatchSplitKeys; j++ {
				start := j * maxBatchSplitKeys
				end := utils.MinInt((j+1)*maxBatchSplitKeys, len(keys))
				splitRegionStart := codec.EncodeBytes([]byte{}, keys[start])
				splitRegionEnd := codec.EncodeBytes([]byte{}, keys[end-1])
				if bytes.Compare(splitRegionStart, splitRegion.Region.StartKey) <= 0 || !beforeEnd(splitRegionEnd, splitRegion.Region.EndKey) {
					log.L().Fatal("no valid key in region",
						zap.Binary("startKey", splitRegionStart), zap.Binary("endKey", splitRegionEnd),
						zap.Binary("regionStart", splitRegion.Region.StartKey), zap.Binary("regionEnd", splitRegion.Region.EndKey),
						zap.Reflect("region", splitRegion))
				}
				splitRegion, newRegions, err = local.BatchSplitRegions(ctx, splitRegion, keys[start:end])
				if err != nil {
					if strings.Contains(err.Error(), "no valid key") {
						for _, key := range keys {
							log.L().Warn("no valid key",
								zap.Binary("startKey", region.Region.StartKey),
								zap.Binary("endKey", region.Region.EndKey),
								zap.Binary("key", codec.EncodeBytes([]byte{}, key)))
						}
						return errors.Trace(err)
					}
					log.L().Warn("split regions", log.ShortError(err), zap.Int("retry time", j+1),
						zap.Uint64("region_id", regionID))
					retryKeys = append(retryKeys, keys[start:]...)
					break
				} else {
					sort.Slice(newRegions, func(i, j int) bool {
						return bytes.Compare(newRegions[i].Region.StartKey, newRegions[j].Region.StartKey) < 0
					})
					scatterRegions = append(scatterRegions, newRegions...)
					// the region with the max start key is the region need to be further split.
					if bytes.Compare(splitRegion.Region.StartKey, newRegions[len(newRegions)-1].Region.StartKey) < 0 {
						splitRegion = newRegions[len(newRegions)-1]
					}
				}
			}
		}
		if len(retryKeys) == 0 {
			break
		} else {
			sort.Slice(retryKeys, func(i, j int) bool {
				return bytes.Compare(retryKeys[i], retryKeys[j]) < 0
			})
			minKey = retryKeys[0]
			maxKey = nextKey(retryKeys[len(retryKeys)-1])
		}
	}
	if err != nil {
		return errors.Trace(err)
	}

	startTime := time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		local.waitForScatterRegion(ctx, region)
		if time.Since(startTime) > split.ScatterWaitUpperInterval {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.L().Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.L().Info("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

func paginateScanRegion(
	ctx context.Context, client split.SplitClient, startKey, endKey []byte, limit int,
) ([]*split.RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.Errorf("startKey > endKey, startKey %s, endkey %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	var regions []*split.RegionInfo
	for {
		batch, err := client.ScanRegions(ctx, startKey, endKey, limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regions = append(regions, batch...)
		if len(batch) < limit {
			// No more region
			break
		}
		startKey = batch[len(batch)-1].Region.GetEndKey()
		if len(startKey) == 0 ||
			(len(endKey) > 0 && bytes.Compare(startKey, endKey) >= 0) {
			// All key space have scanned
			break
		}
	}
	return regions, nil
}

func (local *local) BatchSplitRegions(ctx context.Context, region *split.RegionInfo, keys [][]byte) (*split.RegionInfo, []*split.RegionInfo, error) {
	region, newRegions, err := local.splitCli.BatchSplitRegionsWithOrigin(ctx, region, keys)
	if err != nil {
		return nil, nil, err
	}
	var failedErr error
	retryRegions := make([]*split.RegionInfo, 0)
	scatterRegions := newRegions
	waitTime := time.Second
	for i := 0; i < maxRetryTimes; i++ {
		for _, region := range scatterRegions {
			// Wait for a while until the regions successfully splits.
			local.waitForSplit(ctx, region.Region.Id)
			if err = local.splitCli.ScatterRegion(ctx, region); err != nil {
				failedErr = err
				retryRegions = append(retryRegions, region)
			}
		}
		if len(retryRegions) == 0 {
			break
		}
		// the scatter operation likely fails because region replicate not finish yet
		// pack them to one log to avoid printing a lot warn logs.
		log.L().Warn("scatter region failed", zap.Int("regionCount", len(newRegions)),
			zap.Int("failedCount", len(retryRegions)), zap.Error(failedErr), zap.Int("retry", i))
		scatterRegions = retryRegions
		retryRegions = make([]*split.RegionInfo, 0)
		select {
		case <-time.After(waitTime):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		waitTime *= 2
	}

	return region, newRegions, nil
}

func (local *local) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := local.splitCli.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func (local *local) waitForSplit(ctx context.Context, regionID uint64) {
	for i := 0; i < split.SplitCheckMaxRetryTimes; i++ {
		ok, err := local.hasRegion(ctx, regionID)
		if err != nil {
			log.L().Info("wait for split failed", log.ShortError(err))
			return
		}
		if ok {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (local *local) waitForScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) {
	regionID := regionInfo.Region.GetId()
	for i := 0; i < split.ScatterWaitMaxRetryTimes; i++ {
		ok, err := local.isScatterRegionFinished(ctx, regionID)
		if err != nil {
			log.L().Warn("scatter region failed: do not have the region",
				zap.Stringer("region", regionInfo.Region))
			return
		}
		if ok {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (local *local) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, error) {
	resp, err := local.splitCli.GetOperator(ctx, regionID)
	if err != nil {
		return false, err
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		// don't return error if region replicate not complete
		// TODO: should add a new error type to avoid this check by string matching
		matches, _ := regexp.MatchString("region \\d+ is not fully replicated", respErr.Message)
		if matches {
			return false, nil
		}
		return false, errors.Errorf("get operator error: %s", respErr.GetType())
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func getSplitKeysByRanges(ranges []Range, regions []*split.RegionInfo) map[uint64][][]byte {
	checkKeys := make([][]byte, 0)
	var lastEnd []byte
	for _, rg := range ranges {
		if !bytes.Equal(lastEnd, rg.start) {
			checkKeys = append(checkKeys, rg.start)
		}
		checkKeys = append(checkKeys, rg.end)
		lastEnd = rg.end
	}
	return getSplitKeys(checkKeys, regions)
}

func getSplitKeys(checkKeys [][]byte, regions []*split.RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	for _, key := range checkKeys {
		if region := needSplit(key, regions); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			log.L().Debug("get key for split region",
				zap.Binary("key", key),
				zap.Binary("startKey", region.Region.StartKey),
				zap.Binary("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// needSplit checks whether a key is necessary to split, if true returns the split region
func needSplit(key []byte, regions []*split.RegionInfo) *split.RegionInfo {
	// If splitKey is the max key.
	if len(key) == 0 {
		return nil
	}
	splitKey := codec.EncodeBytes([]byte{}, key)

	for _, region := range regions {
		// If splitKey is the boundary of the region
		if bytes.Equal(splitKey, region.Region.GetStartKey()) {
			return nil
		}
		// If splitKey is in a region
		if bytes.Compare(splitKey, region.Region.GetStartKey()) > 0 && beforeEnd(splitKey, region.Region.GetEndKey()) {
			log.L().Debug("need split",
				zap.Binary("splitKey", key),
				zap.Binary("encodedKey", splitKey),
				zap.Binary("region start", region.Region.GetStartKey()),
				zap.Binary("region end", region.Region.GetEndKey()),
			)
			return region
		}
	}
	return nil
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func insideRegion(region *metapb.Region, meta *sst.SSTMeta) bool {
	rg := meta.GetRange()
	return keyInsideRegion(region, rg.GetStart()) && keyInsideRegion(region, rg.GetEnd())
}

func keyInsideRegion(region *metapb.Region, key []byte) bool {
	return bytes.Compare(key, region.GetStartKey()) >= 0 && (beforeEnd(key, region.GetEndKey()))
}

func intersectRange(region *metapb.Region, rg Range) Range {
	var startKey, endKey []byte
	if len(region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.StartKey, []byte{})
	}
	if bytes.Compare(startKey, rg.start) < 0 {
		startKey = rg.start
	}
	if len(region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.EndKey, []byte{})
	}
	if beforeEnd(rg.end, endKey) {
		endKey = rg.end
	}

	return Range{start: startKey, end: endKey}
}
