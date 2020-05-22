package backend

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	split "github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/log"
)

const SplitRetryTimes = 32

// TODO remove this file and use br internal functions
// This File include region split & scatter operation just like br.
// we can simply call br function, but we need to change some function signature of br

func (local *local) SplitAndScatterRegionByRanges(ctx context.Context, ranges []Range) error {
	if len(ranges) == 0 {
		// TODO log

		return nil
	}

	minKey := codec.EncodeBytes([]byte{}, ranges[0].start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].end)

	log.L().Info("split and scatter region",
		zap.Binary("minKey", minKey),
		zap.Binary("maxKey", maxKey),
	)

	regions, err := paginateScanRegion(ctx, local.splitCli, minKey, maxKey, 128)
	if err != nil {
		return err
	}

	splitKeyMap := getSplitKeys(ranges, regions)

	regionMap := make(map[uint64]*split.RegionInfo)
	for _, region := range regions {
		regionMap[region.Region.GetId()] = region
	}

	scatterRegions := make([]*split.RegionInfo, 0)
SplitRegions:
	for i := 0; i < SplitRetryTimes; i++ {
		for regionID, keys := range splitKeyMap {
			var newRegions []*split.RegionInfo
			region := regionMap[regionID]
			newRegions, errSplit := local.BatchSplitRegions(ctx, region, keys)
			if errSplit != nil {
				if strings.Contains(errSplit.Error(), "no valid key") {
					for _, key := range keys {
						log.L().Error("no valid key",
							zap.Binary("startKey", region.Region.StartKey),
							zap.Binary("endKey", region.Region.EndKey),
							zap.Binary("key", codec.EncodeBytes([]byte{}, key)))
					}
					return errors.Trace(errSplit)
				}
				log.L().Warn("split regions", zap.Error(errSplit))
				time.Sleep(time.Second)
				continue SplitRegions
			}
			scatterRegions = append(scatterRegions, newRegions...)
		}
		break
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
		log.L().Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

func paginateScanRegion(
	ctx context.Context, client split.SplitClient, startKey, endKey []byte, limit int,
) ([]*split.RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.Errorf("startKey > endKey, startKey %s, endkey %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	regions := []*split.RegionInfo{}
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

func (local *local) BatchSplitRegions(ctx context.Context, region *split.RegionInfo, keys [][]byte) ([]*split.RegionInfo, error) {
	newRegions, err := local.splitCli.BatchSplitRegions(ctx, region, keys)
	if err != nil {
		return nil, err
	}
	for _, region := range newRegions {
		// Wait for a while until the regions successfully splits.
		local.waitForSplit(ctx, region.Region.Id)
		if err = local.splitCli.ScatterRegion(ctx, region); err != nil {
			log.L().Warn("scatter region failed", zap.Stringer("region", region.Region), zap.Error(err))
		}
	}
	return newRegions, nil
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
			log.L().Warn("wait for split failed", zap.Error(err))
			return
		}
		if ok {
			break
		}
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		return false, errors.Errorf("get operator error: %s", respErr.GetType())
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func getSplitKeys(ranges []Range, regions []*split.RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	checkKeys := make([][]byte, 0)
	for _, rg := range ranges {
		checkKeys = append(checkKeys, truncateRowKey(rg.end))
	}
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
func needSplit(splitKey []byte, regions []*split.RegionInfo) *split.RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	splitKey = codec.EncodeBytes([]byte{}, splitKey)

	for _, region := range regions {
		// If splitKey is the boundary of the region
		log.L().Debug("need split",
			zap.Binary("splitKey", splitKey),
			zap.Binary("region start", region.Region.GetStartKey()),
			zap.Binary("region end", region.Region.GetEndKey()),
		)
		if bytes.Equal(splitKey, region.Region.GetStartKey()) {
			return nil
		}
		// If splitKey is in a region
		if bytes.Compare(splitKey, region.Region.GetStartKey()) > 0 && beforeEnd(splitKey, region.Region.GetEndKey()) {
			return region
		}
	}
	return nil
}

var (
	tablePrefix  = []byte{'t'}
	idLen        = 8
	recordPrefix = []byte("_r")
)

func truncateRowKey(key []byte) []byte {
	if bytes.HasPrefix(key, tablePrefix) &&
		len(key) > tablecodec.RecordRowKeyLen &&
		bytes.HasPrefix(key[len(tablePrefix)+idLen:], recordPrefix) {
		return key[:tablecodec.RecordRowKeyLen]
	}
	return key
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
