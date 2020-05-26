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
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	split "github.com/pingcap/br/pkg/restore"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

const (
	dialTimeout             = 5 * time.Second
	defaultRangeConcurrency = 32
)

// Range record start and end key for localFile.DB
// so we can write it to tikv in streaming
type Range struct {
	start  []byte
	end    []byte
	length int
}

type localFile struct {
	ts        uint64
	db        *pebble.DB
	meta      sst.SSTMeta
	length    int64
	totalSize int64

	ranges   []Range
	startKey []byte
	endKey   []byte

	uuid uuid.UUID
}

func (e *localFile) Close() error {
	return e.db.Close()
}

type grpcClis struct {
	mu   sync.Mutex
	clis map[uint64]*grpc.ClientConn
}

type local struct {
	engines  sync.Map
	grpcClis grpcClis
	splitCli split.SplitClient
	tlsConf  *tls.Config

	localFile       string
	regionSplitSize int64

	rangeConcurrency *worker.Pool
	sendKVPairs      int
	pairPool         sync.Pool
}

// NewLocal creates new connections to tikv.
func NewLocalBackend(ctx context.Context, tls *common.TLS, pdAddr string, regionSplitSize int64, localFile string, rangeConcurrency int, sendKVPairs int) (Backend, error) {
	pdCli, err := pd.NewClient([]string{pdAddr}, tls.ToPDSecurityOption())
	if err != nil {
		return MakeBackend(nil), errors.Annotate(err, "construct pd client failed")
	}
	tlsConf := tls.TransToTlsConfig()
	if err != nil {
		return MakeBackend(nil), err
	}
	splitCli := split.NewSplitClient(pdCli, tlsConf)

	err = os.Mkdir(localFile, 0700)
	if err != nil {
		return MakeBackend(nil), err
	}

	if rangeConcurrency == 0 {
		rangeConcurrency = defaultRangeConcurrency
	}

	local := &local{
		engines:  sync.Map{},
		splitCli: splitCli,
		tlsConf:  tlsConf,

		localFile:       localFile,
		regionSplitSize: regionSplitSize,

		rangeConcurrency: worker.NewPool(ctx, rangeConcurrency, "range"),
		sendKVPairs:      sendKVPairs,
		pairPool:         sync.Pool{New: func() interface{} { return &sst.Pair{} }},
	}
	local.grpcClis.clis = make(map[uint64]*grpc.ClientConn)
	return MakeBackend(local), nil
}

func (local *local) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {

	store, err := local.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if local.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(local.tlsConf))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	keepAlive := 10
	keepAliveTimeout := 3
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	conn, err := grpc.DialContext(
		ctx,
		store.GetAddress(),
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(keepAlive) * time.Second,
			Timeout:             time.Duration(keepAliveTimeout) * time.Second,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Cache the conn.
	local.grpcClis.clis[storeID] = conn
	return conn, nil
}

// Close the importer connection.
func (local *local) Close() {
	local.engines.Range(func(k, v interface{}) bool {
		v.(*localFile).Close()
		return true
	})
	err := os.RemoveAll(local.localFile)
	if err != nil {
		log.L().Error("remove local db file failed", zap.Error(err))
	}
}

func (local *local) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (local *local) MaxChunkSize() int {
	// a batch size write to leveldb
	return int(local.regionSplitSize)
}

func (local *local) ShouldPostProcess() bool {
	return true
}

func (local *local) OpenEngine(ctx context.Context, engineUUID uuid.UUID) error {
	dbPath := path.Join(local.localFile, engineUUID.String())
	cache := pebble.NewCache(128 << 20)
	defer cache.Unref()
	opt := &pebble.Options{
		Cache:                    cache,
		MemTableSize:             128 << 20,
		MaxConcurrentCompactions: 16,
		MinCompactionRate:        1024 << 20,
		L0CompactionThreshold:    2 << 31, // set to max try to disable compaction
		L0StopWritesThreshold:    2 << 31, // set to max try to disable compaction
		MaxOpenFiles:             10000,
		DisableWAL:               true,
	}
	db, err := pebble.Open(dbPath, opt)
	if err != nil {
		return err
	}
	local.engines.Store(engineUUID, &localFile{db: db, length: 0, ranges: make([]Range, 0), uuid: engineUUID})
	return nil
}

func (local *local) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engineFile, ok := local.engines.Load(engineUUID)
	if !ok {
		return errors.Errorf("could not find engine %s in CloseEngine", engineUUID.String())
	}
	db := engineFile.(*localFile).db
	return db.Flush()
}

func (local *local) getImportClient(ctx context.Context, peer *metapb.Peer) (sst.ImportSSTClient, error) {
	local.grpcClis.mu.Lock()
	defer local.grpcClis.mu.Unlock()
	var err error

	conn, ok := local.grpcClis.clis[peer.GetStoreId()]
	if !ok {
		conn, err = local.getGrpcConnLocked(ctx, peer.GetStoreId())
		if err != nil {
			log.L().Error("could not get grpc connect ", zap.Uint64("storeId", peer.GetStoreId()))
			return nil, err
		}
	}
	return sst.NewImportSSTClient(conn), nil
}

func (local *local) WriteToPeer(
	ctx context.Context,
	meta *sst.SSTMeta,
	ts uint64,
	peer *metapb.Peer,
	pairs []*sst.Pair) (metas []*sst.SSTMeta, err error) {

	select {
	case <-ctx.Done():
		return
	default:
	}

	cli, err := local.getImportClient(ctx, peer)
	if err != nil {
		return
	}

	wstream, err := cli.Write(ctx)
	if err != nil {
		return
	}

	// Bind uuid for this write request
	req := &sst.WriteRequest{
		Chunk: &sst.WriteRequest_Meta{
			Meta: meta,
		},
	}
	if err = wstream.Send(req); err != nil {
		return
	}

	startOffset := 0
	step := local.sendKVPairs
	for startOffset < len(pairs) {
		endOffset := startOffset + step
		if endOffset > len(pairs) {
			endOffset = len(pairs)
		}
		req.Reset()
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: ts,
				Pairs:    pairs[startOffset:endOffset],
			},
		}
		err = wstream.Send(req)
		if err != nil {
			return
		}
		startOffset += step
	}

	if resp, closeErr := wstream.CloseAndRecv(); closeErr != nil {
		err = closeErr
	} else {
		metas = resp.Metas
		log.L().Debug("get metas after write kv stream to tikv", zap.Reflect("metas", metas))
	}
	return
}

func (local *local) WriteToTiKV(
	ctx context.Context,
	meta *sst.SSTMeta,
	ts uint64,
	region *split.RegionInfo,
	pairs []*sst.Pair) ([]*sst.SSTMeta, error) {
	var leaderPeerMetas []*sst.SSTMeta

	select {
	case <-ctx.Done():
		return nil, nil
	default:
	}

	leaderID := region.Leader.GetId()
	for _, peer := range region.Region.GetPeers() {
		metas, err := local.WriteToPeer(ctx, meta, ts, peer, pairs)
		if err != nil {
			return nil, err
		}
		if leaderID == peer.GetId() {
			leaderPeerMetas = metas
			log.L().Debug("lock metas", zap.Reflect("metas", leaderPeerMetas))
		}
		log.L().Debug("write to kv", zap.Reflect("peer", peer),
			zap.Reflect("region", region), zap.Uint64("leader", leaderID),
			zap.Reflect("meta", meta), zap.Reflect("return metas", metas))
	}
	return leaderPeerMetas, nil
}

func (local *local) Ingest(ctx context.Context, meta *sst.SSTMeta, region *split.RegionInfo) (*sst.IngestResponse, error) {

	select {
	case <-ctx.Done():
		return nil, nil
	default:
	}

	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := local.getImportClient(ctx, leader)
	if err != nil {
		return nil, err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	req := &sst.IngestRequest{
		Context: reqCtx,
		Sst:     meta,
	}
	resp, err := cli.Ingest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (local *local) printSSTableInfos(tables [][]pebble.TableInfo, uid uuid.UUID) {
	tableCount := 0
	totalSize := uint64(0)
	maxTableSize := uint64(0)
	minTableSize := uint64(2 << 31)

	zf := make([]zap.Field, 0)
	zf = append(zf, zap.Stringer("uuid", uid))

	for i, levelTables := range tables {
		for idx, t := range levelTables {
			tableCount++
			totalSize += t.Size
			sk := append([]byte{}, t.Smallest.UserKey...)
			lk := append([]byte{}, t.Largest.UserKey...)
			zf = append(zf, zap.Int("level", i))
			zf = append(zf, zap.Int("idx", idx))
			zf = append(zf, zap.Binary("start", sk))
			zf = append(zf, zap.Binary("end", lk))
			zf = append(zf, zap.Uint64("size", t.Size))
			if t.Size > maxTableSize {
				maxTableSize = t.Size
			}
			if t.Size < minTableSize {
				minTableSize = t.Size
			}
		}
	}
	zf = append(zf, zap.Int("table count", tableCount))
	zf = append(zf, zap.Uint64("max table size", maxTableSize))
	zf = append(zf, zap.Uint64("min table size", minTableSize))
	zf = append(zf, zap.Uint64("total table size", totalSize))
	log.L().Info("ssttables summary infos", zf...)
}

func (local *local) ReadAndSplitIntoRange(engineFile *localFile, engineUUID uuid.UUID) ([]Range, error) {
	if engineFile.length == 0 {
		return nil, nil
	}
	ranges := make([]Range, 0)
	iter := engineFile.db.NewIter(nil)
	defer iter.Close()
	//size := int64(0)
	//length := 0
	var startKey, endKey []byte
	if iter.First() {
		startKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, errors.Errorf("could not find first pair, this shouldn't happen")
	}
	if iter.Last() {
		endKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, errors.Errorf("could not find last pair, this shouldn't happen")
	}

	iter1 := engineFile.db.NewIter(nil)
	total := 0
	totalBytes := 0
	for iter1.First(); iter1.Valid(); iter1.Next() {
		total ++
		totalBytes += len(iter.Key()) + len(iter.Value())
	}
	_ = iter1.Close()

	fmt.Printf("engine %s ReadAndSplitIntoRange: total_keys: %d,  total_bytes: %d\n", engineUUID, total, totalBytes)

	// <= 96MB no need to split into range
	if engineFile.totalSize <= local.regionSplitSize {
		ranges = append(ranges, Range{start: startKey, end: nextKey(endKey), length: int(engineFile.length)})
		return ranges, nil
	}

	log.L().Info("ReadAndSplitIntoRange", zap.Binary("start", startKey), zap.Binary("end", endKey))

	// split data into n ranges, then seek n times to get n + 1 ranges
	n := engineFile.totalSize / local.regionSplitSize

	if tablecodec.IsIndexKey(startKey) {
		// index engine
		tableID, startIndexID, _, err := tablecodec.DecodeIndexKey(startKey)
		if err != nil {
			return nil, err
		}
		tableID, endIndexID, _, err := tablecodec.DecodeIndexKey(endKey)
		if err != nil {
			return nil, err
		}
		indexCount := (endIndexID - startIndexID) + 1

		// each index has to split into n / indexCount ranges
		indexRangeCount := n / indexCount

		for i := startIndexID; i <= endIndexID; i++ {
			k := tablecodec.EncodeTableIndexPrefix(tableID, i)
			iter.SeekGE(k)
			// get first key of index i
			startKeyOfIndex := append([]byte{}, iter.Key()...)

			k = tablecodec.EncodeTableIndexPrefix(tableID, i+1)
			// get last key of index i
			iter.SeekLT(k)

			lastKeyOfIndex := append([]byte{}, iter.Key()...)

			_, startIndexID, startValues, err := tablecodec.DecodeIndexKeyPrefix(startKeyOfIndex)
			if err != nil {
				return nil, err
			}
			_, endIndexID, endValues, err := tablecodec.DecodeIndexKeyPrefix(lastKeyOfIndex)
			if err != nil {
				return nil, err
			}

			if startIndexID != endIndexID {
				// this shouldn't happen
				log.L().Error("index ID not match",
					zap.Int64("startID", startIndexID), zap.Int64("endID", endIndexID))
				return nil, errors.New("index ID not match")
			}

			// if index is Unique or Primary, the key is encoded as
			// tablePrefix{tableID}_indexPrefixSep{indexID}_indexedColumnsValue
			// if index is non-Unique, key is encoded as
			// tablePrefix{tableID}_indexPrefixSep{indexID}_indexedColumnsValue_rowID

			// we can split by indexColumnsValue to get indexRangeCount ranges from above Keys

			log.L().Info("split index to range",
				zap.Int64("indexID", i), zap.Int64("rangeCount", indexRangeCount),
				zap.Binary("start", startKeyOfIndex), zap.Binary("end", lastKeyOfIndex))

			values := splitValuesToRange(startValues, endValues, indexRangeCount)

			keyPrefix := tablecodec.EncodeTableIndexPrefix(tableID, i)
			for _, v := range values {
				e := append([]byte{}, append(keyPrefix, v...)...)
				ranges = append(ranges, Range{start: append([]byte{}, startKeyOfIndex...), end: nextKey(e)})
				startKeyOfIndex = nextKey(e)
			}
		}
	} else {
		// data engine
		tableID, startHandle, err := tablecodec.DecodeRecordKey(startKey)
		if err != nil {
			return nil, err
		}
		endHandle, err := tablecodec.DecodeRowKey(endKey)
		if err != nil {
			return nil, err
		}
		step := (endHandle - startHandle) / n
		index := int64(0)
		var skey, ekey []byte

		log.L().Info("data engine", zap.Int64("step", step),
			zap.Int64("startHandle", startHandle), zap.Int64("endHandle", endHandle))

		for i := startHandle; i+step <= endHandle; i += step {
			skey = tablecodec.EncodeRowKeyWithHandle(tableID, i)
			ekey = tablecodec.EncodeRowKeyWithHandle(tableID, i+step-1)
			index = i
			log.L().Debug("data engine append range", zap.Int64("start handle", i),
				zap.Int64("end handle", i+step-1), zap.Binary("start key", skey),
				zap.Binary("end key", ekey), zap.Int64("step", step))

			ranges = append(ranges, Range{start: skey, end: nextKey(ekey)})
		}
		log.L().Debug("data engine append range at final",
			zap.Int64("start handle", index+step), zap.Int64("end handle", endHandle),
			zap.Binary("start key", skey), zap.Binary("end key", endKey),
			zap.Int64("step", endHandle-index-step+1))

		skey = tablecodec.EncodeRowKeyWithHandle(tableID, index+step)
		ranges = append(ranges, Range{start: skey, end: nextKey(endKey)})
	}
	return ranges, nil
}

func (local *local) writeAndIngestByRange(
	ctx context.Context,
	db *pebble.DB,
	start, end []byte,
	ts uint64,
	engineUUID uuid.UUID) error {

	select {
	case <-ctx.Done():
		return errors.New("context is cancel by other reason")
	default:
	}

	pairs := make([]*sst.Pair, 0, 128)
	ito := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	iter := db.NewIter(ito)
	defer iter.Close()
	// Needs seek to first because NewIter returns an iterator that is unpositioned
	iter.First()

	size := int64(0)
	count := 0
	for iter.Valid() {
		size += int64(len(iter.Key()) + len(iter.Value()))
		pair := &sst.Pair{
			Key:   append([]byte{}, iter.Key()...),
			Value: append([]byte{}, iter.Value()...),
		}
		pairs = append(pairs, pair)
		count ++
		iter.Next()
	}

	fmt.Printf("engine: %v, writeAndIngestByRange count: %d, bytes: %d\n", engineUUID, count, size)

	if len(pairs) == 0 {
		log.L().Info("There is no pairs in iterator",
			zap.Binary("start", start),
			zap.Binary("end", end),
			zap.Binary("next end", nextKey(end)))
		return nil
	}

	var regions []*split.RegionInfo
	var err error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
WriteAndIngest:
	for retry := 0; retry < maxRetryTimes; retry++ {
		if retry != 0 {
			time.Sleep(time.Second)
		}
		startKey := codec.EncodeBytes([]byte{}, pairs[0].Key)
		endKey := codec.EncodeBytes([]byte{}, nextKey(pairs[len(pairs)-1].Key))
		regions, err = paginateScanRegion(ctx, local.splitCli, startKey, endKey, 128)
		if err != nil {
			log.L().Warn("scan region failed", zap.Error(err))
			continue WriteAndIngest
		}

		shouldWait := false
		errChan := make(chan error, len(regions))
		startIndex := 0
		for _, region := range regions {
			regionEnd := region.Region.EndKey
			_, endKey, _ := codec.DecodeBytes(region.Region.EndKey, []byte{})
			endIndex := sort.Search(len(pairs), func(i int) bool {
				return bytes.Compare(pairs[i].Key, endKey) >= 0
			})

			if endIndex <= startIndex {
				log.L().Warn("empty range for region", zap.Binary("rangeStart", pairs[startIndex].Key),
					zap.Binary("rangeEnd", pairs[endIndex-1].Key), zap.Reflect("region", region))
			}

			endKey = codec.EncodeBytes([]byte{}, pairs[endIndex-1].Key)

			log.L().Debug("get region", zap.Int("retry", retry), zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey), zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()), zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()), zap.Reflect("peers", region.Region.GetPeers()))

			// generate new uuid for concurrent write to tikv
			meta := &sst.SSTMeta{
				Uuid:        uuid.NewV4().Bytes(),
				RegionId:    region.Region.GetId(),
				RegionEpoch: region.Region.GetRegionEpoch(),
				Range: &sst.Range{
					Start: startKey,
					End:   endKey,
				},
			}
			if !insideRegion(region.Region, meta) {
				log.L().Error("[shouldn't happen] sst out of range",
					zap.Reflect("meta", meta), zap.Reflect("region", region))
				continue WriteAndIngest
			}

			if len(regions) == 1 {
				if err := local.WriteAndIngestPairs(ctx, meta, ts, region, pairs[startIndex:endIndex]); err != nil {
					continue WriteAndIngest
				}
			} else {
				shouldWait = true
				go func(start, end int) {
					errChan <- local.WriteAndIngestPairs(ctx, meta, ts, region, pairs[start:end])
				}(startIndex, endIndex)
			}

			startKey = regionEnd
			startIndex = endIndex
		}
		if shouldWait {
			shouldRetry := false
			for i := 0; i < len(regions); i++ {
				err = <-errChan
				if err != nil {
					log.L().Warn("should retry this range", zap.Int("retry", retry), zap.Error(err))
					shouldRetry = true
				}
			}
			if !shouldRetry {
				return nil
			} else {
				continue
			}
		}
		break
	}
	return err
}

func (local *local) WriteAndIngestPairs(
	ctx context.Context,
	meta *sst.SSTMeta,
	ts uint64,
	region *split.RegionInfo,
	pairs []*sst.Pair,
) error {
	metas, err := local.WriteToTiKV(ctx, meta, ts, region, pairs)
	if err != nil {
		log.L().Warn("write to tikv failed", zap.Error(err))
		return err
	}

	for _, meta := range metas {
		for i := 0; i < maxRetryTimes; i++ {
			log.L().Debug("ingest meta", zap.Reflect("meta", meta))
			resp, err := local.Ingest(ctx, meta, region)
			if err != nil {
				log.L().Warn("ingest failed", zap.Error(err))
				continue
			}
			needRetry, newRegion, errIngest := isIngestRetryable(resp, region, meta)
			if errIngest == nil {
				// ingest next meta
				break
			}
			if !needRetry {
				// met non-retryable error retry whole Write procedure
				return errIngest
			}

			err = errIngest
			// retry with not leader and epoch not match error
			if newRegion != nil && i < maxRetryTimes - 1 {
				region = newRegion
			} else {
				log.L().Warn("retry ingest due to",
					zap.Reflect("meta", meta), zap.Reflect("region", region),
					zap.Reflect("new region", newRegion), zap.Error(errIngest))
				return errIngest
			}
		}
	}
	return nil
}

func (local *local) WriteAndIngestByRanges(ctx context.Context, engineFile *localFile, ranges []Range) error {
	if engineFile.length == 0 {
		log.L().Error("the ranges is empty")
		return nil
	}
	log.L().Debug("the ranges length write to tikv", zap.Int("length", len(ranges)))

	var wg sync.WaitGroup
	errCh := make(chan error, len(ranges))
	finishCh := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, r := range ranges {
		db := engineFile.db
		startKey := r.start
		endKey := r.end
		w := local.rangeConcurrency.Apply()
		wg.Add(1)
		go func(w *worker.Worker) {
			defer func() {
				wg.Done()
				local.rangeConcurrency.Recycle(w)
			}()
			var err error
			for i := 0; i < maxRetryTimes; i++ {
				if err = local.writeAndIngestByRange(ctx, db, startKey, endKey, engineFile.ts, engineFile.uuid); err != nil {
					log.L().Warn("write and ingest by range failed",
						zap.Int("retry time", i+1), zap.Error(err))
				} else {
					return
				}
			}
			if err != nil {
				errCh <- err
			}
		}(w)
	}
	go func() {
		wg.Wait()
		close(finishCh)
	}()

	for {
		select {
		case _, ok := <-finishCh:
			if !ok {
				// Finished
				return nil
			}
		case err := <-errCh:
			return err
		}
	}
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engineFile, ok := local.engines.Load(engineUUID)
	if !ok {
		return errors.Errorf("could not find engine %s in ImportEngine", engineUUID.String())
	}
	// split sorted file into range by 96MB size per file
	ranges, err := local.ReadAndSplitIntoRange(engineFile.(*localFile), engineUUID)
	if err != nil {
		return err
	}
	// split region by given ranges
	err = local.SplitAndScatterRegionByRanges(ctx, ranges)
	if err != nil {
		log.L().Error("split & scatter ranges failed", zap.Error(err))
		return err
	}
	// start to write to kv and ingest
	err = local.WriteAndIngestByRanges(ctx, engineFile.(*localFile), ranges)
	if err != nil {
		log.L().Error("write and ingest ranges failed", zap.Error(err))
		return err
	}
	log.L().Info("import engine success", zap.Stringer("uuid", engineUUID))
	return nil
}

func (local *local) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// release this engine after import success
	engineFile, ok := local.engines.Load(engineUUID)
	if ok {
		engineFile.(*localFile).Close()
		local.engines.Delete(engineUUID)
	} else {
		log.L().Error("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
	}
	return nil
}

func (local *local) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	columnNames []string,
	ts uint64,
	rows Rows,
) (finalErr error) {
	kvs := rows.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	fmt.Printf("table %s engine %s WriteRows %d\n", tableName, engineUUID, len(kvs))

	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engineFile := e.(*localFile)

	// write to go leveldb get get sorted kv
	wb := engineFile.db.NewBatch()
	defer wb.Close()
	wo := &pebble.WriteOptions{Sync: false}

	size := int64(0)
	for _, pair := range kvs {
		wb.Set(pair.Key, pair.Val, wo)
		size += int64(len(pair.Key) + len(pair.Val))
	}
	err := wb.Commit(wo)
	if err != nil {
		return err
	}
	engineFile.length += int64(len(kvs))
	engineFile.totalSize += size
	engineFile.ts = ts
	local.engines.Store(engineUUID, engineFile)
	return
}

func (local *local) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (local *local) NewEncoder(tbl table.Table, options *SessionOptions) Encoder {
	return NewTableKVEncoder(tbl, options)
}

func isIngestRetryable(resp *sst.IngestResponse, region *split.RegionInfo, meta *sst.SSTMeta) (bool, *split.RegionInfo, error) {
	if resp.GetError() == nil {
		return false, nil, nil
	}

	var newRegion *split.RegionInfo
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
			newRegion = &split.RegionInfo{
				Leader: newLeader,
				Region: region.Region,
			}
			return true, newRegion, errors.Errorf("not leader: %s", errPb.GetMessage())
		}
	case errPb.EpochNotMatch != nil:
		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, meta) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &split.RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		return true, newRegion, errors.Errorf("epoch not match: %s", errPb.GetMessage())
	}
	return false, nil, errors.Errorf("non retryable error: %s", resp.GetError().GetMessage())
}

func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}
	res := make([]byte, 0, len(key)+1)
	pos := 0
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] != '\xff' {
			pos = i
			break
		}
	}
	s, e := key[:pos], key[pos]+1
	res = append(append(res, s...), e)
	return res
}

// splitValuesToRange try to cut [start, end] to count range approximately
// just like [start, v1], [v1, v2]... [vCount, end]
// return value []{v1, v2... vCount}
func splitValuesToRange(start []byte, end []byte, count int64) [][]byte {
	if bytes.Compare(start, end) == 0 {
		log.L().Info("couldn't split range due to start end are same",
			zap.Binary("start", start),
			zap.Binary("end", end),
			zap.Int64("count", count))
		return [][]byte{end}
	}

	startBytes := make([]byte, 8)
	endBytes := make([]byte, 8)

	minLen := len(start)
	if minLen > len(end) {
		minLen = len(end)
	}

	offset := 0
	for i := 0; i < minLen; i++ {
		if start[i] != end[i] {
			offset = i
			break
		}
	}

	copy(startBytes, start[offset:])
	copy(endBytes, end[offset:])

	sValue := binary.BigEndian.Uint64(startBytes)
	eValue := binary.BigEndian.Uint64(endBytes)

	step := (eValue - sValue) / uint64(count)
	if step == uint64(0) {
		step = uint64(1)
	}

	res := make([][]byte, 0, count)
	for cur := sValue + step; cur <= eValue-step; cur += step {
		curBytes := make([]byte, offset+8)
		copy(curBytes, start[:offset])
		binary.BigEndian.PutUint64(curBytes[offset:], cur)
		res = append(res, curBytes)
	}
	res = append(res, end)

	return res
}

// find last key that is less than target key
func binarySearch(pairs []*sst.Pair, targetKey []byte) int {
	if len(targetKey) == 0 {
		// target is max, so return max index pair
		return len(pairs) - 1
	}
	low := 0
	high := len(pairs) - 1
	for low <= high {
		mid := low + (high-low)/2
		midValue := pairs[mid]
		compare := bytes.Compare(codec.EncodeBytes([]byte{}, midValue.Key), targetKey)
		if compare < 0 {
			if mid == len(pairs)-1 || bytes.Compare(codec.EncodeBytes([]byte{}, pairs[mid+1].Key), targetKey) > 0 {
				return mid
			}
			low = mid + 1
		} else if compare == 0 {
			return mid - 1
		} else {
			high = mid - 1
		}
	}
	return -1
}
