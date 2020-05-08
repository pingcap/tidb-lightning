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
	"context"
	"crypto/tls"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	dbopt "github.com/syndtr/goleveldb/leveldb/opt"
	dbutil "github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	split "github.com/pingcap/br/pkg/restore"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	dialTimeout = 5 * time.Second
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
	db        *leveldb.DB
	meta      sst.SSTMeta
	length    int64
	totalSize int64

	ranges   []Range
	startKey []byte
	endKey   []byte
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

	pairPool sync.Pool
}

// NewLocal creates new connections to tikv.
func NewLocalBackend(ctx context.Context, tls *common.TLS, pdAddr string, regionSplitSize int64, localFile string) (Backend, error) {
	pdCli, err := pd.NewClient([]string{pdAddr}, tls.ToPDSecurityOption())
	if err != nil {
		return MakeBackend(nil), errors.Annotate(err, "construct pd client failed")
	}
	tlsConf := tls.TransToTlsConfig()
	if err != nil {
		return MakeBackend(nil), err
	}
	splitCli := split.NewSplitClient(pdCli, tlsConf)

	local := &local{
		engines:  sync.Map{},
		splitCli: splitCli,
		tlsConf:  tlsConf,

		localFile:       localFile,
		regionSplitSize: regionSplitSize,

		pairPool: sync.Pool{New: func() interface{} { return &sst.Pair{} }},
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
	db, err := leveldb.OpenFile(dbPath, &dbopt.Options{
		OpenFilesCacheCapacity: 1024,
		BlockCacheCapacity: 768 / 2 * dbopt.MiB,
		WriteBuffer:        768 / 4 * dbopt.MiB,
		CompactionL0Trigger: 40,
		CompactionTotalSizeMultiplier: 15,
		WriteL0PauseTrigger: 120,
		WriteL0SlowdownTrigger: 80,
	})
	if err != nil {
		return err
	}
	local.engines.Store(engineUUID, &localFile{db: db, length: 0, ranges: make([]Range, 0)})
	return nil
}

func (local *local) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// Do nothing since we will do prepare jobs in importEngine, just like tikv-importer
	return nil
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

	req.Reset()
	req.Chunk = &sst.WriteRequest_Batch{
		Batch: &sst.WriteBatch{
			CommitTs:  ts,
			Pairs: pairs,
		},
	}
	err = wstream.Send(req)
	if err != nil {
		return
	}

	if resp, closeErr := wstream.CloseAndRecv(); closeErr != nil {
		if err == nil {
			err = closeErr
		}
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

func (local *local) ReadAndSplitIntoRange(engineFile *localFile) ([]Range, error) {
	if engineFile.length == 0 {
		return nil, nil
	}
	ranges := make([]Range, 0)
	iter := engineFile.db.NewIterator(nil, nil)
	size := int64(0)
	length := 0
	var k, v []byte
	var startKey, endKey []byte
	first := true
	for iter.Next() {
		k = iter.Key()
		v = iter.Value()
		length++
		if first {
			first = false
			startKey = append([]byte{}, k...)
		}
		size += int64(len(k) + len(v))
		if size > local.regionSplitSize {
			endKey = append([]byte{}, k...)
			ranges = append(ranges, Range{start: startKey, end: endKey, length: length})
			first = true
			size = 0
			length = 0
		}
	}
	iter.Release()
	if size > 0 {
		ranges = append(ranges, Range{start: startKey, end: k, length: length})
	}
	return ranges, nil
}

func (local *local) writeAndIngestByRange(
	ctx context.Context,
	iter iterator.Iterator,
	ts uint64,
	length int) error {

	defer iter.Release()
	index := 0
	pairs := make([]*sst.Pair, length)

	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		//pairs[index] = local.pairPool.Get().(*sst.Pair)
		//pairs[index].Key = append(pairs[index].Key, k...)
		//pairs[index].Value = append(pairs[index].Value, v...)
		pairs[index] = &sst.Pair{
			Key: append([]byte{}, k...),
			Value: append([]byte{}, v...),
		}
		index += 1
	}
	if index == 0 {
		return nil
	}

	startKey := pairs[0].Key
	endKey := pairs[index-1].Key
	region, err := local.splitCli.GetRegion(ctx, startKey)
	if err != nil {
		log.L().Error("get region in write failed", zap.Error(err))
		return err
	}

	log.L().Debug("get region",
		zap.Uint64("id", region.Region.GetId()),
		zap.Stringer("epoch", region.Region.GetRegionEpoch()),
		zap.Binary("start", region.Region.GetStartKey()),
		zap.Binary("end", region.Region.GetEndKey()),
		zap.Reflect("peers", region.Region.GetPeers()),
	)

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
	// TODO split mutation to batch
	metas, err := local.WriteToTiKV(ctx, meta, ts, region, pairs)
	if err != nil {
		log.L().Error("write to tikv failed", zap.Error(err))
		return err
	}

	//for _, pair := range pairs {
	//	pair.Value = pair.Value[:0]
	//	pair.Key = pair.Key[:0]
	//	local.pairPool.Put(pair)
	//}

	var resp *sst.IngestResponse
	for _, meta := range metas {
		for i := 0; i < maxRetryTimes; i++ {
			resp, err = local.Ingest(ctx, meta, region)
			if err != nil {
				log.L().Error("ingest to tikv failed", zap.Error(err))
				return err
			}
			needRetry, newRegion, errIngest := isIngestRetryable(resp, region, meta)
			if !needRetry {
				return errIngest
			}
			log.L().Warn("retry ingest due to",
				zap.Reflect("meta", meta),
				zap.Int("retry time", i),
				zap.Reflect("region", region),
				zap.Reflect("new region", newRegion),
				zap.Error(errIngest),
			)

			// retry with not leader and epoch not match error
			region = newRegion
		}
	}
	return err
}

func (local *local) WriteAndIngestByRanges(ctx context.Context, engineFile *localFile, ranges []Range) error {
	if engineFile.length == 0 {
		return nil
	}
	var eg errgroup.Group
	for _, r := range ranges {
		log.L().Debug("deliver range",
			zap.Binary("start", r.start),
			zap.Binary("end", r.end),
			zap.Int("len", r.length))
		iter := engineFile.db.NewIterator(&dbutil.Range{Start: r.start, Limit: nextKey(r.end)}, nil)
		length := r.length
		eg.Go(func() error {
			var err error
			for i := 0; i < maxRetryTimes; i++ {
				if err = local.writeAndIngestByRange(ctx, iter, engineFile.ts, length); err != nil {
					log.L().Warn("write and ingest by range failed",
						zap.Int("retry time", i+1),
						zap.Error(err))
				} else {
					return nil
				}
			}
			log.L().Error("write and ingest by range retry exceed maxRetryTimes:3")
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engineFile, ok := local.engines.Load(engineUUID)
	if !ok {
		return errors.Errorf("could not find engine %s in ImportEngine", engineUUID.String())
	}
	// split sorted file into range by 96MB size per file
	ranges, err := local.ReadAndSplitIntoRange(engineFile.(*localFile))
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

	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engineFile := e.(*localFile)

	// write to go leveldb get get sorted kv
	batch := new(leveldb.Batch)
	size := int64(0)
	for _, pair := range kvs {
		batch.Put(pair.Key, pair.Val)
		size += int64(len(pair.Key) + len(pair.Val))
	}
	engineFile.length += int64(batch.Len())
	engineFile.totalSize += size
	err := engineFile.db.Write(batch, &dbopt.WriteOptions{NoWriteMerge:true})
	if err != nil {
		return err
	}
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
