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
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coreos/go-semver/semver"
	split "github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	uuid "github.com/satori/go.uuid"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/manual"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

const (
	dialTimeout          = 5 * time.Second
	bigValueSize         = 1 << 16 // 64K
	engineMetaFileSuffix = ".meta"

	gRPCKeepAliveTime    = 10 * time.Second
	gRPCKeepAliveTimeout = 3 * time.Second
	gRPCBackOffMaxDelay  = 3 * time.Second

	LocalMemoryTableSize = 512 << 20

	// See: https://github.com/tikv/tikv/blob/e030a0aae9622f3774df89c62f21b2171a72a69e/etc/config-template.toml#L360
	regionMaxKeyCount = 1_440_000
)

var (
	localMinTiDBVersion = *semver.New("4.0.0")
	localMinTiKVVersion = *semver.New("4.0.0")
	localMinPDVersion   = *semver.New("4.0.0")
)

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	start  []byte
	end    []byte
	length int
}

// localFileMeta contains some field that is necessary to continue the engine restore/import process.
// These field should be written to disk when we update chunk checkpoint
type localFileMeta struct {
	Ts        uint64 `json:"ts"`
	Length    int64  `json:"length"`
	TotalSize int64  `json:"total_size"`
}

type LocalFile struct {
	localFileMeta
	db   *pebble.DB
	Uuid uuid.UUID
}

func (e *LocalFile) Close() error {
	return e.db.Close()
}

// Cleanup remove meta and db files
func (e *LocalFile) Cleanup(dataDir string) error {
	metaPath := filepath.Join(dataDir, e.Uuid.String()+engineMetaFileSuffix)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}

	dbPath := filepath.Join(dataDir, e.Uuid.String())
	return os.RemoveAll(dbPath)
}

type grpcClis struct {
	mu   sync.Mutex
	clis map[uint64]*grpc.ClientConn
}

type local struct {
	engines  sync.Map
	grpcClis grpcClis
	splitCli split.SplitClient
	tls      *common.TLS
	pdAddr   string

	localStoreDir   string
	regionSplitSize int64

	rangeConcurrency  *worker.Pool
	ingestConcurrency *worker.Pool
	batchWriteKVPairs int
	checkpointEnabled bool
}

// NewLocalBackend creates new connections to tikv.
func NewLocalBackend(
	ctx context.Context,
	tls *common.TLS,
	pdAddr string,
	regionSplitSize int64,
	localFile string,
	rangeConcurrency int,
	sendKVPairs int,
	enableCheckpoint bool,
) (Backend, error) {
	pdCli, err := pd.NewClient([]string{pdAddr}, tls.ToPDSecurityOption())
	if err != nil {
		return MakeBackend(nil), errors.Annotate(err, "construct pd client failed")
	}
	splitCli := split.NewSplitClient(pdCli, tls.TLSConfig())

	shouldCreate := true
	if enableCheckpoint {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return MakeBackend(nil), err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err = os.Mkdir(localFile, 0700)
		if err != nil {
			return MakeBackend(nil), err
		}
	}

	local := &local{
		engines:  sync.Map{},
		splitCli: splitCli,
		tls:      tls,
		pdAddr:   pdAddr,

		localStoreDir:   localFile,
		regionSplitSize: regionSplitSize,

		rangeConcurrency:  worker.NewPool(ctx, rangeConcurrency, "range"),
		ingestConcurrency: worker.NewPool(ctx, rangeConcurrency*2, "ingest"),
		batchWriteKVPairs: sendKVPairs,
		checkpointEnabled: enableCheckpoint,
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
	if local.tls.TLSConfig() != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(local.tls.TLSConfig()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
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
		v.(*LocalFile).Close()
		return true
	})

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !local.checkpointEnabled || common.IsEmptyDir(local.localStoreDir) {
		err := os.RemoveAll(local.localStoreDir)
		if err != nil {
			log.L().Warn("remove local db file failed", zap.Error(err))
		}
	}
}

// Flush ensure the written data is saved successfully, to make sure no data lose after restart
func (local *local) Flush(engineId uuid.UUID) error {
	if engine, ok := local.engines.Load(engineId); ok {
		engineFile := engine.(*LocalFile)
		if err := engineFile.db.Flush(); err != nil {
			return err
		}
		return local.saveEngineMeta(engineFile)
	}
	return errors.Errorf("engine '%s' not found", engineId)
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

func (local *local) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize:             LocalMemoryTableSize,
		MaxConcurrentCompactions: 16,
		MinCompactionRate:        1 << 30,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		MaxOpenFiles:             10000,
		DisableWAL:               true,
		ReadOnly:                 readOnly,
	}
	dbPath := filepath.Join(local.localStoreDir, engineUUID.String())
	return pebble.Open(dbPath, opt)
}

func (local *local) saveEngineMeta(engine *LocalFile) error {
	jsonBytes, err := json.Marshal(&engine.localFileMeta)
	if err != nil {
		return errors.Trace(err)
	}
	metaPath := filepath.Join(local.localStoreDir, engine.Uuid.String()+engineMetaFileSuffix)
	return errors.Trace(ioutil.WriteFile(metaPath, jsonBytes, 0644))
}

func (local *local) LoadEngineMeta(engineUUID uuid.UUID) (localFileMeta, error) {
	var meta localFileMeta

	mataPath := filepath.Join(local.localStoreDir, engineUUID.String()+engineMetaFileSuffix)
	f, err := os.Open(mataPath)
	if err != nil {
		return meta, err
	}
	err = json.NewDecoder(f).Decode(&meta)
	return meta, err
}

func (local *local) OpenEngine(ctx context.Context, engineUUID uuid.UUID) error {
	meta, err := local.LoadEngineMeta(engineUUID)
	if err != nil {
		meta = localFileMeta{}
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}
	local.engines.Store(engineUUID, &LocalFile{localFileMeta: meta, db: db, Uuid: engineUUID})
	return nil
}

// Close backend engine by uuid
// NOTE: we will return nil if engine is not exist. This will happen if engine import&cleanup successfully
// but exit before update checkpoint. Thus after restart, we will try to import this engine again.
func (local *local) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engine, ok := local.engines.Load(engineUUID)
	if !ok {
		// recovery mode, we should reopen this engine file
		meta, err := local.LoadEngineMeta(engineUUID)
		if err != nil {
			// if engine meta not exist, just skip
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		db, err := local.openEngineDB(engineUUID, true)
		if err != nil {
			return err
		}
		engineFile := &LocalFile{
			localFileMeta: meta,
			Uuid:          engineUUID,
			db:            db,
		}
		local.engines.Store(engineUUID, engineFile)
		return nil
	}
	engineFile := engine.(*LocalFile)
	err := engineFile.db.Flush()
	if err != nil {
		return err
	}
	return local.saveEngineMeta(engineFile)
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

// WriteToTiKV writer engine key-value pairs to tikv and return the sst meta generated by tikv.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will takes the responsibility to do so.
func (local *local) WriteToTiKV(
	ctx context.Context,
	engineFile *LocalFile,
	region *split.RegionInfo,
	start, end []byte,
) ([]*sst.SSTMeta, *Range, error) {
	var startKey, endKey []byte
	if len(region.Region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.Region.StartKey, []byte{})
	}
	if bytes.Compare(startKey, start) < 0 {
		startKey = start
	}
	if len(region.Region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.Region.EndKey, []byte{})
	}
	if beforeEnd(end, endKey) {
		endKey = end
	}
	opt := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	iter := engineFile.db.NewIter(opt)
	defer iter.Close()

	if !iter.First() {
		log.L().Warn("keys within region is empty, skip ingest")
		return nil, nil, nil
	}

	firstKey := codec.EncodeBytes([]byte{}, iter.Key())
	iter.Last()
	lastKey := codec.EncodeBytes([]byte{}, iter.Key())

	meta := &sst.SSTMeta{
		Uuid:        uuid.NewV4().Bytes(),
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
	}

	leaderID := region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.Region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.Region.GetPeers()))
	for _, peer := range region.Region.GetPeers() {
		cli, err := local.getImportClient(ctx, peer)
		if err != nil {
			return nil, nil, err
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, nil, err
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return nil, nil, err
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: engineFile.Ts,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
	}

	bytesBuf := newBytesBuffer()
	defer bytesBuf.destroy()
	pairs := make([]*sst.Pair, 0, local.batchWriteKVPairs)
	count := 0
	size := int64(0)
	totalCount := 0
	firstLoop := true
	regionMaxSize := local.regionSplitSize * 4 / 3

	for iter.First(); iter.Valid(); iter.Next() {
		size += int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if firstLoop {
			pair := &sst.Pair{
				Key:   bytesBuf.addBytes(iter.Key()),
				Value: bytesBuf.addBytes(iter.Value()),
			}
			pairs = append(pairs, pair)
		} else {
			pairs[count].Key = bytesBuf.addBytes(iter.Key())
			pairs[count].Value = bytesBuf.addBytes(iter.Value())
		}
		count++
		totalCount++

		if count >= local.batchWriteKVPairs || size >= regionMaxSize || totalCount >= regionMaxKeyCount {
			for i := range clients {
				requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
				if err := clients[i].Send(requests[i]); err != nil {
					return nil, nil, err
				}
			}
			count = 0
			bytesBuf.reset()
			firstLoop = false
		}
		if size >= regionMaxSize || totalCount >= regionMaxKeyCount {
			break
		}
	}

	if count > 0 {
		for i := range clients {
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return nil, nil, err
			}
		}
	}

	if iter.Error() != nil {
		return nil, nil, errors.Trace(iter.Error())
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		if resp, closeErr := wStream.CloseAndRecv(); closeErr != nil {
			return nil, nil, closeErr
		} else {
			if leaderID == region.Region.Peers[i].GetId() {
				leaderPeerMetas = resp.Metas
				log.L().Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
			}
		}
	}

	// if there is not leader currently, we should directly return an error
	if leaderPeerMetas == nil {
		log.L().Error("write to tikv no leader", zap.Reflect("region", region),
			zap.Uint64("leader_id", leaderID), zap.Reflect("meta", meta),
			zap.Int("kv_pairs", totalCount), zap.Int64("total_bytes", size))
		return nil, nil, errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			region.Region.Id, leaderID)
	}

	log.L().Debug("write to kv", zap.Reflect("region", region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.totalSize()))

	var remainRange *Range
	if iter.Valid() && iter.Next() {
		firstKey := append([]byte{}, iter.Key()...)
		remainRange = &Range{start: firstKey, end: endKey}
		log.L().Info("write to tikv partial finish", zap.Int("count", totalCount),
			zap.Int64("size", size), zap.Binary("startKey", startKey), zap.Binary("endKey", endKey),
			zap.Binary("remainStart", remainRange.start), zap.Binary("remainEnd", remainRange.end),
			zap.Reflect("region", region))
	}

	return leaderPeerMetas, remainRange, nil
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

func (local *local) readAndSplitIntoRange(engineFile *LocalFile, engineUUID uuid.UUID) ([]Range, error) {
	if engineFile.Length == 0 {
		return nil, nil
	}

	iter := engineFile.db.NewIter(nil)
	defer iter.Close()

	var firstKey, lastKey []byte
	if iter.First() {
		firstKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, errors.New("could not find first pair, this shouldn't happen")
	}
	if iter.Last() {
		lastKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, errors.New("could not find last pair, this shouldn't happen")
	}
	endKey := nextKey(lastKey)

	// <= 96MB no need to split into range
	if engineFile.TotalSize <= local.regionSplitSize {
		ranges := []Range{{start: firstKey, end: endKey, length: int(engineFile.Length)}}
		return ranges, nil
	}

	log.L().Info("doReadAndSplitIntoRange", zap.Binary("firstKey", firstKey), zap.Binary("lastKey", lastKey))

	// split data into n * 4/3 ranges, then seek n times to get n + 1 ranges
	// because we don't split very accurate, so wo try to split 1/4 more regions to avoid region to be too big
	// estimiate regions size by the bigger of region size in bytes and kv count
	splitTargetSize := (local.regionSplitSize*3 + 3) / 4
	n := (engineFile.TotalSize + splitTargetSize - 1) / splitTargetSize
	numByKeyCount := engineFile.Length / (regionMaxKeyCount * 3 / 4)
	if n < numByKeyCount {
		n = numByKeyCount
	}

	ranges := make([]Range, 0, n+1)
	appendRanges := func(ranges []Range, start []byte, ends [][]byte) []Range {
		for _, e := range ends {
			ranges = append(ranges, Range{start: start, end: e})
			start = e
		}
		return ranges
	}
	if tablecodec.IsIndexKey(firstKey) {
		type tblIndexRange struct {
			tblID    int64
			indexID  int64
			startKey []byte
			endKey   []byte
		}

		// for partitioned table, there will be multiple physical tables and each physical table contains multiple indices
		indexRanges := make([]*tblIndexRange, 0)
		iter.First()
		for iter.Valid() {
			startKey := append([]byte{}, iter.Key()...)

			tableID, indexID, _, err := tablecodec.DecodeKeyHead(startKey)
			if err != nil {
				return nil, err
			}

			k := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
			iter.SeekLT(k)

			endKey := append([]byte{}, iter.Key()...)
			indexRanges = append(indexRanges, &tblIndexRange{tableID, indexID, startKey, endKey})
			log.L().Debug("index key range", zap.Int64("tableID", tableID), zap.Int64("index", indexID),
				zap.Binary("startKey", startKey), zap.Binary("endKey", endKey))

			iter.Next()
		}

		indexRangeCount := (int(n) + len(indexRanges)) / len(indexRanges)

		log.L().Info("split table index kv to range",
			zap.Int("total index count", len(indexRanges)), zap.Int64("ranges", n),
			zap.Int("index range count", indexRangeCount))

		for _, indexRange := range indexRanges {
			values := engineFile.splitValuesToRange(indexRange.startKey, nextKey(indexRange.endKey), int64(indexRangeCount), len(indexRanges))
			ranges = appendRanges(ranges, indexRange.startKey, values)
		}
	} else {
		// data engine, we split keys by sample keys instead of by handle
		// because handles are also not distributed evenly
		values := engineFile.splitValuesToRange(firstKey, endKey, n, 1)
		ranges = appendRanges(ranges, firstKey, values)
	}
	return ranges, nil
}

type bytesRecycleChan struct {
	ch chan []byte
}

// recycleChan is used for reusing allocated []byte so we can use memory more efficiently
//
// NOTE: we don't used a `sync.Pool` because when will sync.Pool release is depending on the
// garbage collector which always release the memory so late. Use a fixed size chan to reuse
// can decrease the memory usage to 1/3 compare with sync.Pool.
var recycleChan *bytesRecycleChan

func init() {
	recycleChan = &bytesRecycleChan{
		ch: make(chan []byte, 1024),
	}
}

func (c *bytesRecycleChan) Acquire() []byte {
	select {
	case b := <-c.ch:
		return b
	default:
		return manual.New(1 << 20) // 1M
	}
}

func (c *bytesRecycleChan) Release(w []byte) {
	select {
	case c.ch <- w:
		return
	default:
		manual.Free(w)
	}
}

type bytesBuffer struct {
	bufs      [][]byte
	curBuf    []byte
	curIdx    int
	curBufIdx int
	curBufLen int
}

func newBytesBuffer() *bytesBuffer {
	return &bytesBuffer{bufs: make([][]byte, 0, 128), curBufIdx: -1}
}

func (b *bytesBuffer) addBuf() {
	if b.curBufIdx < len(b.bufs)-1 {
		b.curBufIdx += 1
		b.curBuf = b.bufs[b.curBufIdx]
	} else {
		buf := recycleChan.Acquire()
		b.bufs = append(b.bufs, buf)
		b.curBuf = buf
		b.curBufIdx = len(b.bufs) - 1
	}

	b.curBufLen = len(b.curBuf)
	b.curIdx = 0
}

func (b *bytesBuffer) reset() {
	if len(b.bufs) > 0 {
		b.curBuf = b.bufs[0]
		b.curBufLen = len(b.bufs[0])
		b.curBufIdx = 0
		b.curIdx = 0
	}
}

func (b *bytesBuffer) destroy() {
	for _, buf := range b.bufs {
		recycleChan.Release(buf)
	}
	b.bufs = b.bufs[:0]
}

func (b *bytesBuffer) totalSize() int64 {
	return int64(len(b.bufs)) * int64(1<<20)
}

func (b *bytesBuffer) addBytes(bytes []byte) []byte {
	if len(bytes) > bigValueSize {
		return append([]byte{}, bytes...)
	}

	if b.curIdx+len(bytes) > b.curBufLen {
		b.addBuf()
	}
	idx := b.curIdx
	copy(b.curBuf[idx:], bytes)
	b.curIdx += len(bytes)
	return b.curBuf[idx:b.curIdx]
}

func (local *local) writeAndIngestByRange(
	ctxt context.Context,
	engineFile *LocalFile,
	start, end []byte,
	remainRanges *syncdRanges,
) error {
	ito := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	iter := engineFile.db.NewIter(ito)
	defer iter.Close()
	// Needs seek to first because NewIter returns an iterator that is unpositioned
	hasKey := iter.First()
	if !hasKey {
		log.L().Info("There is no pairs in iterator",
			zap.Binary("start", start),
			zap.Binary("end", end),
			zap.Binary("next end", nextKey(end)))
		return nil
	}
	pairStart := append([]byte{}, iter.Key()...)
	iter.Last()
	pairEnd := append([]byte{}, iter.Key()...)

	var regions []*split.RegionInfo
	var err error
	ctx, cancel := context.WithCancel(ctxt)
	defer cancel()

WriteAndIngest:
	for retry := 0; retry < maxRetryTimes; retry++ {
		if retry != 0 {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		startKey := codec.EncodeBytes([]byte{}, pairStart)
		endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
		regions, err = paginateScanRegion(ctx, local.splitCli, startKey, endKey, 128)
		if err != nil || len(regions) == 0 {
			log.L().Warn("scan region failed", zap.Error(err), zap.Int("region_len", len(regions)))
			continue WriteAndIngest
		}

		shouldWait := false
		errChan := make(chan error, len(regions))
		for _, region := range regions {
			log.L().Debug("get region", zap.Int("retry", retry), zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey), zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()), zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()), zap.Reflect("peers", region.Region.GetPeers()))

			// generate new uuid for concurrent write to tikv

			if len(regions) == 1 {
				w := local.ingestConcurrency.Apply()
				rg, err1 := local.WriteAndIngestPairs(ctx, engineFile, region, start, end)
				local.ingestConcurrency.Recycle(w)
				if err1 != nil {
					err = err1
					continue WriteAndIngest
				}
				if rg != nil {
					remainRanges.add(*rg)
				}
			} else {
				shouldWait = true
				go func(r *split.RegionInfo) {
					w := local.ingestConcurrency.Apply()
					rg, err := local.WriteAndIngestPairs(ctx, engineFile, r, start, end)
					local.ingestConcurrency.Recycle(w)
					errChan <- err
					if err == nil && rg != nil {
						remainRanges.add(*rg)
					}
				}(region)
			}
		}
		if shouldWait {
			shouldRetry := false
			for i := 0; i < len(regions); i++ {
				err1 := <-errChan
				if err1 != nil {
					err = err1
					log.L().Warn("should retry this range", zap.Int("retry", retry), zap.Error(err))
					shouldRetry = true
				}
			}
			if !shouldRetry {
				return nil
			} else {
				continue WriteAndIngest
			}
		}
		return nil
	}
	if err == nil {
		err = errors.New("all retry failed")
	}
	return err
}

func (local *local) WriteAndIngestPairs(
	ctx context.Context,
	engineFile *LocalFile,
	region *split.RegionInfo,
	start, end []byte,
) (*Range, error) {
	metas, remainRange, err := local.WriteToTiKV(ctx, engineFile, region, start, end)
	if err != nil {
		log.L().Warn("write to tikv failed", zap.Error(err))
		return remainRange, err
	}

	for _, meta := range metas {
		var err error
		for i := 0; i < maxRetryTimes; i++ {
			log.L().Debug("ingest meta", zap.Reflect("meta", meta))
			var resp *sst.IngestResponse
			resp, err = local.Ingest(ctx, meta, region)
			if err != nil {
				log.L().Warn("ingest failed", zap.Error(err), zap.Reflect("meta", meta),
					zap.Reflect("region", region))
				continue
			}
			failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
				switch val.(string) {
				case "notleader":
					resp.Error.NotLeader = &errorpb.NotLeader{
						RegionId: region.Region.Id, Leader: region.Leader}
				case "epochnotmatch":
					resp.Error.EpochNotMatch = &errorpb.EpochNotMatch{
						CurrentRegions: []*metapb.Region{region.Region}}
				}
			})
			var needRetry bool
			var newRegion *split.RegionInfo
			needRetry, newRegion, err = isIngestRetryable(resp, region, meta)
			if err == nil {
				// ingest next meta
				break
			}
			if !needRetry {
				log.L().Warn("ingest failed noretry", zap.Error(err), zap.Reflect("meta", meta),
					zap.Reflect("region", region))
				// met non-retryable error retry whole Write procedure
				return remainRange, err
			}
			// retry with not leader and epoch not match error
			if newRegion != nil {
				region = newRegion
			} else {
				log.L().Warn("retry ingest due to",
					zap.Reflect("meta", meta), zap.Reflect("region", region),
					zap.Reflect("new region", newRegion), zap.Error(err))
				return remainRange, err
			}
		}
		if err != nil {
			log.L().Error("all retry ingest failed", zap.Reflect("ingest meta", meta), zap.Error(err))
			return remainRange, errors.Trace(err)
		}
	}
	return remainRange, nil
}

func (local *local) WriteAndIngestByRanges(ctx context.Context, engineFile *LocalFile, ranges []Range, remainRanges *syncdRanges) error {
	if engineFile.Length == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.L().Warn("engine contains no data", zap.Stringer("uuid", engineFile.Uuid))
		return nil
	}
	log.L().Debug("the ranges Length write to tikv", zap.Int("Length", len(ranges)))

	errCh := make(chan error, len(ranges))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, r := range ranges {
		startKey := r.start
		endKey := r.end
		w := local.rangeConcurrency.Apply()
		go func(w *worker.Worker) {
			defer local.rangeConcurrency.Recycle(w)
			var err error
			for i := 0; i < maxRetryTimes; i++ {
				if err = local.writeAndIngestByRange(ctx, engineFile, startKey, endKey, remainRanges); err != nil {
					log.L().Warn("write and ingest by range failed",
						zap.Int("retry time", i+1), zap.Error(err))
				} else {
					break
				}
			}
			errCh <- err
		}(w)
	}

	for i := 0; i < len(ranges); i++ {
		e := <-errCh
		if e != nil {
			return e
		}
	}
	return nil
}

type syncdRanges struct {
	sync.Mutex
	ranges []Range
}

func (r *syncdRanges) add(g Range) {
	r.Lock()
	r.ranges = append(r.ranges, g)
	r.Unlock()
}

func (r *syncdRanges) take() []Range {
	r.Lock()
	rg := r.ranges
	r.ranges = []Range{}
	r.Unlock()
	if len(rg) > 0 {
		sort.Slice(rg, func(i, j int) bool {
			return bytes.Compare(rg[i].start, rg[j].start) < 0
		})
	}
	return rg
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engineFile, ok := local.engines.Load(engineUUID)
	if !ok {
		// skip if engine not exist. See the comment of `CloseEngine` for more detail.
		return nil
	}
	// split sorted file into range by 96MB size per file
	ranges, err := local.readAndSplitIntoRange(engineFile.(*LocalFile), engineUUID)
	if err != nil {
		return err
	}
	remains := &syncdRanges{}

	for {
		// split region by given ranges
		err = local.SplitAndScatterRegionByRanges(ctx, ranges)
		if err != nil {
			log.L().Error("split & scatter ranges failed", zap.Error(err))
			return err
		}
		// start to write to kv and ingest
		err = local.WriteAndIngestByRanges(ctx, engineFile.(*LocalFile), ranges, remains)
		if err != nil {
			log.L().Error("write and ingest ranges failed", zap.Error(err))
			return err
		}

		unfinishedRanges := remains.take()
		if len(unfinishedRanges) == 0 {
			break
		}
		log.L().Debug("ingest ranges unfinished", zap.Int("remain ranges", len(unfinishedRanges)))
		ranges = unfinishedRanges
	}

	log.L().Info("import engine success", zap.Stringer("uuid", engineUUID))
	return nil
}

func (local *local) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// release this engine after import success
	engineFile, ok := local.engines.Load(engineUUID)
	if ok {
		localEngine := engineFile.(*LocalFile)
		err := localEngine.Close()
		if err != nil {
			return err
		}
		err = localEngine.Cleanup(local.localStoreDir)
		if err != nil {
			return err
		}
		local.engines.Delete(engineUUID)
	} else {
		log.L().Error("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
	}
	return nil
}

func (local *local) CheckRequirements() error {
	if err := checkTiDBVersion(local.tls, localMinTiDBVersion); err != nil {
		return err
	}
	if err := checkPDVersion(local.tls, local.pdAddr, localMinPDVersion); err != nil {
		return err
	}
	if err := checkTiKVVersion(local.tls, local.pdAddr, localMinTiKVVersion); err != nil {
		return err
	}
	return nil
}

func (local *local) FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error) {
	return fetchRemoteTableModelsFromTLS(local.tls, schemaName)
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
	engineFile := e.(*LocalFile)

	// write to pebble to make them sorted
	wb := engineFile.db.NewBatch()
	defer wb.Close()
	wo := &pebble.WriteOptions{Sync: false}

	size := int64(0)
	for _, pair := range kvs {
		wb.Set(pair.Key, pair.Val, wo)
		size += int64(len(pair.Key) + len(pair.Val))
	}
	if err := wb.Commit(wo); err != nil {
		return err
	}
	atomic.AddInt64(&engineFile.Length, int64(len(kvs)))
	atomic.AddInt64(&engineFile.TotalSize, size)
	engineFile.Ts = ts
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

// splitValuesToRange try to cut [start, end) to count range approximately
// just like [start, v1), [v1, v2)... [vCount-1, end)
// return value []{v1, v2... vCount-1, End}
func (l *LocalFile) splitValuesToRange(start []byte, end []byte, count int64, sampleFactor int) [][]byte {
	opt := &pebble.IterOptions{LowerBound: start, UpperBound: end}
	iter := l.db.NewIter(opt)
	defer iter.Close()

	iter.First()
	start = append([]byte{}, iter.Key()...)
	iter.Last()
	end = nextKey(iter.Key())

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

	naiveFn := func() [][]byte {
		step := (eValue - sValue) / uint64(count)
		if step == uint64(0) {
			step = uint64(1)
		}

		res := make([][]byte, 0, count)
		var curBytes []byte
		iter.First()
		for cur := sValue + step; cur <= eValue-step; cur += step {
			curBytes = make([]byte, offset+8)
			copy(curBytes, start[:offset])
			binary.BigEndian.PutUint64(curBytes[offset:], cur)
			// if range is empty, skip range
			if bytes.Compare(curBytes, iter.Key()) < 0 {
				continue
			}
			// move to next range
			iter.SeekGE(curBytes)
			res = append(res, curBytes)
		}
		res = append(res, end)

		log.L().Info("split value naively", zap.Int64("count", count),
			zap.Int("ranges", len(res)))

		return res
	}

	sampleCount := uint64(l.Length) / 100 / uint64(sampleFactor)

	if sampleCount == 0 || eValue-sValue < sampleCount*20 {
		return naiveFn()
	}

	step := (eValue - sValue) / sampleCount

	sampleValues := make([]uint64, 0, sampleCount/10)

	lastValue := uint64(0)
	valueBuf := make([]byte, 8)
	seekKey := make([]byte, offset+8)
	copy(seekKey, start[:offset])
	for i := sValue; i < eValue; i += step {
		if i <= lastValue {
			continue
		}
		binary.BigEndian.PutUint64(seekKey[offset:], i)
		iter.SeekGE(seekKey)
		copy(valueBuf, iter.Key()[offset:])
		value := binary.BigEndian.Uint64(valueBuf)
		sampleValues = append(sampleValues, value)
		lastValue = value
	}

	// if too few sample values, fall back to naive func
	if len(sampleValues) < int(count)*20 {
		log.L().Info("too few samples, fallback to naive split", zap.Int64("count", count),
			zap.Int("samples", len(sampleValues)), zap.Binary("start", start),
			zap.Binary("end", end), zap.Int64("engine_kv", l.Length))
		return naiveFn()
	}

	s := float64(len(sampleValues)) / float64(count)

	res := make([][]byte, 0, count)
	for i := s - 1; int(i) < len(sampleValues); i += s {
		curBytes := make([]byte, offset+8)
		copy(curBytes, start[:offset])
		binary.BigEndian.PutUint64(curBytes[offset:], sampleValues[int(i)])
		res = append(res, curBytes)
	}
	// adjust last value
	if bytes.Compare(res[len(res)-1], end) < 0 {
		if len(res) < int(count) {
			res = append(res, end)
		} else {
			res[len(res)-1] = end
		}
	}
	log.L().Info("split value with sample", zap.Int64("count", count),
		zap.Int("ranges", len(res)), zap.Int("samples", len(sampleValues)),
		zap.Int64("engine_kv", l.Length))
	return res
}
