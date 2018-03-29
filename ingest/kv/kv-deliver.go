package kv

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/ingest/importpb"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	log "github.com/sirupsen/logrus"

	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	errInvalidUUID = errors.New("uuid length must be 16")
	invalidUUID    = uuid.Nil
)

const (
	_G             uint64 = 1 << 30
	flushSizeLimit uint64 = 1 * _G
	maxRetryTimes  int    = 3
)

var (
	DeliverTxnSizeLimit  = int64(500 * _G)
	DeliverTxnPairsLimit = int64(math.MaxInt64)
)

type KVDeliver interface {
	Put([]kvec.KvPair) error
	Flush() error // + Import() error
	Cleanup() error
	Compact(start, end []byte) error
	Close() error
}

func ConfigDeliverTxnBatchSize(kvBatchSize int64) {
	if kvBatchSize > 0 {
		DeliverTxnSizeLimit = int64(uint64(kvBatchSize) * _G)
	}
}

/////////////////////// KV Deliver Manager ///////////////////////

const (
	opPut int = iota
	opFlush
	opImport
	opCleanup
)

type deliverTask struct {
	op    int
	kvs   []kvec.KvPair
	retry int

	// TODO .. callback ?
}

type PipeKvDeliver struct {
	wg  sync.WaitGroup
	mux sync.Mutex

	ctx      context.Context
	shutdown context.CancelFunc

	uuid    uuid.UUID
	deliver *KVDeliverClient
	tasks   chan *deliverTask

	sumPuts   uint32
	sumKVSize uint64
}

func NewPipeKvDeliver(uuid uuid.UUID, importServerAddr string, pdAddr string) (*PipeKvDeliver, error) {
	ctx, shutdown := context.WithCancel(context.Background())

	deliver, err := NewKVDeliverClient(context.Background(), uuid, importServerAddr, pdAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := &PipeKvDeliver{
		ctx:      ctx,
		shutdown: shutdown,
		uuid:     uuid,
		deliver:  deliver,
		tasks:    make(chan *deliverTask, 128),
	}
	p.start()
	return p, nil
}

func (p *PipeKvDeliver) Close() error {
	p.shutdown()
	p.wg.Wait()
	return p.deliver.Close()
}

func (p *PipeKvDeliver) CloseAndWait() error {
	p.shutdown()
	p.wg.Wait()

	for len(p.tasks) > 0 {
		select {
		case task := <-p.tasks:
			p.handle(task)
		default:
		}
	}

	return p.deliver.Close()
}

func (p *PipeKvDeliver) Put(kvs []kvec.KvPair) error {
	atomic.AddUint32(&p.sumPuts, 1)
	p.tasks <- &deliverTask{
		op:    opPut,
		kvs:   kvs,
		retry: 0,
	}
	return nil
}

func (p *PipeKvDeliver) Flush() error {
	p.tasks <- &deliverTask{op: opFlush}
	return nil
}

func (p *PipeKvDeliver) Cleanup() error {
	p.tasks <- &deliverTask{op: opCleanup}
	return nil
}

func (p *PipeKvDeliver) start() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(p.ctx)
	}()
}

func (p *PipeKvDeliver) run(ctx context.Context) {
	var task *deliverTask
	var err error

	for {
		select {
		case <-ctx.Done():
			return
		case task = <-p.tasks:
			if err = p.handle(task); err != nil {
				log.Warnf("[%s] Deliver task failed (retry = %d) : %s", p.uuid, err.Error())
				if task.retry > maxRetryTimes {
					break // TODO ...
				}
				// ps : p.tasks might full ~
				task.retry++
				p.tasks <- task
			}
		}
	}
}

func (p *PipeKvDeliver) handle(task *deliverTask) error {
	if task == nil {
		return nil
	}

	var err error
	switch task.op {
	case opPut:
		var dataSize int
		for _, pair := range task.kvs {
			dataSize += len(pair.Key) + len(pair.Val)
		}

		err = p.deliver.Put(task.kvs)
		if err != nil {
			log.Errorf("kv deliver manager put failed : %s", err.Error())
		} else {
			p.sumKVSize += uint64(dataSize)
			// TODO ... determine to call flush ~
		}

		/*if p.sumKVSize >= flushSizeLimit {
			if err := p.doFlush(); err != nil {
				log.Errorf("kv deliver manager auto flush failed (put size = %d) : %s",
					p.sumKVSize, err.Error())
			} else {
				p.sumKVSize = 0
			}
		}*/

	case opFlush:
		err = p.doFlush()
	case opCleanup:
		err = p.deliver.Cleanup() // TODO .. error
	default:
	}

	return errors.Trace(err)
}

func (p *PipeKvDeliver) doFlush() error {
	log.Infof("kv deliver manager do flush !")

	err := p.deliver.Flush()
	if err != nil {
		log.Errorf("kv deliver manager flush failed : %s", err.Error())
	}

	return errors.Trace(err)
}

/////////////////////// KV Deliver Transaction ///////////////////////

const (
	txnNil int = iota
	txnPutting
	txnFlushing
	txnClosed
)

type deliverTxn struct {
	mux     sync.RWMutex
	uuid    uuid.UUID
	stat    int
	kvSize  int64
	kvPairs int64
}

func newDeliverTxn(uuid uuid.UUID) *deliverTxn {
	return &deliverTxn{
		uuid:    uuid,
		stat:    txnPutting,
		kvSize:  0,
		kvPairs: 0,
	}
}

func (txn *deliverTxn) update(kvSize int, kvPairs int) {
	txn.mux.Lock()
	txn.kvSize += int64(kvSize)
	txn.kvPairs += int64(kvPairs)
	txn.mux.Unlock()
}

func (txn *deliverTxn) isOverLimit(kvSizeLimit int64, kvPairsLimit int64) bool {
	txn.mux.RLock()
	over := (txn.kvSize >= kvSizeLimit) || (txn.kvPairs >= kvPairsLimit)
	txn.mux.RUnlock()
	return over
}

func (txn *deliverTxn) updateStatus(stat int) {
	txn.mux.Lock()
	txn.stat = stat
	txn.mux.Unlock()
}

func (txn *deliverTxn) inStatus(stat int) bool {
	txn.mux.RLock()
	res := (txn.stat == stat)
	txn.mux.RUnlock()
	return res
}

/////////////////////// KV Deliver Keeper ///////////////////////

type KVDeliverKeeper struct {
	mux      sync.Mutex
	wg       sync.WaitGroup
	ctx      context.Context
	shutdown context.CancelFunc

	importServerAddr string
	pdAddr           string
	clientsPool      []*KVDeliverClient // aka. connection pool

	txnIdCounter int // TODO : need to update to another algorithm
	txnBoard     map[uuid.UUID]*txnInfo
	txns         map[string][]*deliverTxn // map[tag]{*txn, *txn, *txn ...}

	flushWg       sync.WaitGroup
	txnFlushQueue chan *deliverTxn
}

type txnInfo struct {
	txn     *deliverTxn
	db      string
	table   string
	clients int
}

func NewKVDeliverKeeper(importServerAddr, pdAddr string) *KVDeliverKeeper {
	ctx, shutdown := context.WithCancel(context.Background())

	keeper := &KVDeliverKeeper{
		ctx:      ctx,
		shutdown: shutdown,

		importServerAddr: importServerAddr,
		pdAddr:           pdAddr,
		clientsPool:      make([]*KVDeliverClient, 0, 32),

		txnIdCounter:  0, // TODO : need to update to another algorithm
		txns:          make(map[string][]*deliverTxn),
		txnBoard:      make(map[uuid.UUID]*txnInfo),
		txnFlushQueue: make(chan *deliverTxn, 64),
	}

	go keeper.handleTxnFlush(keeper.ctx)

	return keeper
}

func buildTag(db string, table string) string {
	return fmt.Sprintf("%s.%s", db, table)
}

func (k *KVDeliverKeeper) Close() error {
	k.mux.Lock()
	defer k.mux.Unlock()

	k.shutdown()
	k.wg.Wait()

	// close all client/connection
	for _, cli := range k.clientsPool {
		cli.Close()
	}

	return nil
}

func (k *KVDeliverKeeper) validate(txn *deliverTxn) bool {
	// check - txn size limit
	// check - txn status
	return txn.inStatus(txnPutting) &&
		!txn.isOverLimit(DeliverTxnSizeLimit, DeliverTxnPairsLimit)
}

func (k *KVDeliverKeeper) newTxn(db string, table string) *deliverTxn {
	k.txnIdCounter++
	uuid := uuid.Must(uuid.NewV4())

	tag := buildTag(db, table)
	txn := newDeliverTxn(uuid)
	log.Infof("[deliver-keeper] new txn (UUID = %s) for [%s]", txn.uuid, tag)

	return txn
}

func (k *KVDeliverKeeper) applyTxn(db string, table string) *deliverTxn {
	var txn *deliverTxn

	// try to choose a valid deliver txn to join
	tag := buildTag(db, table)
	tagTxns, ok := k.txns[tag]
	if ok {
		for _, tx := range tagTxns {
			if k.validate(tx) {
				txn = tx
				break
			}
		}
	}

	// not any valid txn to join, so create a fresh deliver transaction
	if txn == nil {
		txn = k.newTxn(db, table)

		tagTxns = make([]*deliverTxn, 0, 4)
		tagTxns = append(tagTxns, txn)
		k.txns[tag] = tagTxns
		log.Infof("[deliver-keeper] holds [%s] txn count = %d", tag, len(tagTxns))

		k.txnBoard[txn.uuid] = &txnInfo{
			txn:     txn,
			db:      db,
			table:   table,
			clients: 0,
		}
		log.Infof("[deliver-keeper] holds txn total = %d", len(k.txnBoard))
	}

	return txn
}

func (k *KVDeliverKeeper) RecycleClient(cli *KVDeliverClient) {
	k.mux.Lock()
	defer k.mux.Unlock()

	// reusing client / connection
	k.clientsPool = append(k.clientsPool, cli)
	// log.Debugf("after recycle, clients = %d", len(k.clientsPool))

	// update txn to check whether to do flushing
	txn := cli.txn
	txnInfo, ok := k.txnBoard[txn.uuid]
	if !ok {
		log.Warnf("Impossible, txn not found (UUID = %s)", txn.uuid)
		return
	}

	txnInfo.clients-- // ps : simple counter to mark txn is being followed
	if txnInfo.clients <= 0 &&
		txn.inStatus(txnPutting) &&
		txn.isOverLimit(DeliverTxnSizeLimit, DeliverTxnPairsLimit) {

		k.flushTxn(txn)
	}
}

func (k *KVDeliverKeeper) AcquireClient(db string, table string) *KVDeliverClient {
	k.mux.Lock()
	defer k.mux.Unlock()

	// try to choose an existing transaction
	txn := k.applyTxn(db, table)

	// pop client/connection from pool
	size := len(k.clientsPool)
	if size == 0 {
		cli, err := NewKVDeliverClient(k.ctx, txn.uuid, k.importServerAddr, k.pdAddr)
		if err != nil {
			log.Infof("[deliver-keeper] failed to create deliver client (UUID = %s) : %s ", txn.uuid, err.Error())
			return nil
		}

		k.clientsPool = append(k.clientsPool, cli)
		size = 1
	}

	cli := k.clientsPool[size-1]
	k.clientsPool = k.clientsPool[:size-1]

	// address client with choosing deliver transaction
	k.txnBoard[txn.uuid].clients++ // ps : simple counter to mark txn is being joined
	cli.bind(txn)

	return cli
}

func (k *KVDeliverKeeper) Compact(start, end []byte) error {
	cli, err := NewKVDeliverClient(k.ctx, uuid.Nil, k.importServerAddr, k.pdAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	return cli.Compact(start, end)
}

func (k *KVDeliverKeeper) Flush() error {
	k.mux.Lock()
	defer k.mux.Unlock()

	for _, ttxns := range k.txns {
		for _, t := range ttxns {
			if t.inStatus(txnPutting) {
				k.flushTxn(t)
			}
		}
	}

	// TODO ... update txn board ??

	k.flushWg.Wait() // block and wait all txn finished flushing

	return nil
}

func (k *KVDeliverKeeper) closeTxnClients(txn *deliverTxn) {
	/*
		TODO :
			Store all clients in case missing recycle any client
		so that here we can close all clients generated from keeper perfetly.
			Otherwise, if there any remainning client/connecton bind to the txn,
		it's unable to flush that txn !
	*/

	// close all connection related to this txn
	for _, cli := range k.clientsPool {
		if cli.txn.uuid == txn.uuid {
			cli.exitTxn()
		}
	}
}

func (k *KVDeliverKeeper) flushTxn(txn *deliverTxn) {
	log.Infof("[deliver-keeper] gonna to flush txn (UUID = %s)", txn.uuid)

	// release relating client/connection first
	k.closeTxnClients(txn)

	// flush all kvs owned by this txn
	txn.updateStatus(txnFlushing)
	k.flushWg.Add(1)
	k.txnFlushQueue <- txn
}

func (k *KVDeliverKeeper) handleTxnFlush(ctx context.Context) {
	doFlush := func(txn *deliverTxn) {
		cli, err := NewKVDeliverClient(ctx, txn.uuid, k.importServerAddr, k.pdAddr)
		if err != nil {
			log.Infof("[deliver-keeper] failed to create deliver client (UUID = %s) : %s ", txn.uuid, err.Error())
			return
		}
		defer func() {
			cli.Close()
			txn.updateStatus(txnClosed)
		}()

		if err := cli.Flush(); err != nil {
			log.Infof("[deliver-keeper] txn (UUID = %s) flush failed : %s ", txn.uuid, err.Error())
		} else {
			cli.Cleanup()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case txn := <-k.txnFlushQueue:
			now := time.Now()
			log.Infof("[deliver-keeper] start flushing txn (UUID = %s) ... ", txn.uuid)

			doFlush(txn)

			k.flushWg.Done()
			log.Infof("[deliver-keeper] finished flushing txn (UUID = %s)", txn.uuid)
			log.Infof("[deliver-keeper] cost time = %.1f sec", time.Since(now).Seconds())
		}
	}
}

/////////////////////// KV Deliver Handler ///////////////////////

/* ps : not thread safe !!! */

type KVDeliverClient struct {
	ctx context.Context

	importServerAddr string
	pdAddr           string
	ts               uint64
	txn              *deliverTxn

	conn    *grpc.ClientConn
	cli     importpb.ImportKVClient
	wstream importpb.ImportKV_WriteClient
}

func newImportClient(importServerAddr string) (*grpc.ClientConn, importpb.ImportKVClient, error) {
	conn, err := grpc.Dial(importServerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return conn, importpb.NewImportKVClient(conn), nil
}

func NewKVDeliverClient(ctx context.Context, uuid uuid.UUID, importServerAddr string, pdAddr string) (*KVDeliverClient, error) {
	conn, rpcCli, err := newImportClient(importServerAddr) // goruntine safe ???
	if err != nil {
		return nil, err
	}

	cli := &KVDeliverClient{
		ctx:              ctx,
		ts:               uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
		importServerAddr: importServerAddr,
		pdAddr:           pdAddr,
		conn:             conn,
		cli:              rpcCli,
		txn:              newDeliverTxn(uuid),
	}

	return cli, nil
}

func (c *KVDeliverClient) Close() error {
	defer c.conn.Close()
	return c.closeWriteStream()
}

func (c *KVDeliverClient) bind(txn *deliverTxn) {
	log.Infof("Bind kv client with txn (UUID = %s)", txn.uuid)

	if c.txn.uuid != txn.uuid {
		// So as to update stream bound to a new uuid,
		// It's necessary to close former in using stream at first.
		c.closeWriteStream()
	}

	c.txn = txn
	return
}

func (c *KVDeliverClient) exitTxn() {
	log.Infof("Release kv client from txn (UUID = %s)", c.txn.uuid)
	c.closeWriteStream()
	c.txn = newDeliverTxn(invalidUUID)
	return
}

func (c *KVDeliverClient) open(uuid uuid.UUID) error {
	openRequest := &importpb.OpenRequest{
		Uuid: c.txn.uuid.Bytes(),
	}

	_, err := c.cli.Open(c.ctx, openRequest)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *KVDeliverClient) newWriteStream() (importpb.ImportKV_WriteClient, error) {
	if err := c.open(c.txn.uuid); err != nil {
		return nil, errors.Trace(err)
	}

	wstream, err := c.cli.Write(c.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Bind uuid for this write request
	req := &importpb.WriteRequest{
		Head: &importpb.WriteRequest_Head{
			Uuid: c.txn.uuid.Bytes(),
		},
	}
	if err = wstream.Send(req); err != nil {
		wstream.CloseAndRecv()
		return nil, errors.Trace(err)
	}

	return wstream, nil
}

func (c *KVDeliverClient) closeWriteStream() error {
	if c.wstream == nil {
		return nil
	}
	defer func() {
		c.wstream = nil
	}()

	if _, err := c.wstream.CloseAndRecv(); err != nil {
		log.Errorf("close write stream cause failed : %v", err)
		return errors.Trace(err)
	}
	return nil
}

func (c *KVDeliverClient) getWriteStream() (importpb.ImportKV_WriteClient, error) {
	if c.wstream == nil {
		wstream, err := c.newWriteStream()
		if err != nil {
			log.Errorf("[kv-deliver] failed to build write stream : %s", err.Error())
			return nil, errors.Trace(err)
		}
		c.wstream = wstream
	}
	return c.wstream, nil
}

func (c *KVDeliverClient) Put(kvs []kvec.KvPair) error {
	wstream, err := c.getWriteStream()
	if err != nil {
		return errors.Trace(err)
	}

	// Send kv paris as write request content
	// TODO :
	//		* too many to seperate batch ??
	//		* buffer pool []*importpb.Mutation
	// 		* handle partial transportation -- rollback ? clear ?
	pairNum := len(kvs)
	mutations := make([]*importpb.Mutation, 0, pairNum)
	for _, pair := range kvs {
		mutations = append(mutations, &importpb.Mutation{
			Op:    importpb.Mutation_Put,
			Key:   pair.Key,
			Value: pair.Val,
		})
	}

	write := &importpb.WriteRequest{
		Batch: &importpb.WriteBatch{
			CommitTs:  c.ts,
			Mutations: mutations,
		},
	}

	if err := wstream.Send(write); err != nil {
		log.Errorf("[kv-deliver] write stream failed to send : %s", err.Error())
		c.closeWriteStream()
		return errors.Trace(err)
	}

	kvSize := 0
	for _, kv := range kvs {
		kvSize += (len(kv.Key) + len(kv.Val))
	}
	c.txn.update(kvSize, len(kvs))

	return nil
}

func (c *KVDeliverClient) Cleanup() error {
	c.closeWriteStream()

	req := &importpb.CleanupRequest{Uuid: c.txn.uuid.Bytes()}
	_, err := c.cli.Cleanup(c.ctx, req)
	return errors.Trace(err)
}

func (c *KVDeliverClient) Flush() error {
	c.closeWriteStream()

	ops := []func() error{c.callClose, c.callImport}
	for step, fn := range ops {
		if err := fn(); err != nil {
			log.Errorf("[kv-deliver] flush stage with error (step = %d) : %s", step, err.Error())
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *KVDeliverClient) Compact(start, end []byte) error {
	return c.callCompact(start, end)
}

// Do compaction for specific table. `start` and `end`` key can be got in the following way:
// start key = GenTablePrefix(tableID)
// end key = GenTablePrefix(tableID + 1)
func (c *KVDeliverClient) callCompact(start, end []byte) error {
	log.Infof("call compact ...")
	req := &importpb.CompactRequest{PdAddr: c.pdAddr, Range: &importpb.Range{Start: start, End: end}}
	_, err := c.cli.Compact(c.ctx, req)
	log.Infof("finish call compact !")

	return errors.Trace(err)
}

func (c *KVDeliverClient) callClose() error {
	log.Infof("call close ...")
	req := &importpb.CloseRequest{Uuid: c.txn.uuid.Bytes()}
	_, err := c.cli.Close(c.ctx, req)
	log.Infof("finish call close !")

	return errors.Trace(err)
}

func (c *KVDeliverClient) callImport() error {
	// TODO ... no matter what, to enusure available to import, call close first !
	log.Infof("call import ...")
	req := &importpb.ImportRequest{Uuid: c.txn.uuid.Bytes()}
	_, err := c.cli.Import(c.ctx, req)
	log.Infof("finish call import !")

	return errors.Trace(err)
}
