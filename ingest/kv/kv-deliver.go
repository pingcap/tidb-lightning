package kv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/importpb"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	errInvalidUUID error = errors.New("uuid length must be 16")
)

const (
	_G             uint64 = 1 << 30
	flushSizeLimit uint64 = 1 * _G
	maxRetryTimes  int    = 3
)

type KVDeliver interface {
	Put([]kvec.KvPair) error
	Flush() error // + Import() error
	Cleanup() error
	Close() error
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
	wg       sync.WaitGroup
	ctx      context.Context
	shutdown context.CancelFunc

	uuid    string
	deliver *KVDeliverClient
	tasks   chan *deliverTask

	sumPuts   uint32
	sumKVSize uint64
}

func NewPipeKvDeliver(uuid string, backend string, pdAddr string) (*PipeKvDeliver, error) {
	ctx, shutdown := context.WithCancel(context.Background())

	deliver, err := NewKVDeliverClient(context.Background(), uuid, backend, pdAddr)
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
	p.sumPuts = atomic.AddUint32(&p.sumPuts, 1)
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
				go func() {
					// ps : p.tasks might full ~
					task.retry += 1
					p.tasks <- task
				}()
			}
		}
	}

	return
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

/////////////////////// KV Deliver Handler ///////////////////////

type KVDeliverClient struct {
	ctx     context.Context
	backend string
	pdAddr  string // useless

	uuid []byte
	ts   uint64

	conn    *grpc.ClientConn
	cli     importpb.ImportKVClient
	wstream importpb.ImportKV_WriteClient
}

func newImportClient(addr string) (*grpc.ClientConn, importpb.ImportKVClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}

	return conn, importpb.NewImportKVClient(conn), nil
}

func NewKVDeliverClient(ctx context.Context, uuid string, backend string, pdAddr string) (*KVDeliverClient, error) {
	if len(uuid) != 16 {
		return nil, errInvalidUUID
	}

	log.Infof("KV deliver handle UUID = [%s]", uuid)

	conn, rpcCli, err := newImportClient(backend) // goruntine safe ???
	if err != nil {
		return nil, err
	}

	c := &KVDeliverClient{
		ctx:     ctx,
		uuid:    []byte(uuid),
		pdAddr:  pdAddr, // useless
		backend: backend,
		conn:    conn,
		cli:     rpcCli,
		ts:      uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
	}

	// c.wstream, err = c.newWriteStream()
	// if err != nil {
	// 	return nil, err
	// }

	return c, nil
}

func (c *KVDeliverClient) Close() error {
	defer c.conn.Close()
	return c.closeWriteStream()
}

func (c *KVDeliverClient) newWriteStream() (importpb.ImportKV_WriteClient, error) {
	wstream, err := c.cli.Write(c.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Bind uuid for this write request
	req := &importpb.WriteRequest{
		Head: &importpb.WriteRequest_Head{
			Uuid: []byte(c.uuid),
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

	return nil
}

func (c *KVDeliverClient) Cleanup() error {
	c.closeWriteStream()

	req := &importpb.DeleteRequest{Uuid: c.uuid}
	_, err := c.cli.Delete(c.ctx, req)
	return errors.Trace(err)
}

func (c *KVDeliverClient) Flush() error {
	c.closeWriteStream()

	ops := []func() error{c.callFlush, c.callClose, c.callImport}
	for step, fn := range ops {
		if err := fn(); err != nil {
			log.Errorf("[kv-deliver] flush stage with error (step = %d) : %s", step, err.Error())
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *KVDeliverClient) callFlush() error {
	wstream, err := c.getWriteStream()
	if err != nil {
		return errors.Trace(err)
	}
	defer c.closeWriteStream()

	write := &importpb.WriteRequest{
		Batch: &importpb.WriteBatch{
			CommitTs:  c.ts,
			Mutations: []*importpb.Mutation{},
		},
		// flush to flush
		Options: &importpb.WriteOptions{Flush: true},
	}

	return wstream.Send(write)
}

func (c *KVDeliverClient) callClose() error {
	req := &importpb.CloseRequest{Uuid: c.uuid}
	_, err := c.cli.Close(c.ctx, req)
	return errors.Trace(err)
}

func (c *KVDeliverClient) callImport() error {
	req := &importpb.ImportRequest{Uuid: c.uuid}
	_, err := c.cli.Import(c.ctx, req)
	return errors.Trace(err)
}
