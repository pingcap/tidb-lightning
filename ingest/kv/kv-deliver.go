package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/importpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	kvec "github.com/pingcap/tidb/util/kvencoder"
)

var (
	errInvalidUUID error = fmt.Errorf("uuid length must be 16")
)

const (
	_G             uint64 = 1 << 30
	flushSizeLimit uint64 = 1 * _G
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
	mux      sync.Mutex
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
	p.closeAndWait()
	return nil
}

func (p *PipeKvDeliver) closeAndWait() {
	p.shutdown()
	p.wg.Wait()

	for len(p.tasks) > 0 {
		select {
		case task := <-p.tasks:
			p.handle(task)
		default:
		}
	}

	p.deliver.Close()
	return
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
				if task.retry > 3 {
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
		var dataSize int = 0
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

	return err
}

func (p *PipeKvDeliver) doFlush() error {
	log.Infof("kv deliver manager do flush !")

	err := p.deliver.Flush()
	if err != nil {
		log.Errorf("kv deliver manager flush failed : %s", err.Error())
	}

	return err
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
		return nil, err
	}

	// Bind uuid for this write request
	req := &importpb.WriteRequest{
		Head: &importpb.WriteRequest_Head{
			Uuid: []byte(c.uuid),
		},
	}
	if err = wstream.Send(req); err != nil {
		wstream.CloseAndRecv()
		return nil, err
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
		return err
	}
	return nil
}

func (c *KVDeliverClient) Put(kvs []kvec.KvPair) error {
	if c.wstream == nil {
		if wstream, err := c.newWriteStream(); err != nil {
			return nil
		} else {
			c.wstream = wstream
		}
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

	if err := c.wstream.Send(write); err != nil {
		c.closeWriteStream()
		return errors.Trace(err)
	}

	return nil
}

func (c *KVDeliverClient) Cleanup() error {
	c.closeWriteStream()

	req := &importpb.CleanupRequest{Uuid: c.uuid}
	_, err := c.cli.Cleanup(c.ctx, req)
	return err
}

func (c *KVDeliverClient) Flush() error {
	c.closeWriteStream()

	// TODO ...

	ops := []func() error{c.callFlush, c.callImport}
	for _, fn := range ops {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (c *KVDeliverClient) callFlush() error {
	req := &importpb.FlushRequest{Uuid: c.uuid}
	_, err := c.cli.Flush(c.ctx, req)
	return err
}

func (c *KVDeliverClient) callImport() error {
	req := &importpb.ImportRequest{Uuid: c.uuid}
	_, err := c.cli.Import(c.ctx, req)
	return err
}
