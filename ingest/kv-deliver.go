package ingest

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/importpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/ngaut/log"
	_ "github.com/twinj/uuid"

	kvec "github.com/pingcap/tidb/util/kvencoder"
)

var (
	errInvalidUUID error = fmt.Errorf("uuid length must be 16")
)

func newImportClient(addr string) (importpb.ImportKVClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return importpb.NewImportKVClient(conn), nil
}

type KVDeliver struct {
	ctx     context.Context
	backend string
	pdAddr  string

	uuid []byte
	ts   uint64

	cli     importpb.ImportKVClient
	wstream importpb.ImportKV_WriteClient
}

func NewKVDeliver(ctx context.Context, uuid string, backend string, pdAddr string) (*KVDeliver, error) {
	if len(uuid) != 16 {
		return nil, errInvalidUUID
	}

	log.Infof("KV deliver handle UUID = [%s]", uuid)

	rpcCli, err := newImportClient(backend) // goruntine safe ???
	if err != nil {
		return nil, err
	}

	dlv := &KVDeliver{
		ctx:     ctx,
		uuid:    []byte(uuid),
		pdAddr:  pdAddr,
		backend: backend,
		cli:     rpcCli,
		ts:      uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
	}

	dlv.wstream, err = dlv.newWriteStream(uuid)
	if err != nil {
		return nil, err
	}

	return dlv, nil
}

func (d *KVDeliver) newWriteStream(uuid string) (importpb.ImportKV_WriteClient, error) {
	wstream, err := d.cli.Write(d.ctx)
	if err != nil {
		return nil, err
	}

	// Bind uuid for this write request
	req := &importpb.WriteRequest{
		Head: &importpb.WriteRequest_Head{
			Uuid: []byte(uuid),
		},
	}
	if err = wstream.Send(req); err != nil {
		wstream.CloseAndRecv()
		return nil, err
	}

	return wstream, nil
}

func (d *KVDeliver) Close() error {
	return d.closeWriteStream()
}

func (d *KVDeliver) closeWriteStream() error {
	if d.wstream == nil {
		return nil
	}
	defer func() {
		d.wstream = nil
	}()

	if _, err := d.wstream.CloseAndRecv(); err != nil {
		log.Errorf("close write stream cause failed : %v", err)
		return err
	}
	return nil
}

func (dlv *KVDeliver) Put(kvs []kvec.KvPair) error {
	/*
		TODO : reusing streaming connection in a session !
	*/

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
			CommitTs:  dlv.ts,
			Mutations: mutations,
		},
	}
	if err := dlv.wstream.Send(write); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (dlv *KVDeliver) Flush() error {
	dlv.closeWriteStream()

	flush := &importpb.FlushRequest{
		Uuid:    dlv.uuid,
		Address: dlv.pdAddr,
	}

	_, err := dlv.cli.Flush(dlv.ctx, flush)
	return err
}
