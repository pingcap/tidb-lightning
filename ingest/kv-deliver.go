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
	cli  importpb.ImportKVClient
}

func NewKVDeliver(ctx context.Context, uuid string, backend string, pdAddr string) (*KVDeliver, error) {
	if len(uuid) != 16 {
		return nil, errInvalidUUID
	}

	log.Infof("KV deliver [%s] ~", uuid)

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

	return dlv, nil
}

func (d *KVDeliver) Close() error {
	// TODO :
	//	* to close rpc connection ?
	//	* to release client ?
	return nil
}

func (dlv *KVDeliver) Put(kvs []kvec.KvPair) error {
	/*
		TODO : reusing streaming connection in a session !
	*/

	stream, err := dlv.cli.Write(dlv.ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. bind uuid for this write request
	wreq := &importpb.WriteRequest{
		Head: &importpb.WriteRequest_Head{
			Uuid: dlv.uuid,
		},
	}
	if err := stream.Send(wreq); err != nil {
		return errors.Trace(err)
	}

	// 2. send kv paris as write request content
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
	if err := stream.Send(write); err != nil {
		return errors.Trace(err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (dlv *KVDeliver) Flush() error {
	flush := &importpb.FlushRequest{
		Uuid:    dlv.uuid,
		Address: dlv.pdAddr,
	}

	_, err := dlv.cli.Flush(dlv.ctx, flush)
	return err
}
