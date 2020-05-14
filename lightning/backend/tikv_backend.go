package backend

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
	"time"
)

type tikvBackend struct {
	kvClient *tikv.RawKVClient
	tls *common.TLS
}

func NewTiKVBackend(pdAddr string, tls *common.TLS) (Backend, error) {
	client, err := tikv.NewRawKVClient([]string{pdAddr}, config.Security{})
	if err != nil {
		return MakeBackend(nil), err
	}
	return MakeBackend(&tikvBackend{
		kvClient:client,
		tls: tls,
	}), nil
}

func (be *tikvBackend) Close() {
	_ = be.kvClient.Close()
}

func (be *tikvBackend) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (be *tikvBackend) RetryImportDelay() time.Duration {
	return 0
}

func (be *tikvBackend) MaxChunkSize() int {
	// 96 MB
	return 96 * 1024 * 1024
}

func (be *tikvBackend) ShouldPostProcess() bool {
	return false
}

func (be *tikvBackend) CheckRequirements() error {
	log.L().Info("skipping check requirements for tikv backend")
	return nil
}

func (be *tikvBackend) NewEncoder(tbl table.Table, options *SessionOptions) Encoder {
	return NewTableKVEncoder(tbl, options)
}

func (be *tikvBackend) OpenEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) CloseEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) CleanupEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) ImportEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tikvBackend) WriteRows(ctx context.Context, _ uuid.UUID, tableName string, columnNames []string, _ uint64, r Rows) error {
	kvs := r.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	keys := make([][]byte, 0, len(kvs))
	values := make([][]byte, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
		values = append(values, kv.Val)
	}

	return be.kvClient.BatchPut(keys, values)
}

func (be *tikvBackend) FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	err := be.tls.GetJSON("/schema/"+schemaName, &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read schema '%s' from remote", schemaName)
	}
	return tables, nil
}