package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
)

type tikvBackend struct {
	tikvStore kv.Storage
	tls       *common.TLS
}

func NewTiKVBackend(pdAddr string, tls *common.TLS) (Backend, error) {
	driver := &tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s", pdAddr))
	if err != nil {
		return MakeBackend(nil), err
	}
	return MakeBackend(&tikvBackend{
		tikvStore: store,
		tls:       tls,
	}), nil
}

func (be *tikvBackend) Close() {
	_ = be.tikvStore.Close()
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
	return true
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

func (be *tikvBackend) WriteRows(ctx context.Context, _ uuid.UUID, tableName string, columnNames []string, ts uint64, r Rows) error {
	kvs := r.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	tsn, err := be.tikvStore.Begin()
	if err != nil {
		return err
	}

	for _, pair := range kvs {
		if err = tsn.Set(pair.Key, pair.Val); err != nil {
			return err
		}
	}

	return tsn.Commit(ctx)
}

func (be *tikvBackend) FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	err := be.tls.GetJSON("/schema/"+schemaName, &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read schema '%s' from remote", schemaName)
	}
	return tables, nil
}
