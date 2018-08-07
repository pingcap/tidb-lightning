package kv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb/kv"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

const (
	encodeBatchRows = 1024
)

func InitMembufCap(batchSQLLength int64) {
	kv.ImportingTxnMembufCap = int(batchSQLLength) * 4
	// TODO : calculate predicted ratio, bwtween sql and kvs' size, base on specified DDL
}

type TableKVEncoder struct {
	table   string
	tableID int64
	columns int

	stmtIds   []uint32
	bufValues []interface{}

	encoder     kvec.KvEncoder
	idAllocator *kvec.Allocator
}

func NewTableKVEncoder(
	dbName string,
	table string, tableID int64,
	columns int, sqlMode string, alloc *kvec.Allocator) (*TableKVEncoder, error) {

	encoder, err := kvec.New(dbName, alloc)
	if err != nil {
		common.AppLogger.Errorf("err %s", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	kvcodec := &TableKVEncoder{
		table:       table,
		tableID:     tableID,
		encoder:     encoder,
		idAllocator: alloc,
		columns:     columns,
	}

	if err := kvcodec.init(sqlMode); err != nil {
		kvcodec.Close()
		return nil, errors.Trace(err)
	}

	metric.KvEncoderCounter.WithLabelValues("open").Inc()

	return kvcodec, nil
}

func (kvcodec *TableKVEncoder) init(sqlMode string) error {
	err := kvcodec.encoder.SetSystemVariable("sql_mode", sqlMode)
	if err != nil {
		return errors.Trace(err)
	}
	common.AppLogger.Debugf("set sql_mode=%s", sqlMode)

	return nil
}

func (kvcodec *TableKVEncoder) ResetRowID(rowID int64) {
	kvcodec.idAllocator.Reset(rowID)
}

func (kvcodec *TableKVEncoder) Close() error {
	metric.KvEncoderCounter.WithLabelValues("closed").Inc()
	return errors.Trace(kvcodec.encoder.Close())
}

func (kvcodec *TableKVEncoder) NextRowID() int64 {
	return kvcodec.idAllocator.Base() + 1
}

func (kvcodec *TableKVEncoder) SQL2KV(sql []byte) ([]kvec.KvPair, uint64, error) {
	// via sql execution
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(string(sql), kvcodec.tableID)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] sql encode error = %v", err)
		return nil, 0, errors.Trace(err)
	}

	return kvPairs, rowsAffected, nil
}
