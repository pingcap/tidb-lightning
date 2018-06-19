package kv

import (
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
	sqltool "github.com/pingcap/tidb-lightning/lightning/sql"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

type TableKVEncoder struct {
	db          string
	table       string
	tableID     int64
	tableSchema string
	columns     int

	stmtIDs struct {
		values map[int]uint32
		mu     *sync.RWMutex
	}

	encoder        kvec.KvEncoder
	idAllocator    *kvec.Allocator
	usePrepareStmt bool
}

// TODO: parameters are too long, refine it.
func NewTableKVEncoder(
	db string, table string, tableID int64,
	columns int, tableSchema string,
	sqlMode string, idAlloc *kvec.Allocator, usePrepareStmt bool) (*TableKVEncoder, error) {

	kvEncoder, err := kvec.New(db, idAlloc)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] kv encoder create failed : %v", err)
		return nil, errors.Trace(err)
	}

	err = kvEncoder.SetSystemVariable("sql_mode", sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	common.AppLogger.Debugf("set sql_mode=%s", sqlMode)

	enc := &TableKVEncoder{
		db:             db,
		table:          table,
		tableID:        tableID,
		columns:        columns,
		encoder:        kvEncoder,
		idAllocator:    idAlloc,
		tableSchema:    tableSchema,
		usePrepareStmt: usePrepareStmt,
	}
	enc.stmtIDs.values = make(map[int]uint32)
	enc.stmtIDs.mu = new(sync.RWMutex)

	if err = enc.init(); err != nil {
		enc.Close()
		return nil, errors.Trace(err)
	}

	return enc, nil
}

func (enc *TableKVEncoder) init() error {
	if err := enc.encoder.ExecDDLSQL(enc.tableSchema); err != nil {
		common.AppLogger.Errorf("[sql2kv] tableSchema execute failed : %v", err)
		return errors.Trace(err)
	}

	return nil
}

func (enc *TableKVEncoder) makeStatements(rows int) (uint32, error) {
	stmt := sqltool.MakePrepareStatement(enc.table, enc.columns, rows)
	stmtID, err := enc.encoder.PrepareStmt(stmt)
	return stmtID, errors.Trace(err)
}

func (enc *TableKVEncoder) ResetRowID(rowID int64) {
	enc.idAllocator.Reset(rowID)
}

func (enc *TableKVEncoder) Close() error {
	return errors.Trace(enc.encoder.Close())
}

func (enc *TableKVEncoder) NextRowID() int64 {
	return enc.idAllocator.Base() + 1
}

func (enc *TableKVEncoder) SQL2KV(payload *base.Payload) ([]kvec.KvPair, uint64, error) {
	if enc.usePrepareStmt {
		// via prepare statement
		kvPairs, rowsAffected, err := enc.encodeViaPstmt(payload.Params)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		return kvPairs, rowsAffected, nil
	}

	// via sql execution
	kvPairs, rowsAffected, err := enc.encoder.Encode(payload.SQL, enc.tableID)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] sql encode error = %v", err)
		return nil, 0, errors.Trace(err)
	}

	return kvPairs, rowsAffected, nil
}

func (enc *TableKVEncoder) encodeViaPstmt(params []interface{}) ([]kvec.KvPair, uint64, error) {
	if len(params)%enc.columns != 0 {
		// TODO: refine it
		panic("no exact division")
	}
	stmtID, err := enc.applyStmtID(len(params) / enc.columns)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	kvs, affected, err := enc.encoder.EncodePrepareStmt(enc.tableID, stmtID, params...)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return kvs, affected, nil
}

func (enc *TableKVEncoder) applyStmtID(rows int) (uint32, error) {
	enc.stmtIDs.mu.RLock()
	if stmtID, ok := enc.stmtIDs.values[rows]; ok {
		enc.stmtIDs.mu.RUnlock()
		return stmtID, nil
	}
	enc.stmtIDs.mu.RUnlock()

	// lazy prepare
	common.AppLogger.Infof("make prepare statement for %d rows", rows)
	stmtID, err := enc.makeStatements(rows)
	if err != nil {
		return 0, errors.Trace(err)
	}
	enc.stmtIDs.mu.Lock()
	enc.stmtIDs.values[rows] = stmtID
	enc.stmtIDs.mu.Unlock()

	return stmtID, nil
}
