package kv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	sqltool "github.com/pingcap/tidb-lightning/lightning/sql"
	"github.com/pingcap/tidb/kv"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

const (
	encodeBatchRows = 1024
)

var (
	PrepareStmtMode = false
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
	if PrepareStmtMode {
		reserve := (encodeBatchRows * kvcodec.columns) << 1 // TODO : rows x ( cols + indices )
		kvcodec.bufValues = make([]interface{}, 0, reserve)

		stmtIds, err := kvcodec.makeStatments(encodeBatchRows)
		if err != nil {
			return errors.Trace(err)
		}
		kvcodec.stmtIds = stmtIds
	}

	return nil
}

func (kvcodec *TableKVEncoder) makeStatments(maxRows int) ([]uint32, error) {
	stmtIds := make([]uint32, maxRows+1)
	for rows := 1; rows <= maxRows; rows++ {
		stmtID, err := kvcodec.prepareStatment(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		stmtIds[rows] = stmtID
	}

	return stmtIds, nil
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
	if PrepareStmtMode {
		// via prepare statment
		kvPairs, rowsAffected, err := kvcodec.encodeViaPstmt(sql)
		if err == nil {
			return kvPairs, rowsAffected, nil
		}
		common.AppLogger.Warnf("[sql2kv] stmt encode err : %s", err.Error())
	}

	// via sql execution
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(string(sql), kvcodec.tableID)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] sql encode error = %v", err)
		return nil, 0, errors.Trace(err)
	}

	return kvPairs, rowsAffected, nil
}

func (kvcodec *TableKVEncoder) encodeViaPstmt(sql []byte) ([]kvec.KvPair, uint64, error) {
	values := kvcodec.bufValues
	defer func() {
		kvcodec.bufValues = kvcodec.bufValues[:0]
	}() // TODO ... calls many times ????

	err := sqltool.ParseInsertStmt(sql, &values)
	if err != nil {
		common.AppLogger.Errorf("[sql->kv] stmt mode encode failed : %s", err.Error())
		return nil, 0, errors.Trace(err)
	}

	cols := kvcodec.columns
	rows := len(values) / cols
	if len(values)%cols > 0 {
		err = errors.Errorf("[sql->kv] stmt values num not match (%d %% %d = %d) !",
			len(values), cols, len(values)%cols)
		common.AppLogger.Errorf(err.Error())
		return nil, 0, errors.Trace(err)
	}

	size := rows * cols
	kvPairs := make([]kvec.KvPair, 0, size+size>>1) // TODO : rows x (cols + indices)
	affectRows := uint64(0)

	encoder := kvcodec.encoder
	tableID := kvcodec.tableID
	for i := 0; i < rows; {
		batchRows := encodeBatchRows
		remainRows := rows - i
		if remainRows < batchRows {
			batchRows = remainRows
		}
		vals := values[i*cols : (i+batchRows)*cols]

		stmtID, err := kvcodec.applyStmtID(batchRows)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}

		kvs, affected, err := encoder.EncodePrepareStmt(tableID, stmtID, vals...)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}

		kvPairs = append(kvPairs, kvs...)
		affectRows += affected
		i += batchRows
	}

	return kvPairs, affectRows, nil
}

func (kvcodec *TableKVEncoder) applyStmtID(rows int) (uint32, error) {
	if rows < len(kvcodec.stmtIds) {
		return kvcodec.stmtIds[rows], nil
	}

	return kvcodec.prepareStatment(rows)
}

func (kvcodec *TableKVEncoder) prepareStatment(rows int) (uint32, error) {
	stmt := sqltool.MakePrepareStatement(kvcodec.table, kvcodec.columns, rows)
	stmtID, err := kvcodec.encoder.PrepareStmt(stmt)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] prepare stmt failed : %s", err.Error())
		return stmtID, errors.Trace(err)
	}

	return stmtID, nil
}
