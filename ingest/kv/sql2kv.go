package kv

import (
	"github.com/juju/errors"
	sqltool "github.com/pingcap/tidb-lightning/ingest/sql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/plan"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	log "github.com/sirupsen/logrus"
)

const (
	encodeBatchRows = 1024
)

var (
	PrepareStmtMode = false
)

func setGlobalVars() {
	// hardcode it
	plan.PreparedPlanCacheEnabled = true
	plan.PreparedPlanCacheCapacity = 10
}

func InitMembufCap(batchSQLLength int64) {
	kv.ImportingTxnMembufCap = int(batchSQLLength) * 4
	// TODO : calculate predicted ratio, bwtween sql and kvs' size, base on specified DDL
}

type TableKVEncoder struct {
	db      string
	table   string
	dbID    int64
	tableID int64
	ddl     string
	columns int

	stmtIds   []uint32
	bufValues []interface{}

	encoder     kvec.KvEncoder
	idAllocator autoid.Allocator
}

func NewTableKVEncoder(
	db string, dbID int64,
	table string, tableID int64,
	columns int, tableSchema string, sqlMode string) (*TableKVEncoder, error) {

	idAllocator := kvec.NewAllocator()
	idAllocator.Rebase(tableID, 0, false)
	kvEncoder, err := kvec.New(db, idAllocator)
	if err != nil {
		log.Errorf("[sql2kv] kv encoder create failed : %v", err)
		return nil, errors.Trace(err)
	}

	err = kvEncoder.SetSystemVariable("sql_mode", sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debugf("set sql_mode=%s", sqlMode)

	setGlobalVars()

	kvcodec := &TableKVEncoder{
		db:          db,
		table:       table,
		dbID:        dbID,
		tableID:     tableID,
		encoder:     kvEncoder,
		idAllocator: idAllocator,
		ddl:         tableSchema,
		columns:     columns,
	}

	if err = kvcodec.init(); err != nil {
		kvcodec.Close()
		return nil, errors.Trace(err)
	}

	return kvcodec, nil
}

func (kvcodec *TableKVEncoder) init() error {
	if err := kvcodec.encoder.ExecDDLSQL(kvcodec.ddl); err != nil {
		log.Errorf("[sql2kv] ddl execute failed : %v", err)
		return errors.Trace(err)
	}

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

func (kvcodec *TableKVEncoder) RebaseRowID(rowID int64) {
	kvcodec.idAllocator.Rebase(kvcodec.tableID, rowID, false)
}

func (kvcodec *TableKVEncoder) Close() error {
	return kvcodec.encoder.Close()
}

func (kvcodec *TableKVEncoder) NextRowID() int64 {
	return kvcodec.idAllocator.Base()
}

func (kvcodec *TableKVEncoder) BuildMetaKvs(rowID int64) ([]kvec.KvPair, error) {
	kv, err := kvcodec.encoder.EncodeMetaAutoID(kvcodec.dbID, kvcodec.tableID, rowID)
	if err != nil {
		log.Errorf("[sql2kv] build auot_id meta key error = %v", err)
	}
	return []kvec.KvPair{kv}, nil
}

func (kvcodec *TableKVEncoder) SQL2KV(sql []byte) ([]kvec.KvPair, uint64, error) {
	if PrepareStmtMode {
		// via prepare statment
		kvPairs, rowsAffected, err := kvcodec.encodeViaPstmt(sql)
		if err == nil {
			return kvPairs, rowsAffected, nil
		}
		log.Warnf("[sql2kv] stmt encode err : %s", err.Error())
	}

	// via sql execution
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(string(sql), kvcodec.tableID)
	if err != nil {
		log.Errorf("[sql2kv] sql encode error = %v", err)
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
		log.Errorf("[sql->kv] stmt mode encode failed : %s", err.Error())
		return nil, 0, errors.Trace(err)
	}

	cols := kvcodec.columns
	rows := len(values) / cols
	if len(values)%cols > 0 {
		err = errors.Errorf("[sql->kv] stmt values num not match (%d %% %d = %d) !",
			len(values), cols, len(values)%cols)
		log.Errorf(err.Error())
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
		log.Errorf("[sql2kv] prepare stmt failed : %s", err.Error())
		return stmtID, errors.Trace(err)
	}

	return stmtID, nil
}
