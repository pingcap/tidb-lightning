package ingest

import (
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/meta/autoid"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

type TableKVEncoder struct {
	db      string
	table   string
	dbID    int64
	tableID int64

	encoder     kvec.KvEncoder
	idAllocator autoid.Allocator
}

func NewTableKVEncoder(
	db string, dbID int64,
	table string, tableID int64,
	tableSchema string, baseRowID int64) *TableKVEncoder {

	idAllocator := kvec.NewAllocator()
	idAllocator.Rebase(tableID, 0, false)

	kvEncoder, err := kvec.New(db, idAllocator)
	if err != nil {
		log.Errorf("kv encoder create failed : %v", err)
		return nil
	}

	kvcodec := &TableKVEncoder{
		db:          db,
		table:       table,
		dbID:        dbID,
		tableID:     tableID,
		encoder:     kvEncoder,
		idAllocator: idAllocator,
	}

	return kvcodec.init(tableSchema, baseRowID)
}

func (kvcodec *TableKVEncoder) init(sqlCreateTable string, baseRowID int64) *TableKVEncoder {
	if err := kvcodec.encoder.ExecDDLSQL(sqlCreateTable); err != nil {
		log.Errorf("ddl execute failed : %v", err)
	}
	kvcodec.idAllocator.Rebase(kvcodec.tableID, baseRowID, false)
	return kvcodec
}

func (kvcodec *TableKVEncoder) RebaseRowID(rowID int64) {
	kvcodec.idAllocator.Rebase(kvcodec.tableID, rowID, false)
}

func (kvcodec *TableKVEncoder) Close() {
	kvcodec.encoder.Close()
}

func (kvcodec *TableKVEncoder) NextRowID() int64 {
	return kvcodec.idAllocator.Base()
}

func (kvcodec *TableKVEncoder) Sql2KV(sql string) ([]kvec.KvPair, uint64) {
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(sql, kvcodec.tableID)
	if err != nil {
		log.Errorf("sql2kv execute error = %v", err)
		return []kvec.KvPair{}, 0
	}

	return kvPairs, rowsAffected
}
