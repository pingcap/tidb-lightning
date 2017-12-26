package kv

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
	ddl     string

	encoder     kvec.KvEncoder
	idAllocator autoid.Allocator
}

func NewTableKVEncoder(
	db string, dbID int64,
	table string, tableID int64,
	tableSchema string) *TableKVEncoder {

	idAllocator := kvec.NewAllocator()
	idAllocator.Rebase(tableID, 0, false)

	kvEncoder, err := kvec.New(db, idAllocator)
	if err != nil {
		log.Errorf("[sql2kv] kv encoder create failed : %v", err)
		return nil
	}

	kvcodec := &TableKVEncoder{
		db:          db,
		table:       table,
		dbID:        dbID,
		tableID:     tableID,
		encoder:     kvEncoder,
		idAllocator: idAllocator,
		ddl:         tableSchema,
	}

	return kvcodec.init()
}

func (kvcodec *TableKVEncoder) init() *TableKVEncoder {
	if len(kvcodec.ddl) > 0 {
		if err := kvcodec.encoder.ExecDDLSQL(kvcodec.ddl); err != nil {
			log.Errorf("[sql2kv] ddl execute failed : %v", err)
		}
	}
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

func (kvcodec *TableKVEncoder) BuildMetaKvs(rowID int64) []kvec.KvPair {
	kv, err := kvcodec.encoder.EncodeMetaAutoID(kvcodec.dbID, kvcodec.tableID, rowID)
	if err != nil {
		log.Errorf("[sql2kv] build auot_id meta key error = %v", err)
	}
	return []kvec.KvPair{kv}
}

func (kvcodec *TableKVEncoder) Sql2KV(sql string) ([]kvec.KvPair, uint64) {
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(sql, kvcodec.tableID)
	if err != nil {
		log.Errorf("[sql2kv] execute error = %v", err)
		return []kvec.KvPair{}, 0
	}

	return kvPairs, rowsAffected
}
