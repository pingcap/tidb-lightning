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
	tableSchema string) (*TableKVEncoder, error) {

	idAllocator := kvec.NewAllocator()
	idAllocator.Rebase(tableID, 0, false)
	kvEncoder, err := kvec.New(db, idAllocator)
	if err != nil {
		log.Errorf("[sql2kv] kv encoder create failed : %v", err)
		return nil, err
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

	if err = kvcodec.init(); err != nil {
		kvcodec.Close()
		return nil, err
	}

	return kvcodec, nil
}

func (kvcodec *TableKVEncoder) init() error {
	if err := kvcodec.encoder.ExecDDLSQL(kvcodec.ddl); err != nil {
		log.Errorf("[sql2kv] ddl execute failed : %v", err)
		return err
	}
	return nil
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

func (kvcodec *TableKVEncoder) Sql2KV(sql string) ([]kvec.KvPair, uint64, error) {
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(sql, kvcodec.tableID)
	if err != nil {
		log.Errorf("[sql2kv] execute error = %v", err)
		return nil, 0, err
	}

	return kvPairs, rowsAffected, nil
}
