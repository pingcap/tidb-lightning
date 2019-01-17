// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	"github.com/pkg/errors"
)

func InitMembufCap(batchSQLLength int64) {
	kv.ImportingTxnMembufCap = int(batchSQLLength) * 4
	// TODO : calculate predicted ratio, bwtween sql and kvs' size, base on specified DDL
}

type TableKVEncoder struct {
	table       string
	tableID     int64
	encoder     kvec.KvEncoder
	idAllocator autoid.Allocator
}

func NewTableKVEncoder(
	dbName string,
	table string, tableID int64,
	sqlMode string, alloc autoid.Allocator) (*TableKVEncoder, error) {

	encoder, err := kvec.New(dbName, alloc)
	if err != nil {
		common.AppLogger.Errorf("err %s", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	err = encoder.SetSystemVariable("tidb_opt_write_row_id", "1")
	if err != nil {
		encoder.Close()
		return nil, errors.Trace(err)
	}

	kvcodec := &TableKVEncoder{
		table:       table,
		tableID:     tableID,
		encoder:     encoder,
		idAllocator: alloc,
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

func (kvcodec *TableKVEncoder) Close() error {
	metric.KvEncoderCounter.WithLabelValues("closed").Inc()
	return errors.Trace(kvcodec.encoder.Close())
}

func (kvcodec *TableKVEncoder) SQL2KV(sql string) ([]kvec.KvPair, uint64, error) {
	// via sql execution
	kvPairs, rowsAffected, err := kvcodec.encoder.Encode(sql, kvcodec.tableID)
	if err != nil {
		common.AppLogger.Errorf("[sql2kv] sql encode error = %v", err)
		return nil, 0, errors.Trace(err)
	}

	return kvPairs, rowsAffected, nil
}
