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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

var extraHandleColumnInfo = model.NewExtraHandleColInfo()

type TableKVEncoder struct {
	tbl         table.Table
	se          *session
	recordCache []types.Datum
}

func NewTableKVEncoder(
	tbl table.Table,
	sqlMode mysql.SQLMode,
) *TableKVEncoder {
	metric.KvEncoderCounter.WithLabelValues("open").Inc()

	return &TableKVEncoder{
		tbl: tbl,
		se:  newSession(sqlMode),
	}
}

func (kvcodec *TableKVEncoder) Close() {
	metric.KvEncoderCounter.WithLabelValues("closed").Inc()
}

// Encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *TableKVEncoder) Encode(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) ([]kvec.KvPair, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error
	var record []types.Datum

	if kvcodec.recordCache != nil {
		record = kvcodec.recordCache
	} else {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	for i, col := range cols {
		if j := columnPermutation[i]; j >= 0 {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo())
			if err == nil {
				value, err = col.HandleBadNull(value, kvcodec.se.vars.StmtCtx)
			}
		} else {
			value, err = table.GetColOriginDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		record = append(record, value)
	}

	if !kvcodec.tbl.Meta().PKIsHandle {
		if j := columnPermutation[len(cols)]; j >= 0 {
			value, err = table.CastValue(kvcodec.se, row[j], extraHandleColumnInfo)
		} else {
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		record = append(record, value)
	}

	_, err = kvcodec.tbl.AddRecord(kvcodec.se, record)
	pairs := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]

	return pairs, errors.Trace(err)
}
