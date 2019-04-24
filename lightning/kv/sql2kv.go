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
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

type rowArrayMarshaler []types.Datum

var kindStr = [...]string{
	types.KindNull:          "null",
	types.KindInt64:         "int64",
	types.KindUint64:        "uint64",
	types.KindFloat32:       "float32",
	types.KindFloat64:       "float64",
	types.KindString:        "string",
	types.KindBytes:         "bytes",
	types.KindBinaryLiteral: "binary",
	types.KindMysqlDecimal:  "decimal",
	types.KindMysqlDuration: "duration",
	types.KindMysqlEnum:     "enum",
	types.KindMysqlBit:      "bit",
	types.KindMysqlSet:      "set",
	types.KindMysqlTime:     "time",
	types.KindInterface:     "interface",
	types.KindMinNotNull:    "min",
	types.KindMaxValue:      "max",
	types.KindRaw:           "raw",
	types.KindMysqlJSON:     "json",
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row rowArrayMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for i, datum := range row {
		index := i
		kind := datum.Kind()
		var str string
		var err error
		switch kind {
		case types.KindNull:
			str = "NULL"
		case types.KindMinNotNull:
			str = "-inf"
		case types.KindMaxValue:
			str = "+inf"
		default:
			str, err = datum.ToString()
			if err != nil {
				return err
			}
		}
		encoder.AppendObject(zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			enc.AddInt("col", index)
			enc.AddString("kind", kindStr[kind])
			enc.AddString("val", str)
			return nil
		}))
	}
	return nil
}

func logKVConvertFailed(logger log.Logger, row []types.Datum, j int, colInfo *model.ColumnInfo, err error) {
	logger.Error("kv convert failed",
		zap.Array("originalRow", rowArrayMarshaler(row)),
		zap.Int("originalCol", j),
		zap.String("colName", colInfo.Name.O),
		zap.Stringer("colType", &colInfo.FieldType),
		log.ShortError(err),
	)
}

// Encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *TableKVEncoder) Encode(
	logger log.Logger,
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
		j := columnPermutation[i]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo())
			if err == nil {
				value, err = col.HandleBadNull(value, kvcodec.se.vars.StmtCtx)
			}
		} else if mysql.HasAutoIncrementFlag(col.Flag) {
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo())
		} else {
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			logKVConvertFailed(logger, row, j, col.ToInfo(), err)
			return nil, errors.Trace(err)
		}
		record = append(record, value)
	}

	if !kvcodec.tbl.Meta().PKIsHandle {
		j := columnPermutation[len(cols)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], extraHandleColumnInfo)
		} else {
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			logKVConvertFailed(logger, row, j, extraHandleColumnInfo, err)
			return nil, errors.Trace(err)
		}
		record = append(record, value)
	}

	_, err = kvcodec.tbl.AddRecord(kvcodec.se, record)
	if err != nil {
		logger.Error("kv encode failed",
			zap.Array("originalRow", rowArrayMarshaler(row)),
			zap.Array("convertedRow", rowArrayMarshaler(record)),
			log.ShortError(err),
		)
		return nil, errors.Trace(err)
	}

	pairs := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]

	return pairs, nil
}
