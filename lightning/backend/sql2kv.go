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

package backend

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var extraHandleColumnInfo = model.NewExtraHandleColInfo()

type tableKVEncoder struct {
	tbl         table.Table
	se          *session
	recordCache []types.Datum
}

func NewTableKVEncoder(tbl table.Table, options *SessionOptions) Encoder {
	metric.KvEncoderCounter.WithLabelValues("open").Inc()

	return &tableKVEncoder{
		tbl: tbl,
		se:  newSession(options),
	}
}

func (kvcodec *tableKVEncoder) Close() {
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
	for _, datum := range row {
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
			enc.AddString("kind", kindStr[kind])
			enc.AddString("val", str)
			return nil
		}))
	}
	return nil
}

func logKVConvertFailed(logger log.Logger, row []types.Datum, j int, colInfo *model.ColumnInfo, err error) error {
	var original types.Datum
	if 0 <= j && j < len(row) {
		original = row[j]
		row = row[j : j+1]
	}

	logger.Error("kv convert failed",
		zap.Array("original", rowArrayMarshaler(row)),
		zap.Int("originalCol", j),
		zap.String("colName", colInfo.Name.O),
		zap.Stringer("colType", &colInfo.FieldType),
		log.ShortError(err),
	)

	return errors.Annotatef(
		err,
		"failed to cast `%v` as %s for column `%s` (#%d)",
		original.GetValue(), &colInfo.FieldType, colInfo.Name.O, j+1,
	)
}

type kvPairs []common.KvPair

// MakeRowsFromKvPairs converts a KvPair slice into a Rows instance. This is
// mainly used for testing only. The resulting Rows instance should only be used
// for the importer backend.
func MakeRowsFromKvPairs(pairs []common.KvPair) Rows {
	return kvPairs(pairs)
}

// MakeRowFromKvPairs converts a KvPair slice into a Row instance. This is
// mainly used for testing only. The resulting Row instance should only be used
// for the importer backend.
func MakeRowFromKvPairs(pairs []common.KvPair) Row {
	return kvPairs(pairs)
}

// Encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *tableKVEncoder) Encode(
	logger log.Logger,
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error
	var record []types.Datum

	if kvcodec.recordCache != nil {
		record = kvcodec.recordCache
	} else {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	isAutoRandom := false
	if kvcodec.tbl.Meta().PKIsHandle && !kvcodec.tbl.Meta().ContainsAutoRandomBits() {
		isAutoRandom = true
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		isPk := mysql.HasPriKeyFlag(col.Flag)
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				value, err = col.HandleBadNull(value, kvcodec.se.vars.StmtCtx)
			}
		} else if isAutoIncCol {
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		} else {
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, logKVConvertFailed(logger, row, j, col.ToInfo(), err)
		}
		record = append(record, value)
		if isAutoRandom && isPk {
			typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[mysql.TypeLonglong] * 8)
			incrementalBits := typeBitsLength - kvcodec.tbl.Meta().AutoRandomBits
			hasSignBit := !mysql.HasUnsignedFlag(col.Flag)
			if hasSignBit {
				incrementalBits -= 1
			}
			kvcodec.tbl.RebaseAutoID(kvcodec.se, int64(incrementalBits), false, autoid.AutoRandomType)
		}
		if isAutoIncCol {
			kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.AutoIncrementType)
		}
	}

	if !kvcodec.tbl.Meta().PKIsHandle {
		j := columnPermutation[len(cols)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], extraHandleColumnInfo, false, false)
		} else {
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, logKVConvertFailed(logger, row, j, extraHandleColumnInfo, err)
		}
		record = append(record, value)
		kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.RowIDAllocType)
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

	return kvPairs(pairs), nil
}

func (kvs kvPairs) ClassifyAndAppend(
	data *Rows,
	dataChecksum *verification.KVChecksum,
	indices *Rows,
	indexChecksum *verification.KVChecksum,
) {
	dataKVs := (*data).(kvPairs)
	indexKVs := (*indices).(kvPairs)

	for _, kv := range kvs {
		if kv.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
			dataKVs = append(dataKVs, kv)
			dataChecksum.UpdateOne(kv)
		} else {
			indexKVs = append(indexKVs, kv)
			indexChecksum.UpdateOne(kv)
		}
	}

	*data = dataKVs
	*indices = indexKVs
}

func (totalKVs kvPairs) SplitIntoChunks(splitSize int) []Rows {
	if len(totalKVs) == 0 {
		return nil
	}

	res := make([]Rows, 0, 1)
	i := 0
	cumSize := 0

	for j, pair := range totalKVs {
		size := len(pair.Key) + len(pair.Val)
		if i < j && cumSize+size > splitSize {
			res = append(res, kvPairs(totalKVs[i:j]))
			i = j
			cumSize = 0
		}
		cumSize += size
	}

	return append(res, kvPairs(totalKVs[i:]))
}

func (kvs kvPairs) Clear() Rows {
	return kvPairs(kvs[:0])
}
