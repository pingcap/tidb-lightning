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
	"math/rand"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	// Import tidb/planner/core to initialize expression.RewriteAstExpr
	_ "github.com/pingcap/tidb/planner/core"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var extraHandleColumnInfo = model.NewExtraHandleColInfo()

type genCol struct {
	index int
	expr  expression.Expression
}

type tableKVEncoder struct {
	tbl         table.Table
	se          *session
	recordCache []types.Datum
	genCols     []genCol
	// auto random bits value for this chunk
	autoRandomHeaderBits int64
}

func NewTableKVEncoder(tbl table.Table, options *SessionOptions) (Encoder, error) {
	metric.KvEncoderCounter.WithLabelValues("open").Inc()
	meta := tbl.Meta()
	cols := tbl.Cols()
	se := newSession(options)
	// Set CommonAddRecordCtx to session to reuse the slices and BufStore in AddRecord
	recordCtx := tables.NewCommonAddRecordCtx(len(cols))
	tables.SetAddRecordCtx(se, recordCtx)

	var autoRandomBits int64
	if meta.PKIsHandle && meta.ContainsAutoRandomBits() {
		for _, col := range cols {
			if mysql.HasPriKeyFlag(col.Flag) {
				incrementalBits := autoRandomIncrementBits(col, int(meta.AutoRandomBits))
				autoRandomBits = rand.New(rand.NewSource(options.AutoRandomSeed)).Int63n(1<<meta.AutoRandomBits) << incrementalBits
				break
			}
		}
	}

	// collect expressions for evaluating stored generated columns
	genCols, err := collectGeneratedColumns(se, meta, cols)
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse generated column expressions")
	}

	return &tableKVEncoder{
		tbl:                  tbl,
		se:                   se,
		genCols:              genCols,
		autoRandomHeaderBits: autoRandomBits,
	}, nil
}

func autoRandomIncrementBits(col *table.Column, randomBits int) int {
	typeBitsLength := mysql.DefaultLengthOfMysqlTypes[col.Tp] * 8
	incrementalBits := typeBitsLength - randomBits
	hasSignBit := !mysql.HasUnsignedFlag(col.Flag)
	if hasSignBit {
		incrementalBits -= 1
	}
	return incrementalBits
}

// collectGeneratedColumns collects all expressions required to evaluate the
// results of all stored generated columns. The returning slice is in evaluation
// order.
func collectGeneratedColumns(se *session, meta *model.TableInfo, cols []*table.Column) ([]genCol, error) {
	maxGenColOffset := -1
	for _, col := range cols {
		if col.GeneratedStored && col.Offset > maxGenColOffset {
			maxGenColOffset = col.Offset
		}
	}

	if maxGenColOffset < 0 {
		return nil, nil
	}

	// the expression rewriter requires a non-nil TxnCtx.
	se.vars.TxnCtx = new(variable.TransactionContext)
	defer func() {
		se.vars.TxnCtx = nil
	}()

	// not using TableInfo2SchemaAndNames to avoid parsing all virtual generated columns again.
	exprColumns := make([]*expression.Column, 0, len(cols))
	names := make(types.NameSlice, 0, len(cols))
	for i, col := range cols {
		names = append(names, &types.FieldName{
			OrigTblName: meta.Name,
			OrigColName: col.Name,
			TblName:     meta.Name,
			ColName:     col.Name,
		})
		exprColumns = append(exprColumns, &expression.Column{
			RetType:  col.FieldType.Clone(),
			ID:       col.ID,
			UniqueID: int64(i),
			Index:    col.Offset,
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		})
	}
	schema := expression.NewSchema(exprColumns...)

	// as long as we have a stored generated column, all columns it referred to must be evaluated as well.
	// for simplicity we just evaluate all generated columns (virtual or not) before the last stored one.
	var genCols []genCol
	for i, col := range cols {
		if col.GeneratedExpr != nil && col.Offset <= maxGenColOffset {
			expr, err := expression.RewriteAstExpr(se, col.GeneratedExpr, schema, names)
			if err != nil {
				return nil, err
			}
			genCols = append(genCols, genCol{
				index: i,
				expr:  expr,
			})
		}
	}

	// order the result by column offset so they match the evaluation order.
	sort.Slice(genCols, func(i, j int) bool {
		return cols[genCols[i].index].Offset < cols[genCols[j].index].Offset
	})
	return genCols, nil
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
			enc.AddString("val", log.RedactString(str))
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

	log.L().Error("failed to covert kv value", log.ZapRedactReflect("origVal", original.GetValue()),
		zap.Stringer("fieldType", &colInfo.FieldType), zap.String("column", colInfo.Name.O),
		zap.Int("columnID", j+1))
	return errors.Annotatef(
		err,
		"failed to cast value as %s for column `%s` (#%d)", &colInfo.FieldType, colInfo.Name.O, j+1,
	)
}

func logEvalGenExprFailed(logger log.Logger, row []types.Datum, colInfo *model.ColumnInfo, err error) error {
	logger.Error("kv convert failed: cannot evaluate generated column expression",
		zap.Array("original", rowArrayMarshaler(row)),
		zap.String("colName", colInfo.Name.O),
		log.ShortError(err),
	)

	return errors.Annotatef(
		err,
		"failed to evaluate generated column expression for column `%s`",
		colInfo.Name.O,
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

	isAutoRandom := kvcodec.tbl.Meta().PKIsHandle && kvcodec.tbl.Meta().ContainsAutoRandomBits()
	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		isPk := mysql.HasPriKeyFlag(col.Flag)
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		} else if isAutoIncCol {
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		} else if isAutoRandom && isPk {
			var val types.Datum
			if mysql.HasUnsignedFlag(col.Flag) {
				val = types.NewUintDatum(uint64(kvcodec.autoRandomHeaderBits | rowID))
			} else {
				val = types.NewIntDatum(kvcodec.autoRandomHeaderBits | rowID)
			}
			value, err = table.CastValue(kvcodec.se, val, col.ToInfo(), false, false)
		} else if col.IsGenerated() {
			// inject some dummy value for gen col so that MutRowFromDatums below sees a real value instead of nil.
			// if MutRowFromDatums sees a nil it won't initialize the underlying storage and cause SetDatum to panic.
			value = types.GetMinValue(&col.FieldType)
		} else {
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, logKVConvertFailed(logger, row, j, col.ToInfo(), err)
		}

		record = append(record, value)

		if isAutoRandom && isPk {
			incrementalBits := autoRandomIncrementBits(col, int(kvcodec.tbl.Meta().AutoRandomBits))
			kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64()&((1<<incrementalBits)-1), false, autoid.AutoRandomType)
		}
		if isAutoIncCol {
			kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.AutoIncrementType)
		}
	}

	if common.TableHasAutoRowID(kvcodec.tbl.Meta()) {
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

	if len(kvcodec.genCols) > 0 {
		mutRow := chunk.MutRowFromDatums(record)
		for _, gc := range kvcodec.genCols {
			col := cols[gc.index].ToInfo()
			evaluated, err := gc.expr.Eval(mutRow.ToRow())
			if err != nil {
				return nil, logEvalGenExprFailed(logger, row, col, err)
			}
			value, err := table.CastValue(kvcodec.se, evaluated, col, false, false)
			if err != nil {
				return nil, logEvalGenExprFailed(logger, row, col, err)
			}
			mutRow.SetDatum(gc.index, value)
			record[gc.index] = value
		}
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
