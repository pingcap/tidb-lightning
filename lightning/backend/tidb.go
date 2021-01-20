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
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var (
	extraHandleTableColumn = &table.Column{
		ColumnInfo:    extraHandleColumnInfo,
		GeneratedExpr: nil,
		DefaultExpr:   nil,
	}
)

type tidbRow string

type tidbRows []tidbRow

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row tidbRows) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range row {
		encoder.AppendString(string(r))
	}
	return nil
}

type tidbEncoder struct {
	mode mysql.SQLMode
	tbl  table.Table
	se   *session
	// the index of table columns for each data field.
	// index == len(table.columns) means this field is `_tidb_rowid`
	columnIdx []int
	columnCnt int
}

type tidbBackend struct {
	db          *sql.DB
	onDuplicate string
}

// NewTiDBBackend creates a new TiDB backend using the given database.
//
// The backend does not take ownership of `db`. Caller should close `db`
// manually after the backend expired.
func NewTiDBBackend(db *sql.DB, onDuplicate string) Backend {
	switch onDuplicate {
	case config.ReplaceOnDup, config.IgnoreOnDup, config.ErrorOnDup:
	default:
		log.L().Warn("unsupported action on duplicate, overwrite with `replace`")
		onDuplicate = config.ReplaceOnDup
	}
	return MakeBackend(&tidbBackend{db: db, onDuplicate: onDuplicate})
}

func (row tidbRow) ClassifyAndAppend(data *Rows, checksum *verification.KVChecksum, _ *Rows, _ *verification.KVChecksum) {
	rows := (*data).(tidbRows)
	*data = tidbRows(append(rows, row))
	cs := verification.MakeKVChecksum(uint64(len(row)), 1, 0)
	checksum.Add(&cs)
}

func (rows tidbRows) SplitIntoChunks(splitSize int) []Rows {
	if len(rows) == 0 {
		return nil
	}

	res := make([]Rows, 0, 1)
	i := 0
	cumSize := 0

	for j, row := range rows {
		if i < j && cumSize+len(row) > splitSize {
			res = append(res, rows[i:j])
			i = j
			cumSize = 0
		}
		cumSize += len(row)
	}

	return append(res, rows[i:])
}

func (rows tidbRows) Clear() Rows {
	return rows[:0]
}

func (enc *tidbEncoder) appendSQLBytes(sb *strings.Builder, value []byte) {
	sb.Grow(2 + len(value))
	sb.WriteByte('\'')
	if enc.mode.HasNoBackslashEscapesMode() {
		for _, b := range value {
			if b == '\'' {
				sb.WriteString(`''`)
			} else {
				sb.WriteByte(b)
			}
		}
	} else {
		for _, b := range value {
			switch b {
			case 0:
				sb.WriteString(`\0`)
			case '\b':
				sb.WriteString(`\b`)
			case '\n':
				sb.WriteString(`\n`)
			case '\r':
				sb.WriteString(`\r`)
			case '\t':
				sb.WriteString(`\t`)
			case 26:
				sb.WriteString(`\Z`)
			case '\'':
				sb.WriteString(`''`)
			case '\\':
				sb.WriteString(`\\`)
			default:
				sb.WriteByte(b)
			}
		}
	}
	sb.WriteByte('\'')
}

// appendSQL appends the SQL representation of the Datum into the string builder.
// Note that we cannot use Datum.ToString since it doesn't perform SQL escaping.
func (enc *tidbEncoder) appendSQL(sb *strings.Builder, datum *types.Datum, col *table.Column) error {
	switch datum.Kind() {
	case types.KindNull:
		sb.WriteString("NULL")

	case types.KindMinNotNull:
		sb.WriteString("MINVALUE")

	case types.KindMaxValue:
		sb.WriteString("MAXVALUE")

	case types.KindInt64:
		// longest int64 = -9223372036854775808 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendInt(buffer[:0], datum.GetInt64(), 10)
		sb.Write(value)

	case types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet:
		// longest uint64 = 18446744073709551615 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendUint(buffer[:0], datum.GetUint64(), 10)
		sb.Write(value)

	case types.KindFloat32, types.KindFloat64:
		// float64 has 16 digits of precision, so a buffer size of 32 is more than enough...
		var buffer [32]byte
		value := strconv.AppendFloat(buffer[:0], datum.GetFloat64(), 'g', -1, 64)
		sb.Write(value)
	case types.KindString:
		// See: https://github.com/pingcap/tidb-lightning/issues/550
		//if enc.mode.HasStrictMode() {
		//	d, err := table.CastValue(enc.se, *datum, col.ToInfo(), false, false)
		//	if err != nil {
		//		return errors.Trace(err)
		//	}
		//	datum = &d
		//}

		enc.appendSQLBytes(sb, datum.GetBytes())
	case types.KindBytes:
		enc.appendSQLBytes(sb, datum.GetBytes())

	case types.KindMysqlJSON:
		value, err := datum.GetMysqlJSON().MarshalJSON()
		if err != nil {
			return err
		}
		enc.appendSQLBytes(sb, value)

	case types.KindBinaryLiteral:
		value := datum.GetBinaryLiteral()
		sb.Grow(3 + 2*len(value))
		sb.WriteString("x'")
		hex.NewEncoder(sb).Write(value)
		sb.WriteByte('\'')

	case types.KindMysqlBit:
		var buffer [20]byte
		intValue, err := datum.GetBinaryLiteral().ToInt(nil)
		if err != nil {
			return err
		}
		value := strconv.AppendUint(buffer[:0], intValue, 10)
		sb.Write(value)

		// time, duration, decimal
	default:
		value, err := datum.ToString()
		if err != nil {
			return err
		}
		sb.WriteByte('\'')
		sb.WriteString(value)
		sb.WriteByte('\'')
	}

	return nil
}

func (*tidbEncoder) Close() {}

func getColumnByIndex(cols []*table.Column, index int) *table.Column {
	if index == len(cols) {
		return extraHandleTableColumn
	}
	return cols[index]
}

func (enc *tidbEncoder) Encode(logger log.Logger, row []types.Datum, _ int64, columnPermutation []int) (Row, error) {
	cols := enc.tbl.Cols()

	if len(enc.columnIdx) == 0 {
		columnCount := 0
		columnIdx := make([]int, len(columnPermutation))
		for i, idx := range columnPermutation {
			if idx >= 0 {
				columnIdx[idx] = i
				columnCount++
			}
		}
		enc.columnIdx = columnIdx
		enc.columnCnt = columnCount
	}

	// TODO: since the column count doesn't exactly reflect the real column names, we only check the upper bound currently.
	// See: tests/generated_columns/data/gencol.various_types.0.sql this sql has no columns, so encodeLoop will fill the
	// column permutation with default, thus enc.columnCnt > len(row).
	if len(row) > enc.columnCnt {
		logger.Error("column count mismatch", zap.Ints("column_permutation", columnPermutation),
			zap.Array("data", rowArrayMarshaler(row)))
		return nil, errors.Errorf("column count mismatch, expected %d, got %d", enc.columnCnt, len(row))
	}

	var encoded strings.Builder
	encoded.Grow(8 * len(row))
	encoded.WriteByte('(')
	for i, field := range row {
		if i != 0 {
			encoded.WriteByte(',')
		}
		if err := enc.appendSQL(&encoded, &field, getColumnByIndex(cols, enc.columnIdx[i])); err != nil {
			logger.Error("tidb encode failed",
				zap.Array("original", rowArrayMarshaler(row)),
				zap.Int("originalCol", i),
				log.ShortError(err),
			)
			return nil, err
		}
	}
	encoded.WriteByte(')')
	return tidbRow(encoded.String()), nil
}

func (be *tidbBackend) Close() {
	// *Not* going to close `be.db`. The db object is normally borrowed from a
	// TidbManager, so we let the manager to close it.
}

func (be *tidbBackend) MakeEmptyRows() Rows {
	return tidbRows(nil)
}

func (be *tidbBackend) RetryImportDelay() time.Duration {
	return 0
}

func (be *tidbBackend) MaxChunkSize() int {
	failpoint.Inject("FailIfImportedSomeRows", func() {
		failpoint.Return(1)
	})
	return 1048576
}

func (be *tidbBackend) ShouldPostProcess() bool {
	return false
}

func (be *tidbBackend) CheckRequirements(ctx context.Context) error {
	log.L().Info("skipping check requirements for tidb backend")
	return nil
}

func (be *tidbBackend) NewEncoder(tbl table.Table, options *SessionOptions) (Encoder, error) {
	se := newSession(options)
	if options.SQLMode.HasStrictMode() {
		se.vars.SkipUTF8Check = false
		se.vars.SkipASCIICheck = false
	}

	return &tidbEncoder{mode: options.SQLMode, tbl: tbl, se: se}, nil
}

func (be *tidbBackend) OpenEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) CloseEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) CleanupEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) ImportEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) WriteRows(ctx context.Context, _ uuid.UUID, tableName string, columnNames []string, _ uint64, rows Rows) error {
	var err error
outside:
	for _, r := range rows.SplitIntoChunks(be.MaxChunkSize()) {
		for i := 0; i < maxRetryTimes; i++ {
			err = be.WriteRowsToDB(ctx, tableName, columnNames, r)
			switch {
			case err == nil:
				continue outside
			case common.IsRetryableError(err):
				// retry next loop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] write rows reach max retry %d and still failed", tableName, maxRetryTimes)
	}
	return nil
}

func (be *tidbBackend) WriteRowsToDB(ctx context.Context, tableName string, columnNames []string, r Rows) error {
	rows := r.(tidbRows)
	if len(rows) == 0 {
		return nil
	}

	var insertStmt strings.Builder
	switch be.onDuplicate {
	case config.ReplaceOnDup:
		insertStmt.WriteString("REPLACE INTO ")
	case config.IgnoreOnDup:
		insertStmt.WriteString("INSERT IGNORE INTO ")
	case config.ErrorOnDup:
		insertStmt.WriteString("INSERT INTO ")
	}

	insertStmt.WriteString(tableName)
	if len(columnNames) > 0 {
		insertStmt.WriteByte('(')
		for i, colName := range columnNames {
			if i != 0 {
				insertStmt.WriteByte(',')
			}
			common.WriteMySQLIdentifier(&insertStmt, colName)
		}
		insertStmt.WriteByte(')')
	}
	insertStmt.WriteString(" VALUES")

	// Note: we are not going to do interpolation (prepared statements) to avoid
	// complication arise from data length overflow of BIT and BINARY columns

	for i, row := range rows {
		if i != 0 {
			insertStmt.WriteByte(',')
		}
		insertStmt.WriteString(string(row))
	}

	// Retry will be done externally, so we're not going to retry here.
	_, err := be.db.ExecContext(ctx, insertStmt.String())
	if err != nil {
		log.L().Error("execute statement failed", log.ZapRedactString("stmt", insertStmt.String()),
			log.ZapRedactArray("rows", rows), zap.Error(err))
	}
	failpoint.Inject("FailIfImportedSomeRows", func() {
		panic("forcing failure due to FailIfImportedSomeRows, before saving checkpoint")
	})
	return errors.Trace(err)
}

func (be *tidbBackend) FetchRemoteTableModels(ctx context.Context, schemaName string) (tables []*model.TableInfo, err error) {
	s := common.SQLWithRetry{
		DB:     be.db,
		Logger: log.L(),
	}

	err = s.Transact(ctx, "fetch table columns", func(c context.Context, tx *sql.Tx) error {
		var versionStr string
		if err = tx.QueryRowContext(ctx, "SELECT version()").Scan(&versionStr); err != nil {
			return err
		}
		tidbVersion, err := common.ExtractTiDBVersion(versionStr)
		if err != nil {
			return err
		}

		rows, e := tx.Query(`
			SELECT table_name, column_name, column_type, extra
			FROM information_schema.columns
			WHERE table_schema = ?
			ORDER BY table_name, ordinal_position;
		`, schemaName)
		if e != nil {
			return e
		}
		defer rows.Close()

		var (
			curTableName string
			curColOffset int
			curTable     *model.TableInfo
		)
		for rows.Next() {
			var tableName, columnName, columnType, columnExtra string
			if e := rows.Scan(&tableName, &columnName, &columnType, &columnExtra); e != nil {
				return e
			}
			if tableName != curTableName {
				curTable = &model.TableInfo{
					Name:       model.NewCIStr(tableName),
					State:      model.StatePublic,
					PKIsHandle: true,
				}
				tables = append(tables, curTable)
				curTableName = tableName
				curColOffset = 0
			}

			// see: https://github.com/pingcap/parser/blob/3b2fb4b41d73710bc6c4e1f4e8679d8be6a4863e/types/field_type.go#L185-L191
			var flag uint
			if strings.HasSuffix(columnType, "unsigned") {
				flag |= mysql.UnsignedFlag
			}
			if strings.Contains(columnExtra, "auto_increment") {
				flag |= mysql.AutoIncrementFlag
			}
			curTable.Columns = append(curTable.Columns, &model.ColumnInfo{
				Name:   model.NewCIStr(columnName),
				Offset: curColOffset,
				State:  model.StatePublic,
				FieldType: types.FieldType{
					Flag: flag,
				},
			})
			curColOffset++
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		// for version < v4.0.0 we can use `show table next_row_id` to fetch auto id info, so about should be enough
		if tidbVersion.Major < 4 {
			return nil
		}
		// init auto id column for each table
		for _, tbl := range tables {
			tblName := common.UniqueTable(schemaName, tbl.Name.O)
			rows, e = tx.Query(fmt.Sprintf("SHOW TABLE %s NEXT_ROW_ID", tblName))
			if e != nil {
				return e
			}
			for rows.Next() {
				var (
					dbName, tblName, columnName, idType string
					nextID                              int64
				)
				columns, err := rows.Columns()
				if err != nil {
					return err
				}

				//+--------------+------------+-------------+--------------------+----------------+
				//| DB_NAME      | TABLE_NAME | COLUMN_NAME | NEXT_GLOBAL_ROW_ID | ID_TYPE        |
				//+--------------+------------+-------------+--------------------+----------------+
				//| testsysbench | t          | _tidb_rowid |                  1 | AUTO_INCREMENT |
				//+--------------+------------+-------------+--------------------+----------------+

				// if columns length is 4, it doesn't contains the last column `ID_TYPE`, and it will always be 'AUTO_INCREMENT'
				// for v4.0.0~v4.0.2 show table t next_row_id only returns 4 columns.
				if len(columns) == 4 {
					err = rows.Scan(&dbName, &tblName, &columnName, &nextID)
					idType = "AUTO_INCREMENT"
				} else {
					err = rows.Scan(&dbName, &tblName, &columnName, &nextID, &idType)
				}
				if err != nil {
					return err
				}

				for _, col := range tbl.Columns {
					if col.Name.O == columnName {
						switch idType {
						case "AUTO_INCREMENT":
							col.Flag |= mysql.AutoIncrementFlag
						case "AUTO_RANDOM":
							col.Flag |= mysql.PriKeyFlag
							tbl.PKIsHandle = true
							// set a stub here, since we don't really need the real value
							tbl.AutoRandomBits = 1
						}
					}
				}
			}
			rows.Close()
			if rows.Err() != nil {
				return rows.Err()
			}
		}
		return nil
	})
	return
}

func (be *tidbBackend) EngineFileSizes() []EngineFileSize {
	return nil
}

func (be *tidbBackend) FlushEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) FlushAllEngines(context.Context) error {
	return nil
}

func (be *tidbBackend) ResetEngine(context.Context, uuid.UUID) error {
	return errors.New("cannot reset an engine in TiDB backend")
}

func (be *tidbBackend) LocalWriter(ctx context.Context, engineUUID uuid.UUID, maxCacheSize int64) (EngineWriter, error) {
	return &TiDBWriter{be: be, engineUUID: engineUUID}, nil
}

type TiDBWriter struct {
	be         *tidbBackend
	engineUUID uuid.UUID
}

func (w *TiDBWriter) Close() error {
	return nil
}

func (w *TiDBWriter) AppendRows(ctx context.Context, tableName string, columnNames []string, arg1 uint64, rows Rows) error {
	return w.be.WriteRows(ctx, w.engineUUID, tableName, columnNames, arg1, rows)
}
