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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

type tidbRow string

type tidbRows []tidbRow

type tidbEncoder struct {
	mode mysql.SQLMode
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

func (enc tidbEncoder) appendSQLBytes(sb *strings.Builder, value []byte) {
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
func (enc tidbEncoder) appendSQL(sb *strings.Builder, datum *types.Datum) error {
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

	case types.KindString, types.KindBytes:
		enc.appendSQLBytes(sb, datum.GetBytes())

	case types.KindMysqlJSON:
		value, err := datum.GetMysqlJSON().MarshalJSON()
		if err != nil {
			return err
		}
		enc.appendSQLBytes(sb, value)

	case types.KindBinaryLiteral:
		value := datum.GetBinaryLiteral()
		sb.Grow(2 + 2*len(value))
		sb.WriteString("0x")
		hex.NewEncoder(sb).Write(value)

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

func (tidbEncoder) Close() {}

func (enc tidbEncoder) Encode(logger log.Logger, row []types.Datum, _ int64, _ []int) (Row, error) {
	var encoded strings.Builder
	encoded.Grow(8 * len(row))
	encoded.WriteByte('(')
	for i, field := range row {
		if i != 0 {
			encoded.WriteByte(',')
		}
		if err := enc.appendSQL(&encoded, &field); err != nil {
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

func (be *tidbBackend) Flush(engineId uuid.UUID) error {
	return nil
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

func (be *tidbBackend) NewEncoder(_ table.Table, options *SessionOptions) Encoder {
	return tidbEncoder{mode: options.SQLMode}
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

func (be *tidbBackend) WriteRows(ctx context.Context, _ uuid.UUID, tableName string, columnNames []string, _ uint64, r Rows) error {
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
	// complication arised from data Length overflow of BIT and BINARY columns

	for i, row := range rows {
		if i != 0 {
			insertStmt.WriteByte(',')
		}
		insertStmt.WriteString(string(row))
	}

	// Retry will be done externally, so we're not going to retry here.
	_, err := be.db.ExecContext(ctx, insertStmt.String())
	failpoint.Inject("FailIfImportedSomeRows", func() {
		panic("forcing failure due to FailIfImportedSomeRows, before saving checkpoint")
	})
	return err
}
