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

package mydump

import (
	"bytes"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/worker"
	"github.com/pingcap/tidb/types"
)

type blockParser struct {
	// states for the lexer
	reader      io.Reader
	buf         []byte
	blockBuf    []byte
	isLastChunk bool

	// The list of column names of the last INSERT statement.
	// Assumed to be constant throughout the entire file.
	columns []string

	lastRow Row
	// Current file offset.
	pos int64

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer
	ioWorkers *worker.Pool
}

func makeBlockParser(reader io.Reader, blockBufSize int64, ioWorkers *worker.Pool) blockParser {
	return blockParser{
		reader:    reader,
		blockBuf:  make([]byte, blockBufSize*config.BufferSizeScale),
		remainBuf: &bytes.Buffer{},
		appendBuf: &bytes.Buffer{},
		ioWorkers: ioWorkers,
	}
}

// ChunkParser is a parser of the data files (the file containing only INSERT
// statements).
type ChunkParser struct {
	blockParser

	BackslashEscape bool
}

// Chunk represents a portion of the data file.
type Chunk struct {
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
}

// Row is the content of a row.
type Row struct {
	RowID int64
	Row   []types.Datum
}

type Parser interface {
	Pos() (pos int64, rowID int64)
	SetPos(pos int64, rowID int64)
	Close() error
	ReadRow() error
	LastRow() Row
	Columns() []string
}

// NewChunkParser creates a new parser which can read chunks out of a file.
func NewChunkParser(
	sqlMode mysql.SQLMode,
	reader io.Reader,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) *ChunkParser {
	return &ChunkParser{
		blockParser:     makeBlockParser(reader, blockBufSize, ioWorkers),
		BackslashEscape: !sqlMode.HasNoBackslashEscapesMode(),
	}
}

// Reader returns the underlying reader of this parser.
func (parser *blockParser) Reader() io.Reader {
	return parser.reader
}

// SetPos changes the reported position and row ID.
func (parser *blockParser) SetPos(pos int64, rowID int64) {
	parser.pos = pos
	parser.lastRow.RowID = rowID
}

// Pos returns the current file offset.
func (parser *blockParser) Pos() (int64, int64) {
	return parser.pos, parser.lastRow.RowID
}

func (parser *blockParser) Close() error {
	if closer, ok := parser.reader.(io.Closer); ok {
		return closer.Close()
	}
	return errors.New("this parser is not created with a reader that can be closed")
}

func (parser *blockParser) Columns() []string {
	return parser.columns
}

type token byte

const (
	tokNil token = iota
	tokRowBegin
	tokRowEnd
	tokValues
	tokNull
	tokTrue
	tokFalse
	tokHexString
	tokBinString
	tokSingleQuoted
	tokDoubleQuoted
	tokBackQuoted
	tokUnquoted
)

func (parser *blockParser) readBlock() error {
	startTime := time.Now()

	// limit IO concurrency
	w := parser.ioWorkers.Apply()
	n, err := io.ReadFull(parser.reader, parser.blockBuf)
	parser.ioWorkers.Recycle(w)

	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		parser.isLastChunk = true
		fallthrough
	case nil:
		// `parser.buf` reference to `appendBuf.Bytes`, so should use remainBuf to
		// hold the `parser.buf` rest data to prevent slice overlap
		parser.remainBuf.Reset()
		parser.remainBuf.Write(parser.buf)
		parser.appendBuf.Reset()
		parser.appendBuf.Write(parser.remainBuf.Bytes())
		parser.appendBuf.Write(parser.blockBuf[:n])
		parser.buf = parser.appendBuf.Bytes()
		metric.ChunkParserReadBlockSecondsHistogram.Observe(time.Since(startTime).Seconds())
		return nil
	default:
		return errors.Trace(err)
	}
}

var unescapeRegexp = regexp.MustCompile(`\\.`)

func unescape(input string, delim string, backslashEscape bool) string {
	delim2 := delim + delim
	if strings.Index(input, delim2) != -1 {
		input = strings.Replace(input, delim2, delim, -1)
	}
	if backslashEscape && strings.IndexByte(input, '\\') != -1 {
		input = unescapeRegexp.ReplaceAllStringFunc(input, func(substr string) string {
			switch substr[1] {
			case '0':
				return "\x00"
			case 'b':
				return "\b"
			case 'n':
				return "\n"
			case 'r':
				return "\r"
			case 't':
				return "\t"
			case 'Z':
				return "\x26"
			default:
				return substr[1:]
			}
		})
	}
	return input
}

func (parser *ChunkParser) unescapeString(input string) string {
	if len(input) >= 2 {
		switch input[0] {
		case '\'', '"':
			return unescape(input[1:len(input)-1], input[:1], parser.BackslashEscape)
		case '`':
			return unescape(input[1:len(input)-1], "`", parser.BackslashEscape)
		}
	}
	return strings.ToLower(input)
}

// ReadRow reads a row from the datafile.
func (parser *ChunkParser) ReadRow() error {
	// This parser will recognize contents like:
	//
	// 		`tableName` (...) VALUES (...) (...) (...)
	//
	// Keywords like INSERT, INTO and separators like ',' and ';' are treated
	// like comments and ignored. Therefore, this parser will accept some
	// nonsense input. The advantage is the parser becomes extremely simple,
	// suitable for us where we just want to quickly and accurately split the
	// file apart, not to validate the content.

	type state byte

	const (
		// the state after "INSERT INTO" before the column names or "VALUES"
		stateTableName state = iota

		// the state while reading the column names
		stateColumns

		// the state after reading "VALUES"
		stateValues

		// the state while reading row values
		stateRow
	)

	row := &parser.lastRow
	st := stateValues

	for {
		tok, content, err := parser.lex()
		if err != nil {
			return errors.Trace(err)
		}

		switch st {
		case stateTableName:
			switch tok {
			case tokRowBegin:
				st = stateColumns
			case tokValues:
				st = stateValues
			}
		case stateColumns:
			switch tok {
			case tokRowEnd:
				st = stateValues
			case tokUnquoted, tokDoubleQuoted, tokBackQuoted:
				columnName := parser.unescapeString(string(content))
				parser.columns = append(parser.columns, columnName)
			}
		case stateValues:
			switch tok {
			case tokRowBegin:
				row.RowID++
				row.Row = make([]types.Datum, 0, len(row.Row))
				st = stateRow
			case tokUnquoted, tokDoubleQuoted, tokBackQuoted:
				parser.columns = nil
				st = stateTableName
			}
		case stateRow:
			var value types.Datum
			switch tok {
			case tokRowEnd:
				return nil
			case tokNull:
				value.SetNull()
			case tokTrue:
				value.SetInt64(1)
			case tokFalse:
				value.SetInt64(0)
			case tokUnquoted, tokSingleQuoted, tokDoubleQuoted:
				value.SetString(parser.unescapeString(string(content)))
			case tokHexString:
				hexLit, err := types.ParseHexStr(string(content))
				if err != nil {
					return err
				}
				value.SetBinaryLiteral(hexLit)
			case tokBinString:
				binLit, err := types.ParseBitStr(string(content))
				if err != nil {
					return err
				}
				value.SetBinaryLiteral(binLit)
			default:
				return errors.Errorf("Syntax error at position %d", parser.pos)
			}
			row.Row = append(row.Row, value)
		}
	}
}

// LastRow is the copy of the row parsed by the last call to ReadRow().
func (parser *blockParser) LastRow() Row {
	return parser.lastRow
}

// ReadChunks parses the entire file and splits it into continuous chunks of
// size >= minSize.
func ReadChunks(parser Parser, minSize int64) ([]Chunk, error) {
	var chunks []Chunk

	pos, lastRowID := parser.Pos()
	cur := Chunk{
		Offset:       pos,
		EndOffset:    pos,
		PrevRowIDMax: lastRowID,
		RowIDMax:     lastRowID,
	}

	for {
		switch err := parser.ReadRow(); errors.Cause(err) {
		case nil:
			cur.EndOffset, cur.RowIDMax = parser.Pos()
			if cur.EndOffset-cur.Offset >= minSize {
				chunks = append(chunks, cur)
				cur.Offset = cur.EndOffset
				cur.PrevRowIDMax = cur.RowIDMax
			}

		case io.EOF:
			if cur.Offset < cur.EndOffset {
				chunks = append(chunks, cur)
			}
			return chunks, nil

		default:
			return nil, errors.Trace(err)
		}
	}
}
