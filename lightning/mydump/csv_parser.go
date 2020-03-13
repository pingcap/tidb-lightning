// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/worker"
	"github.com/pingcap/tidb/types"
)

var (
	errUnterminatedQuotedField = errors.NewNoStackError("syntax error: unterminated quoted field")
	errDanglingBackslash       = errors.NewNoStackError("syntax error: no character after backslash")
	errUnexpectedQuoteField    = errors.NewNoStackError("syntax error: cannot have consecutive fields without separator")
)

// CSVParser is basically a copy of encoding/csv, but special-cased for MySQL-like input.
type CSVParser struct {
	blockParser
	cfg       *config.CSVConfig
	escFlavor backslashEscapeFlavor

	comma          byte
	quote          byte
	quoteStopSet   string
	unquoteStopSet string

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The i'th field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes []int
}

func NewCSVParser(
	cfg *config.CSVConfig,
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) *CSVParser {
	quote := byte(0)
	if len(cfg.Delimiter) > 0 {
		quote = cfg.Delimiter[0]
	}

	escFlavor := backslashEscapeFlavorNone
	quoteStopSet := cfg.Delimiter
	unquoteStopSet := "\r\n" + cfg.Separator + cfg.Delimiter
	if cfg.BackslashEscape {
		escFlavor = backslashEscapeFlavorMySQL
		quoteStopSet += `\`
		unquoteStopSet += `\`
		// we need special treatment of the NULL value \N, used by MySQL.
		if !cfg.NotNull && cfg.Null == `\N` {
			escFlavor = backslashEscapeFlavorMySQLWithNull
		}
	}

	return &CSVParser{
		blockParser:    makeBlockParser(reader, blockBufSize, ioWorkers),
		cfg:            cfg,
		comma:          cfg.Separator[0],
		quote:          quote,
		escFlavor:      escFlavor,
		quoteStopSet:   quoteStopSet,
		unquoteStopSet: unquoteStopSet,
	}
}

func (parser *CSVParser) unescapeString(input string) (unescaped string, isNull bool) {
	if parser.escFlavor == backslashEscapeFlavorMySQLWithNull && input == `\N` {
		return input, true
	}
	unescaped = unescape(input, "", parser.escFlavor)
	isNull = parser.escFlavor != backslashEscapeFlavorMySQLWithNull &&
		!parser.cfg.NotNull &&
		unescaped == parser.cfg.Null
	return
}

func (parser *CSVParser) readByte() (byte, error) {
	if len(parser.buf) == 0 {
		if err := parser.readBlock(); err != nil {
			return 0, err
		}
	}
	if len(parser.buf) == 0 {
		return 0, io.EOF
	}
	b := parser.buf[0]
	parser.buf = parser.buf[1:]
	parser.pos++
	return b, nil
}

func (parser *CSVParser) peekByte() (byte, error) {
	if len(parser.buf) == 0 {
		if err := parser.readBlock(); err != nil {
			return 0, err
		}
	}
	if len(parser.buf) == 0 {
		return 0, io.EOF
	}
	return parser.buf[0], nil
}

func (parser *CSVParser) skipByte() {
	parser.buf = parser.buf[1:]
	parser.pos++
}

// readUntil reads the buffer until any character from the `chars` set is found.
// that character is excluded from the final buffer.
func (parser *CSVParser) readUntil(chars string) ([]byte, byte, error) {
	index := bytes.IndexAny(parser.buf, chars)
	if index >= 0 {
		ret := parser.buf[:index]
		parser.buf = parser.buf[index:]
		parser.pos += int64(index)
		return ret, parser.buf[0], nil
	}

	// not found in parser.buf, need allocate and loop.
	var buf []byte
	for {
		buf = append(buf, parser.buf...)
		parser.buf = nil
		if err := parser.readBlock(); err != nil || len(parser.buf) == 0 {
			if err == nil {
				err = io.EOF
			}
			parser.pos += int64(len(buf))
			return buf, 0, errors.Trace(err)
		}
		index := bytes.IndexAny(parser.buf, chars)
		if index >= 0 {
			buf = append(buf, parser.buf[:index]...)
			parser.buf = parser.buf[index:]
			parser.pos += int64(len(buf))
			return buf, parser.buf[0], nil
		}
	}
}

func (parser *CSVParser) readRecord() ([]string, error) {
	parser.recordBuffer = parser.recordBuffer[:0]
	parser.fieldIndexes = parser.fieldIndexes[:0]

	isEmptyLine := true
outside:
	for {
		firstByte, err := parser.readByte()
		if err != nil {
			if isEmptyLine || errors.Cause(err) != io.EOF {
				return nil, err
			}
			// treat EOF as the same as trailing \n.
			firstByte = '\n'
		}

		switch firstByte {
		case parser.comma:
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))

		case parser.quote:
			if err := parser.readQuotedField(); err != nil {
				return nil, err
			}

		case '\r', '\n':
			// new line = end of record (ignore empty lines)
			if isEmptyLine {
				continue
			}
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
			break outside

		default:
			if firstByte == '\\' && parser.escFlavor != backslashEscapeFlavorNone {
				if err := parser.readByteForBackslashEscape(); err != nil {
					return nil, err
				}
			} else {
				parser.recordBuffer = append(parser.recordBuffer, firstByte)
			}
			if err := parser.readUnquoteField(); err != nil {
				return nil, err
			}
		}
		isEmptyLine = false
	}

	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(parser.recordBuffer) // Convert to string once to batch allocations
	dst := make([]string, len(parser.fieldIndexes))
	var preIdx int
	for i, idx := range parser.fieldIndexes {
		dst[i] = str[preIdx:idx]
		preIdx = idx
	}

	// Check or update the expected fields per record.
	return dst, nil
}

func (parser *CSVParser) readByteForBackslashEscape() error {
	b, err := parser.readByte()
	err = parser.replaceEOF(err, errDanglingBackslash)
	if err != nil {
		return err
	}
	parser.recordBuffer = append(parser.recordBuffer, '\\', b)
	return nil
}

func (parser *CSVParser) readQuotedField() error {
	for {
		content, terminator, err := parser.readUntil(parser.quoteStopSet)
		err = parser.replaceEOF(err, errUnterminatedQuotedField)
		if err != nil {
			return err
		}
		parser.recordBuffer = append(parser.recordBuffer, content...)
		parser.skipByte()
		switch terminator {
		case parser.quote:
			// encountered '"' -> continue if we're seeing '""'.
			b, err := parser.peekByte()
			err = parser.replaceEOF(err, nil)
			if err != nil {
				return err
			}
			switch b {
			case parser.quote:
				// consume the double quotation mark and continue
				parser.skipByte()
				parser.recordBuffer = append(parser.recordBuffer, '"')
			case '\r', '\n', parser.comma, 0:
				// end the field if the next is a separator
				return nil
			default:
				// in all other cases, we've got a syntax error.
				parser.logSyntaxError()
				return errors.AddStack(errUnexpectedQuoteField)
			}
		case '\\':
			if err := parser.readByteForBackslashEscape(); err != nil {
				return err
			}
		}
	}
}

func (parser *CSVParser) readUnquoteField() error {
	for {
		content, terminator, err := parser.readUntil(parser.unquoteStopSet)
		parser.recordBuffer = append(parser.recordBuffer, content...)
		err = parser.replaceEOF(err, nil)
		if err != nil {
			return err
		}

		switch terminator {
		case '\r', '\n', parser.comma, 0:
			return nil
		case parser.quote:
			parser.logSyntaxError()
			return errors.AddStack(errUnexpectedQuoteField)
		case '\\':
			parser.skipByte()
			if err := parser.readByteForBackslashEscape(); err != nil {
				return err
			}
		}
	}
}

func (parser *CSVParser) replaceEOF(err error, replaced error) error {
	if err == nil || errors.Cause(err) != io.EOF {
		return err
	}
	if replaced != nil {
		parser.logSyntaxError()
		replaced = errors.AddStack(replaced)
	}
	return replaced
}

// ReadRow reads a row from the datafile.
func (parser *CSVParser) ReadRow() error {
	row := &parser.lastRow
	row.RowID++

	// skip the header first
	if parser.pos == 0 && parser.cfg.Header {
		columns, err := parser.readRecord()
		if err != nil {
			return errors.Trace(err)
		}
		parser.columns = make([]string, 0, len(columns))
		for _, colName := range columns {
			colName, _ = parser.unescapeString(colName)
			parser.columns = append(parser.columns, strings.ToLower(colName))
		}
	}

	records, err := parser.readRecord()
	if err != nil {
		return errors.Trace(err)
	}
	// remove trailing empty values
	if parser.cfg.TrimLastSep {
		var i int
		for i = len(records); i > 0 && len(records[i-1]) == 0; i-- {
		}
		records = records[:i]
	}

	row.Row = parser.acquireDatumSlice()
	for _, record := range records {
		var datum types.Datum
		unescaped, isNull := parser.unescapeString(record)
		if isNull {
			datum.SetNull()
		} else {
			datum.SetString(unescaped)
		}
		row.Row = append(row.Row, datum)
	}

	return nil
}
