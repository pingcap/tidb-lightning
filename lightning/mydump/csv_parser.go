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
	"unicode"

	"github.com/pingcap/br/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/worker"
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

	comma            []byte
	quote            []byte
	quoteIndexFunc   func([]byte) int
	unquoteIndexFunc func([]byte) int

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The i'th field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes []int

	lastRecord []string

	// if set to true, csv parser will treat the first non-empty line as header line
	shouldParseHeader bool
}

func NewCSVParser(
	cfg *config.CSVConfig,
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
	shouldParseHeader bool,
) *CSVParser {
	escFlavor := backslashEscapeFlavorNone
	var quoteStopSet []byte
	unquoteStopSet := []byte{'\r', '\n', cfg.Separator[0]}
	if len(cfg.Delimiter) > 0 {
		quoteStopSet = []byte{cfg.Delimiter[0]}
		unquoteStopSet = append(unquoteStopSet, cfg.Delimiter[0])
	}
	if cfg.BackslashEscape {
		escFlavor = backslashEscapeFlavorMySQL
		quoteStopSet = append(quoteStopSet, '\\')
		unquoteStopSet = append(unquoteStopSet, '\\')
		// we need special treatment of the NULL value \N, used by MySQL.
		if !cfg.NotNull && cfg.Null == `\N` {
			escFlavor = backslashEscapeFlavorMySQLWithNull
		}
	}

	return &CSVParser{
		blockParser:       makeBlockParser(reader, blockBufSize, ioWorkers),
		cfg:               cfg,
		comma:             []byte(cfg.Separator),
		quote:             []byte(cfg.Delimiter),
		escFlavor:         escFlavor,
		quoteIndexFunc:    makeBytesIndexFunc(quoteStopSet),
		unquoteIndexFunc:  makeBytesIndexFunc(unquoteStopSet),
		shouldParseHeader: shouldParseHeader,
	}
}

func makeBytesIndexFunc(chars []byte) func([]byte) int {
	// chars are guaranteed to be ascii str, so this call will always success
	as := makeByteSet(chars)
	return func(s []byte) int {
		return IndexAnyByte(s, &as)
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

func (parser *CSVParser) readBytes(buf []byte) (int, error) {
	cnt := 0
	for cnt < len(buf) {
		if len(parser.buf) == 0 {
			if err := parser.readBlock(); err != nil {
				return cnt, err
			}
		}
		if len(parser.buf) == 0 {
			parser.pos += int64(cnt)
			return cnt, io.EOF
		}
		readCnt := utils.MinInt(len(buf)-cnt, len(parser.buf))
		copy(buf[cnt:], parser.buf[:readCnt])
		parser.buf = parser.buf[readCnt:]
		cnt += readCnt
	}
	parser.pos += int64(cnt)
	return cnt, nil
}

func (parser *CSVParser) peekBytes(cnt int) ([]byte, error) {
	if len(parser.buf) < cnt {
		if err := parser.readBlock(); err != nil {
			return nil, err
		}
	}
	if len(parser.buf) == 0 {
		return nil, io.EOF
	}
	cnt = utils.MinInt(cnt, len(parser.buf))
	return parser.buf[:cnt], nil
}

func (parser *CSVParser) skipBytes(n int) {
	parser.buf = parser.buf[n:]
	parser.pos += int64(n)
}

// readUntil reads the buffer until any character from the `chars` set is found.
// that character is excluded from the final buffer.
func (parser *CSVParser) readUntil(findIndexFunc func([]byte) int) ([]byte, byte, error) {
	index := findIndexFunc(parser.buf)
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
		index := findIndexFunc(parser.buf)
		if index >= 0 {
			buf = append(buf, parser.buf[:index]...)
			parser.buf = parser.buf[index:]
			parser.pos += int64(len(buf))
			return buf, parser.buf[0], nil
		}
	}
}

func (parser *CSVParser) readRecord(dst []string) ([]string, error) {
	parser.recordBuffer = parser.recordBuffer[:0]
	parser.fieldIndexes = parser.fieldIndexes[:0]

	isEmptyLine := true
	whitespaceLine := true

	processDefault := func(b byte) error {
		if b == '\\' && parser.escFlavor != backslashEscapeFlavorNone {
			if err := parser.readByteForBackslashEscape(); err != nil {
				return err
			}
		} else {
			parser.recordBuffer = append(parser.recordBuffer, b)
		}
		return parser.readUnquoteField()
	}

	processQuote := func(b byte) error {
		return parser.readQuotedField()
	}
	if len(parser.quote) > 1 {
		processQuote = func(b byte) error {
			pb, err := parser.peekBytes(len(parser.quote) - 1)
			if err != nil && errors.Cause(err) != io.EOF {
				return err
			}
			if bytes.Equal(pb, parser.quote[1:]) {
				parser.skipBytes(len(parser.quote) - 1)
				return parser.readQuotedField()
			}
			return processDefault(b)
		}
	}

	processComma := func(b byte) error {
		parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
		return nil
	}
	if len(parser.comma) > 1 {
		processNotComma := processDefault
		if len(parser.quote) > 0 && parser.comma[0] == parser.quote[0] {
			processNotComma = processQuote
		}
		processComma = func(b byte) error {
			pb, err := parser.peekBytes(len(parser.comma) - 1)
			if err != nil && errors.Cause(err) != io.EOF {
				return err
			}
			if bytes.Equal(pb, parser.comma[1:]) {
				parser.skipBytes(len(parser.comma) - 1)
				parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
				return nil
			}
			return processNotComma(b)
		}
	}

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

		switch {
		case firstByte == parser.comma[0]:
			whitespaceLine = false
			if err = processComma(firstByte); err != nil {
				return nil, err
			}

		case len(parser.quote) > 0 && firstByte == parser.quote[0]:
			if err = processQuote(firstByte); err != nil {
				return nil, err
			}
			whitespaceLine = false
		case firstByte == '\r', firstByte == '\n':
			// new line = end of record (ignore empty lines)
			if isEmptyLine {
				continue
			}
			// skip lines only contain whitespaces
			if err == nil && whitespaceLine && len(bytes.TrimFunc(parser.recordBuffer, unicode.IsSpace)) == 0 {
				parser.recordBuffer = parser.recordBuffer[:0]
				continue
			}
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
			break outside
		default:
			if err = processDefault(firstByte); err != nil {
				return nil, err
			}
		}
		isEmptyLine = false
	}
	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(parser.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(parser.fieldIndexes) {
		dst = make([]string, len(parser.fieldIndexes))
	}
	dst = dst[:len(parser.fieldIndexes)]
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
	processDefault := func() error {
		// in all other cases, we've got a syntax error.
		parser.logSyntaxError()
		return errors.AddStack(errUnexpectedQuoteField)
	}

	processComma := func() error { return nil }
	if len(parser.comma) > 1 {
		processComma = func() error {
			b, err := parser.peekBytes(len(parser.comma))
			if err != nil && errors.Cause(err) != io.EOF {
				return err
			}
			if !bytes.Equal(b, parser.comma) {
				return processDefault()
			}
			return nil
		}
	}
	for {
		content, terminator, err := parser.readUntil(parser.quoteIndexFunc)
		err = parser.replaceEOF(err, errUnterminatedQuotedField)
		if err != nil {
			return err
		}
		parser.recordBuffer = append(parser.recordBuffer, content...)
		parser.skipBytes(1)
		switch {
		case len(parser.quote) > 0 && terminator == parser.quote[0]:
			if len(parser.quote) > 1 {
				b, err := parser.peekBytes(len(parser.quote) - 1)
				if err != nil && err != io.EOF {
					return err
				}
				if !bytes.Equal(b, parser.quote[1:]) {
					parser.recordBuffer = append(parser.recordBuffer, terminator)
					continue
				}
				parser.skipBytes(len(parser.quote) - 1)
			}
			// encountered '"' -> continue if we're seeing '""'.
			b, err := parser.peekBytes(1)
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				return err
			}
			switch b[0] {
			case parser.quote[0]:
				// consume the double quotation mark and continue
				if len(parser.quote) > 1 {
					b, err := parser.peekBytes(len(parser.quote))
					if err != nil && err != io.EOF {
						return err
					}
					if !bytes.Equal(b, parser.quote) {
						if parser.quote[0] == parser.comma[0] {
							return processComma()
						} else {
							return processDefault()
						}
					}
				}
				parser.skipBytes(len(parser.quote))
				parser.recordBuffer = append(parser.recordBuffer, parser.quote...)
			case '\r', '\n':
				// end the field if the next is a separator
				return nil
			case parser.comma[0]:
				return processComma()
			default:
				return processDefault()
			}

		case terminator == '\\':
			if err := parser.readByteForBackslashEscape(); err != nil {
				return err
			}
		}
	}
}

func (parser *CSVParser) readUnquoteField() error {
	addByte := func(b byte) {
		// read the following byte
		parser.recordBuffer = append(parser.recordBuffer, b)
		parser.skipBytes(1)
	}
	parseQuote := func(b byte) error {
		r, err := parser.checkBytes(parser.quote)
		if err != nil {
			return errors.Trace(err)
		}
		if r {
			parser.logSyntaxError()
			return errors.AddStack(errUnexpectedQuoteField)
		}
		addByte(b)
		return nil
	}

	parserNoComma := func(b byte) error {
		addByte(b)
		return nil
	}
	if len(parser.quote) > 0 && parser.comma[0] == parser.quote[0] {
		parserNoComma = parseQuote
	}
	for {
		content, terminator, err := parser.readUntil(parser.unquoteIndexFunc)
		parser.recordBuffer = append(parser.recordBuffer, content...)
		finished := false
		if err != nil {
			if errors.Cause(err) == io.EOF {
				finished = true
				err = nil
			}
			if err != nil {
				return err
			}
		}

		switch {
		case terminator == '\r', terminator == '\n', finished:
			return nil
		case terminator == parser.comma[0]:
			r, err := parser.checkBytes(parser.comma)
			if err != nil {
				return errors.Trace(err)
			}
			if r {
				return nil
			}
			if err = parserNoComma(terminator); err != nil {
				return err
			}
		case len(parser.quote) > 0 && terminator == parser.quote[0]:
			r, err := parser.checkBytes(parser.quote)
			if err != nil {
				return errors.Trace(err)
			}
			if r {
				parser.logSyntaxError()
				return errors.AddStack(errUnexpectedQuoteField)
			}
		case terminator == '\\':
			parser.skipBytes(1)
			if err := parser.readByteForBackslashEscape(); err != nil {
				return err
			}
		}
	}
}

func (parser *CSVParser) checkBytes(b []byte) (bool, error) {
	if len(b) == 1 {
		return true, nil
	}
	pb, err := parser.peekBytes(len(b))
	if err != nil {
		return false, err
	}
	return bytes.Equal(pb, b), nil
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
	if parser.shouldParseHeader {
		err := parser.ReadColumns()
		if err != nil {
			return errors.Trace(err)
		}
		parser.shouldParseHeader = false
	}

	records, err := parser.readRecord(parser.lastRecord)
	if err != nil {
		return errors.Trace(err)
	}
	parser.lastRecord = records
	// remove the last empty value
	if parser.cfg.TrimLastSep {
		i := len(records) - 1
		if i >= 0 && len(records[i]) == 0 {
			records = records[:i]
		}
	}

	row.Row = parser.acquireDatumSlice()
	if cap(row.Row) >= len(records) {
		row.Row = row.Row[:len(records)]
	} else {
		row.Row = make([]types.Datum, len(records))
	}
	for i, record := range records {
		unescaped, isNull := parser.unescapeString(record)
		if isNull {
			row.Row[i].SetNull()
		} else {
			row.Row[i].SetString(unescaped, "utf8mb4_bin")
		}
	}

	return nil
}

func (parser *CSVParser) ReadColumns() error {
	columns, err := parser.readRecord(nil)
	if err != nil {
		return errors.Trace(err)
	}
	parser.columns = make([]string, 0, len(columns))
	for _, colName := range columns {
		colName, _ = parser.unescapeString(colName)
		parser.columns = append(parser.columns, strings.ToLower(colName))
	}
	return nil
}

var newLineAsciiSet = makeByteSet([]byte{'\r', '\n'})

func indexOfNewLine(b []byte) int {
	return IndexAnyByte(b, &newLineAsciiSet)
}
func (parser *CSVParser) ReadUntilTokNewLine() (int64, error) {
	_, _, err := parser.readUntil(indexOfNewLine)
	if err != nil {
		return 0, err
	}
	parser.skipBytes(1)
	return parser.pos, nil
}
