package mydump

import (
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/worker"
	"github.com/pingcap/tidb/types"
)

type CSVParser struct {
	blockParser
	cfg       *config.CSVConfig
	escFlavor backslashEscapeFlavor
}

func NewCSVParser(
	cfg *config.CSVConfig,
	reader io.Reader,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) *CSVParser {
	escFlavor := backslashEscapeFlavorNone
	if cfg.BackslashEscape {
		escFlavor = backslashEscapeFlavorMySQL
		// we need special treatment of the NULL value \N, used by MySQL.
		if !cfg.NotNull && cfg.Null == `\N` {
			escFlavor = backslashEscapeFlavorMySQLWithNull
		}
	}

	return &CSVParser{
		blockParser: makeBlockParser(reader, blockBufSize, ioWorkers),
		cfg:         cfg,
		escFlavor:   escFlavor,
	}
}

type csvToken byte

const (
	csvTokNil csvToken = iota
	csvTokSep
	csvTokNewLine
	csvTokField
)

func (parser *CSVParser) appendEmptyValues(sepCount int) {
	var datum types.Datum
	if !parser.cfg.NotNull && parser.cfg.Null == "" {
		datum.SetNull()
	} else {
		datum.SetString("")
	}
	for i := 0; i < sepCount; i++ {
		parser.lastRow.Row = append(parser.lastRow.Row, datum)
	}
}

func (parser *CSVParser) appendField(content string) {
	input := parser.unescapeString(content)

	var isNull bool
	if parser.escFlavor == backslashEscapeFlavorMySQLWithNull {
		isNull = input == legacyNullSequence
	} else {
		isNull = !parser.cfg.NotNull && parser.cfg.Null == input
	}

	var datum types.Datum
	if isNull {
		datum.SetNull()
	} else {
		datum.SetString(input)
	}
	parser.lastRow.Row = append(parser.lastRow.Row, datum)
}

func (parser *CSVParser) unescapeString(input string) string {
	delim := parser.cfg.Delimiter
	if len(delim) > 0 && len(input) >= 2 && input[0] == delim[0] {
		return unescape(input[1:len(input)-1], delim, parser.escFlavor)
	}
	return unescape(input, "", parser.escFlavor)
}

// ReadRow reads a row from the datafile.
func (parser *CSVParser) ReadRow() error {
	emptySepCount := 1
	hasField := false

	row := &parser.lastRow
	row.RowID++
	row.Row = make([]types.Datum, 0, len(row.Row))

	// skip the header first
	if parser.pos == 0 && parser.cfg.Header {
		parser.columns = make([]string, 0, len(row.Row))
	outside:
		for {
			tok, content, err := parser.lex()
			if err != nil {
				return errors.Trace(err)
			}
			switch tok {
			case csvTokSep:
			case csvTokField:
				colName := parser.unescapeString(string(content))
				parser.columns = append(parser.columns, strings.ToLower(colName))
			case csvTokNewLine:
				break outside
			}
		}
	}

	for {
		tok, content, err := parser.lex()
		switch errors.Cause(err) {
		case nil:
		case io.EOF:
			if hasField {
				tok = csvTokNewLine
				break
			}
			fallthrough
		default:
			return errors.Trace(err)
		}

		hasField = true

		switch tok {
		case csvTokSep:
			emptySepCount++

		case csvTokField:
			parser.appendEmptyValues(emptySepCount - 1)
			emptySepCount = 0
			parser.appendField(string(content))

		case csvTokNewLine:
			if !parser.cfg.TrimLastSep {
				parser.appendEmptyValues(emptySepCount)
			}
			return nil
		}
	}
}
