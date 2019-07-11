// Please edit `csv_parser.rl` if you want to modify this file. To generate
// `csv_parser_generated.go`, please execute
//
// ```sh
// make data_parsers
// ```

package mydump

import (
	"io"

	"github.com/pingcap/errors"
)

%%{
#`

# This is a ragel parser to quickly scan through a CSV data source file.
# You may find detailed syntax explanation on its website
# <https://www.colm.net/open-source/ragel/>.

machine csv_parser;

# We are not going to use Go's `encoding/csv` package since we have some special cases to deal with.
#
# MySQL supports backslash escaping, so the following has 2 fields, but `encoding/csv` will report
# a syntax error.
#
# 	"5\"6",7
#

q = ^[\r\n] when { fc == delim };
bs = '\\' when { parser.escFlavor != backslashEscapeFlavorNone };
sep = ^[\r\n] when { fc == sep };

c = (^[\r\n] - q - bs - sep) | bs any;

main := |*
	sep => {
		consumedToken = csvTokSep
		fbreak;
	};

	q (c | [\r\n] | sep | q q)* q | c+ => {
		consumedToken = csvTokField
		fbreak;
	};

	[\r\n]+ => {
		consumedToken = csvTokNewLine
		fbreak;
	};
*|;

#`
}%%

%% write data;

func (parser *CSVParser) lex() (csvToken, []byte, error) {
	var delim byte
	if len(parser.cfg.Delimiter) > 0 {
		delim = parser.cfg.Delimiter[0]
	}
	sep := parser.cfg.Separator[0]

	var cs, ts, te, act, p int
	%% write init;

	for {
		data := parser.buf
		consumedToken := csvTokNil
		pe := len(data)
		eof := -1
		if parser.isLastChunk {
			eof = pe
		}

		%% write exec;

		if cs == %%{ write error; }%% {
			parser.logSyntaxError()
			return csvTokNil, nil, errors.New("syntax error")
		}

		if consumedToken != csvTokNil {
			result := data[ts:te]
			parser.buf = data[te:]
			parser.pos += int64(te)
			return consumedToken, result, nil
		}

		if parser.isLastChunk {
			return csvTokNil, nil, io.EOF
		}

		parser.buf = parser.buf[ts:]
		parser.pos += int64(ts)
		p -= ts
		te -= ts
		ts = 0
		if err := parser.readBlock(); err != nil {
			return csvTokNil, nil, errors.Trace(err)
		}
	}
}
