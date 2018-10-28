// Please edit `parser.rl` if you want to modify this file. To generate
// `parser_generated.go`, please execute
//
// ```sh
// make data_parsers
// ```

package mydump

import (
	"io"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pkg/errors"
)

%%{
#`

machine chunk_parser;

block_comment = '/*' any* :>> '*/';
line_comment = /--[^\n]*\n/;
comment = block_comment | line_comment | space | [,;] | 'insert'i | 'into'i;

single_quoted = "'" (^"'" | "\\" any)** "'";
double_quoted = '"' (^'"' | '\\' any)** '"';
back_quoted = '`' ^'`'* '`';
unquoted = ^([,;()'"`] | space)+;

row = '(' (^[)'"`] | single_quoted | double_quoted | back_quoted)* ')';
name = (back_quoted | double_quoted | unquoted)+;

main := |*
	comment;

	'values'i => {
		consumedToken = tokValues
		fbreak;
	};

	row => {
		consumedToken = tokRow
		fbreak;
	};

	name => {
		consumedToken = tokName
		fbreak;
	};
*|;

#`
}%%

%% write data;

func (parser *ChunkParser) lex() (token, []byte, error) {
	var cs, ts, te, act, p int
	%% write init;

	for {
		data := parser.buf
		consumedToken := tokNil
		pe := len(data)
		eof := -1
		if parser.isLastChunk {
			eof = pe
		}

		%% write exec;

		if cs == %%{ write error; }%% {
			common.AppLogger.Errorf("Syntax error near byte %d, content is «%s»", parser.pos, string(data))
			return tokNil, nil, errors.New("Syntax error")
		}

		if consumedToken != tokNil {
			result := data[ts:te]
			parser.buf = data[te:]
			parser.pos += int64(te)
			return consumedToken, result, nil
		}

		if parser.isLastChunk {
			return tokNil, nil, io.EOF
		}

		parser.buf = parser.buf[ts:]
		parser.pos += int64(ts)
		p -= ts
		te -= ts
		ts = 0
		if err := parser.readBlock(); err != nil {
			return tokNil, nil, errors.Trace(err)
		}
	}

	return tokNil, nil, nil
}
