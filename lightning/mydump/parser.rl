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

# This is a ragel parser to quickly scan through a data source file consisting
# of INSERT statements only. You may find detailed syntax explanation on its
# website <https://www.colm.net/open-source/ragel/>.

machine chunk_parser;

# We treat all unimportant patterns as "comments". This include:
#  - Real SQL comments `/* ... */` and `-- ...`
#  - Whitespace
#  - Separators `,` and `;`
#  - The keywords `INSERT` and `INTO` (suffix `i` means case-insensitive).
block_comment = '/*' any* :>> '*/';
line_comment = /--[^\n]*\n/;
comment = block_comment | line_comment | space | [,;] | 'insert'i | 'into'i;

# The patterns parse quoted strings.
# They do NOT handle the escape-by-doubling syntax like `'ten o''clock'`, this
# will be handled as two tokens: `'ten o'` and `'clock'`. See the `name` rule
# below for why this doesn't matter.
single_quoted = "'" (^"'" | "\\" any)** "'";
double_quoted = '"' (^'"' | '\\' any)** '"';
back_quoted = '`' ^'`'* '`';
unquoted = ^([,;()'"`] | space)+;

# Matches a "row" of the form `( ... )`, where the content doesn't matter.
row = '(' (^[)'"`] | single_quoted | double_quoted | back_quoted)* ')';

# Matches a table name, which consists of one or more identifiers. This allows
# us to match a qualified name like `foo.bar`, and also double-backquote like
# ``` `foo``bar` ```.
name = (back_quoted | double_quoted | unquoted)+;

# The actual parser only produces 3 kinds of tokens:
#  - The keyword VALUES, as a separator between column names and data rows
#  - A row (which can be a list of columns or values depending on context)
#  - A table name
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
