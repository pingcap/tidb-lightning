package mydump_test

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

var _ = Suite(&testMydumpCSVParserSuite{})

type testMydumpCSVParserSuite struct {
	ioWorkers *worker.Pool
}

func (s *testMydumpCSVParserSuite) SetUpSuite(c *C) {
	s.ioWorkers = worker.NewPool(context.Background(), 5, "test_csv")
}
func (s *testMydumpCSVParserSuite) TearDownSuite(c *C) {}

type assertPosEq struct {
	*CheckerInfo
}

var posEq = &assertPosEq{
	&CheckerInfo{Name: "posEq", Params: []string{"parser", "pos", "rowID"}},
}

func (checker *assertPosEq) Check(params []interface{}, names []string) (result bool, error string) {
	parser := params[0].(mydump.Parser)
	pos, rowID := parser.Pos()
	expectedPos := int64(params[1].(int))
	expectedRowID := int64(params[2].(int))
	return pos == expectedPos && rowID == expectedRowID, ""
}

var nullDatum types.Datum

type testCase struct {
	input    string
	expected [][]types.Datum
}

func (s *testMydumpCSVParserSuite) runTestCases(c *C, cfg *config.CSVConfig, blockBufSize int64, cases []testCase) {
	for _, tc := range cases {
		parser := mydump.NewCSVParser(cfg, strings.NewReader(tc.input), blockBufSize, s.ioWorkers)
		for i, row := range tc.expected {
			comment := Commentf("input = %q, row = %d", tc.input, i+1)
			c.Assert(parser.ReadRow(), IsNil, comment)
			c.Assert(parser.LastRow(), DeepEquals, mydump.Row{RowID: int64(i) + 1, Row: row}, comment)
		}
		c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF, Commentf("input = %q", tc.input))
	}
}

func (s *testMydumpCSVParserSuite) runFailingTestCases(c *C, cfg *config.CSVConfig, blockBufSize int64, cases []string) {
	for _, tc := range cases {
		parser := mydump.NewCSVParser(cfg, strings.NewReader(tc), blockBufSize, s.ioWorkers)
		c.Assert(parser.ReadRow(), ErrorMatches, "syntax error", Commentf("input = %q", tc))
	}
}

func (s *testMydumpCSVParserSuite) TestTCPH(c *C) {
	reader := strings.NewReader(
		`1|goldenrod lavender spring chocolate lace|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|7|JUMBO PKG|901.00|ly. slyly ironi|
2|blush thistle blue yellow saddle|Manufacturer#1|Brand#13|LARGE BRUSHED BRASS|1|LG CASE|902.00|lar accounts amo|
3|spring green yellow purple cornsilk|Manufacturer#4|Brand#42|STANDARD POLISHED BRASS|21|WRAP CASE|903.00|egular deposits hag|
`)

	cfg := config.CSVConfig{
		Separator:   "|",
		Delimiter:   "",
		TrimLastSep: true,
	}

	parser := mydump.NewCSVParser(&cfg, reader, config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("1"),
			types.NewStringDatum("goldenrod lavender spring chocolate lace"),
			types.NewStringDatum("Manufacturer#1"),
			types.NewStringDatum("Brand#13"),
			types.NewStringDatum("PROMO BURNISHED COPPER"),
			types.NewStringDatum("7"),
			types.NewStringDatum("JUMBO PKG"),
			types.NewStringDatum("901.00"),
			types.NewStringDatum("ly. slyly ironi"),
		},
	})
	c.Assert(parser, posEq, 126, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("2"),
			types.NewStringDatum("blush thistle blue yellow saddle"),
			types.NewStringDatum("Manufacturer#1"),
			types.NewStringDatum("Brand#13"),
			types.NewStringDatum("LARGE BRUSHED BRASS"),
			types.NewStringDatum("1"),
			types.NewStringDatum("LG CASE"),
			types.NewStringDatum("902.00"),
			types.NewStringDatum("lar accounts amo"),
		},
	})
	c.Assert(parser, posEq, 240, 2)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 3,
		Row: []types.Datum{
			types.NewStringDatum("3"),
			types.NewStringDatum("spring green yellow purple cornsilk"),
			types.NewStringDatum("Manufacturer#4"),
			types.NewStringDatum("Brand#42"),
			types.NewStringDatum("STANDARD POLISHED BRASS"),
			types.NewStringDatum("21"),
			types.NewStringDatum("WRAP CASE"),
			types.NewStringDatum("903.00"),
			types.NewStringDatum("egular deposits hag"),
		},
	})
	c.Assert(parser, posEq, 367, 3)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestRFC4180(c *C) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	// example 1, trailing new lines

	parser := mydump.NewCSVParser(&cfg, strings.NewReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
	})
	c.Assert(parser, posEq, 12, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
	})
	c.Assert(parser, posEq, 24, 2)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	// example 2, no trailing new lines

	parser = mydump.NewCSVParser(&cfg, strings.NewReader("aaa,bbb,ccc\nzzz,yyy,xxx"), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
	})
	c.Assert(parser, posEq, 12, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
	})
	c.Assert(parser, posEq, 23, 2)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	// example 5, quoted fields

	parser = mydump.NewCSVParser(&cfg, strings.NewReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
	})
	c.Assert(parser, posEq, 18, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
	})
	c.Assert(parser, posEq, 29, 2)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	// example 6, line breaks within fields

	parser = mydump.NewCSVParser(&cfg, strings.NewReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\nbb"),
			types.NewStringDatum("ccc"),
		},
	})
	c.Assert(parser, posEq, 19, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
	})
	c.Assert(parser, posEq, 30, 2)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	// example 7, quote escaping

	parser = mydump.NewCSVParser(&cfg, strings.NewReader(`"aaa","b""bb","ccc"`), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\"bb"),
			types.NewStringDatum("ccc"),
		},
	})
	c.Assert(parser, posEq, 19, 1)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestMySQL(c *C) {
	cfg := config.CSVConfig{
		Separator:       ",",
		Delimiter:       `"`,
		BackslashEscape: true,
		NotNull:         false,
		Null:            `\N`,
	}

	parser := mydump.NewCSVParser(&cfg, strings.NewReader(`"\"","\\","\?"
"\
",\N,\\N`), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`"`),
			types.NewStringDatum(`\`),
			types.NewStringDatum("?"),
		},
	})
	c.Assert(parser, posEq, 15, 1)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("\n"),
			nullDatum,
			types.NewStringDatum(`\N`),
		},
	})
	c.Assert(parser, posEq, 26, 2)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestSyntaxError(c *C) {
	cfg := config.CSVConfig{
		Separator:       ",",
		Delimiter:       `"`,
		BackslashEscape: true,
	}

	inputs := []string{
		`"???`,
		`\`,
		`"\`,
		`0"`,
		`0\`,
		"\"\v",
		`"""`,
		"\"\r",
		"\"\x01",
	}

	s.runFailingTestCases(c, &cfg, config.ReadBlockSize, inputs)

	cfg.BackslashEscape = false
	s.runFailingTestCases(c, &cfg, config.ReadBlockSize, []string{`"\`})
}

func (s *testMydumpCSVParserSuite) TestTSV(c *C) {
	cfg := config.CSVConfig{
		Separator:       "\t",
		Delimiter:       "",
		BackslashEscape: false,
		NotNull:         false,
		Null:            "",
		Header:          true,
	}

	parser := mydump.NewCSVParser(&cfg, strings.NewReader(`a	b	c	d	e	f
0				foo	0000-00-00
0				foo	0000-00-00
0	abc	def	ghi	bar	1999-12-31`), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			nullDatum,
			nullDatum,
			types.NewStringDatum("foo"),
			types.NewStringDatum("0000-00-00"),
		},
	})
	c.Assert(parser, posEq, 32, 1)
	c.Assert(parser.Columns(), DeepEquals, []string{"a", "b", "c", "d", "e", "f"})

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			nullDatum,
			nullDatum,
			types.NewStringDatum("foo"),
			types.NewStringDatum("0000-00-00"),
		},
	})
	c.Assert(parser, posEq, 52, 2)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 3,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			types.NewStringDatum("abc"),
			types.NewStringDatum("def"),
			types.NewStringDatum("ghi"),
			types.NewStringDatum("bar"),
			types.NewStringDatum("1999-12-31"),
		},
	})
	c.Assert(parser, posEq, 80, 3)

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestEmpty(c *C) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser := mydump.NewCSVParser(&cfg, strings.NewReader(""), config.ReadBlockSize, s.ioWorkers)
	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	// Try again with headers.

	cfg.Header = true

	parser = mydump.NewCSVParser(&cfg, strings.NewReader(""), config.ReadBlockSize, s.ioWorkers)
	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)

	parser = mydump.NewCSVParser(&cfg, strings.NewReader("h\n"), config.ReadBlockSize, s.ioWorkers)
	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestCRLF(c *C) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}
	parser := mydump.NewCSVParser(&cfg, strings.NewReader("a\rb\r\nc\n\n\n\nd"), config.ReadBlockSize, s.ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row:   []types.Datum{types.NewStringDatum("a")},
	})

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row:   []types.Datum{types.NewStringDatum("b")},
	})

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 3,
		Row:   []types.Datum{types.NewStringDatum("c")},
	})

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 4,
		Row:   []types.Datum{types.NewStringDatum("d")},
	})

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestQuotedSeparator(c *C) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser := mydump.NewCSVParser(&cfg, strings.NewReader(`",",','`), config.ReadBlockSize, s.ioWorkers)
	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(","),
			types.NewStringDatum("'"),
			types.NewStringDatum("'"),
		},
	})

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpCSVParserSuite) TestConsecutiveFields(c *C) {
	// Note: the behavior of reading `"xxx"yyy` here is undefined in RFC 4180.
	// Python's CSV module returns `xxxyyy`.
	// Rust's CSV package returns `xxxyyy`.
	// Go's CSV package returns a parse error.
	// NPM's CSV package returns a parse error.
	// MySQL's LOAD DATA statement returns `"xxx"yyy` as-is.
	// For simplicity we treat this as two separate fields.

	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	testCases := []testCase{
		{
			input:    `"x"?`,
			expected: [][]types.Datum{{types.NewStringDatum("x"), types.NewStringDatum("?")}},
		},
		{
			input:    "\"\"\x01",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\x01")}},
		},
		{
			input:    "\"\"\v",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\v")}},
		},
	}

	s.runTestCases(c, &cfg, config.ReadBlockSize, testCases)
}

func (s *testMydumpCSVParserSuite) TestSpecialChars(c *C) {
	cfg := config.CSVConfig{Separator: ",", Delimiter: `"`}
	testCases := []testCase{
		{
			input:    "\x00",
			expected: [][]types.Datum{{types.NewStringDatum("\x00")}},
		},
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "0\v",
			expected: [][]types.Datum{{types.NewStringDatum("0\v")}},
		},
		{
			input:    "0\x00",
			expected: [][]types.Datum{{types.NewStringDatum("0\x00")}},
		},
		{
			input:    "\n\r",
			expected: [][]types.Datum{{nullDatum}},
		},
	}

	s.runTestCases(c, &cfg, config.ReadBlockSize, testCases)
}

func (s *testMydumpCSVParserSuite) TestContinuation(c *C) {
	cfg := config.CSVConfig{
		Separator:       ",",
		Delimiter:       `"`,
		BackslashEscape: true,
		TrimLastSep:     true,
	}

	testCases := []testCase{
		{
			input: `"abcdef",\njklm,nop` + "\r\n" + `"""""","\n",a,`,
			expected: [][]types.Datum{
				{
					types.NewStringDatum("abcdef"),
					types.NewStringDatum("\njklm"),
					types.NewStringDatum("nop"),
				},
				{
					types.NewStringDatum(`""`),
					types.NewStringDatum("\n"),
					types.NewStringDatum("a"),
				},
			},
		},
	}

	s.runTestCases(c, &cfg, 1, testCases)
}

func (s *testMydumpCSVParserSuite) TestOverlappingSepDelim(c *C) {
	// If the same character is simultaneously a separator and a delimiter,
	// we treat paired characters as a delimiter and an orphan character as a
	// separator, due to behavior of picking longest match in Ragel's tokenizer.
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: ",",
	}

	testCases := []testCase{
		{
			input:    `,`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    "0000,0",
			expected: [][]types.Datum{{types.NewStringDatum("0000"), types.NewStringDatum("0")}},
		},
		{
			input:    ",0",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("0")}},
		},
		{
			input:    ",\r",
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    ",\n",
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    ",\r\n",
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    ",,",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    ",c",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("c")}},
		},
		{
			input:    ",\x04",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\x04")}},
		},
		{
			input:    ",\f",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\f")}},
		},
		{
			input:    ",0,",
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    `,\`,
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum(`\`)}},
		},
		{
			input:    "0,00,0",
			expected: [][]types.Datum{{types.NewStringDatum("0"), types.NewStringDatum("00"), types.NewStringDatum("0")}},
		},
		{
			input:    ",,0",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("0")}},
		},
		{
			input:    ",,\f",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\f")}},
		},
		{
			input:    ",,\x8f",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\x8f")}},
		},
		{
			input:    ",,,",
			expected: [][]types.Datum{{types.NewStringDatum(",")}},
		},
	}

	s.runTestCases(c, &cfg, 1, testCases)

	cfg.BackslashEscape = true
	testCases = []testCase{
		{
			input:    ",,\x02",
			expected: [][]types.Datum{{nullDatum, types.NewStringDatum("\x02")}},
		},
		{
			input:    ",,\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    ",,\r",
			expected: [][]types.Datum{{nullDatum}},
		},
	}

	s.runTestCases(c, &cfg, 1, testCases)

	failingInputs := []string{
		`,\`,
		`,,\`,
	}
	s.runFailingTestCases(c, &cfg, 1, failingInputs)
}

func (s *testMydumpCSVParserSuite) TestBackslashAsSep(c *C) {
	cfg := config.CSVConfig{
		Separator: `\`,
		Delimiter: `"`,
	}

	testCases := []testCase{
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0"), nullDatum}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
	}

	s.runTestCases(c, &cfg, 1, testCases)

	failingInputs := []string{
		`"\`,
	}
	s.runFailingTestCases(c, &cfg, 1, failingInputs)

	cfg.BackslashEscape = true

	testCases = []testCase{
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0"), nullDatum}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    `""""\0`,
			expected: [][]types.Datum{{types.NewStringDatum(`"`), types.NewStringDatum("\x00")}},
		},
		{
			input:    `\0`,
			expected: [][]types.Datum{{types.NewStringDatum("\x00")}},
		},
		{
			input:    `"\"`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    `"\"\`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`), nullDatum}},
		},
	}

	s.runTestCases(c, &cfg, 1, testCases)

	failingInputs = []string{
		`"\`,
		"\"\\\xef",
		`"000\0`,
		`"\0`,
		`"\\`,
		"\"\\\v",
		"\"\\\n",
		"\"\\\x00",
		"\"\\\r",
	}
	s.runFailingTestCases(c, &cfg, 1, failingInputs)
}

func (s *testMydumpCSVParserSuite) TestBackslashAsDelim(c *C) {
	// Most of these are just documenting the current behavior for coverage,
	// there's no sane way to describe the desired behavior. The expected
	// results of these tests may change according to the parser's internals.
	//
	// We'll deny these cases when checking the config.
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `\`,
	}

	testCases := []testCase{
		{
			input:    `\\`,
			expected: [][]types.Datum{{nullDatum}},
		},
	}
	s.runTestCases(c, &cfg, 1, testCases)

	failingInputs := []string{
		`"\`,
	}
	s.runFailingTestCases(c, &cfg, 1, failingInputs)

	cfg.BackslashEscape = true

	testCases = []testCase{
		{
			input:    `\0`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\x00",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\\`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\r",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "000\r\\0",
			expected: [][]types.Datum{{types.NewStringDatum("000")}, {nullDatum}},
		},
		{
			input:    "\\\xe3",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\v",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\0\xbf",
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    `\0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    "\\0\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\0\r",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\0\v",
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    "00\n\\00",
			expected: [][]types.Datum{{types.NewStringDatum("00")}, {types.NewStringDatum("0")}},
		},
		{
			input:    `\\0`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "00,\\00",
			expected: [][]types.Datum{{types.NewStringDatum("00"), types.NewStringDatum("0")}},
		},
		{
			input:    "\\\\\x00",
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    `\01`,
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    "\\0\x00",
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    `\,`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\\\r",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\0\\`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    `\0,`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    `\\\\\\\\\\0`,
			expected: [][]types.Datum{{types.NewStringDatum(`\\\`)}},
		},
		{
			input:    `\\,`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    "\\0\\\r",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\\\n",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\r\\",
			expected: [][]types.Datum{{types.NewStringDatum("0\r")}},
		},
		{
			input:    "\\\\\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\0\0`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "0\n\\0\\0",
			expected: [][]types.Datum{{types.NewStringDatum(`0`)}, {types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\\v",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("\v")}},
		},
		{
			input:    "\\0\\\v",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\0",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("0")}},
		},
		{
			input:    "\\0\\\x00",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\\n",
			expected: [][]types.Datum{{types.NewStringDatum("0\n")}},
		},
		{
			input:    "\\0\r\\\r",
			expected: [][]types.Datum{{types.NewStringDatum("0\r")}},
		},
		{
			input:    "\n\\0\n\\0",
			expected: [][]types.Datum{{nullDatum}, {types.NewStringDatum("0\n"), types.NewStringDatum("0")}},
		},
		{
			input:    "\\0\n\\\x01",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("\x01")}},
		},
	}
	s.runTestCases(c, &cfg, 1, testCases)

	failingInputs = []string{
		`0\`,
		`\`,
		`\\\`,
		`\0,\\`,
	}
	s.runFailingTestCases(c, &cfg, 1, failingInputs)
}

func (s *testMydumpCSVParserSuite) TestBackslashAsSepAndDelim(c *C) {
	// Most of these are just documenting the current behavior for coverage,
	// there's no sane way to describe the desired behavior. The expected
	// results of these tests may change according to the parser's internals.
	//
	// We'll deny these cases when checking the config.
	cfg := config.CSVConfig{
		Separator: `\`,
		Delimiter: `\`,
	}

	testCases := []testCase{
		{
			input:    `\`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    `\0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    `\\`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\\\`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
	}
	s.runTestCases(c, &cfg, 1, testCases)

	cfg.BackslashEscape = true

	testCases = []testCase{
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0"), nullDatum}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
		{
			input:    "\\\xe7",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\0`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\x00",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\\`,
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\r",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "000\r\\0",
			expected: [][]types.Datum{{types.NewStringDatum("000")}, {nullDatum}},
		},
		{
			input:    "\\\v",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0")}},
		},
		{
			input:    "00\r\\\\0",
			expected: [][]types.Datum{{types.NewStringDatum("00")}, {types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\0\n\\",
			expected: [][]types.Datum{{types.NewStringDatum("0\n")}},
		},
		{
			input:    "\\\\r",
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\\\\r",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    "\\\\0",
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\\\\v",
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\\\\x00",
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\\\\n",
			expected: [][]types.Datum{{nullDatum}},
		},
		{
			input:    `\\\`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "\\0\\\v",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\\\",
			expected: [][]types.Datum{{types.NewStringDatum("0\n\\")}},
		},
		{
			input:    "\\0\n\\0",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("0")}},
		},
		{
			input:    `\0\\`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\\\x00",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\\n",
			expected: [][]types.Datum{{types.NewStringDatum("0\n")}},
		},
		{
			input:    "\\0\\t",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\n\\\x04",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("\x04")}},
		},
		{
			input:    "\\0\\\r",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "\\0\r\\\r",
			expected: [][]types.Datum{{types.NewStringDatum("0\r")}},
		},
		{
			input:    "\\0\n\\\xdf",
			expected: [][]types.Datum{{types.NewStringDatum("0\n"), types.NewStringDatum("\xdf")}},
		},
		{
			input:    "\n\\0\n\\0",
			expected: [][]types.Datum{{nullDatum}, {types.NewStringDatum("0\n"), types.NewStringDatum("0")}},
		},
		{
			input:    "\\0\r\\\v",
			expected: [][]types.Datum{{types.NewStringDatum("0\r"), types.NewStringDatum("\v")}},
		},
		{
			input:    "\\0\\\n",
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    `\0\0`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    "0\n\\0\\0",
			expected: [][]types.Datum{{types.NewStringDatum("0")}, {types.NewStringDatum(`0\`)}},
		},
	}
	s.runTestCases(c, &cfg, 1, testCases)
}

// errorReader implements the Reader interface which always returns an error.
type errorReader struct{}

func (*errorReader) Read(p []byte) (int, error) {
	return 0, errors.New("fake read error")
}

func (s *testMydumpCSVParserSuite) TestReadError(c *C) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser := mydump.NewCSVParser(&cfg, &errorReader{}, config.ReadBlockSize, s.ioWorkers)
	c.Assert(parser.ReadRow(), ErrorMatches, "fake read error")
}

// TestSyntaxErrorLog checks that a syntax error won't dump huge strings into the log.
func (s *testMydumpCSVParserSuite) TestSyntaxErrorLog(c *C) {
	cfg := config.CSVConfig{
		Separator: "\t",
		Delimiter: "'",
	}

	tc := strings.NewReader("x'" + strings.Repeat("y", 50000))
	parser := mydump.NewCSVParser(&cfg, tc, 50000, s.ioWorkers)
	var buffer *zaptest.Buffer
	parser.Logger, buffer = log.MakeTestLogger()
	c.Assert(parser.ReadRow(), ErrorMatches, "syntax error.*")
	c.Assert(parser.Logger.Sync(), IsNil)

	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"ERROR","$msg":"syntax error","pos":1,"content":"'`+strings.Repeat("y", 255)+`"}`,
	)
}

// Run `go test github.com/pingcap/tidb-lightning/lightning/mydump -check.b -check.bmem -test.v` to get benchmark result.
// Please ensure your temporary storage has (c.N / 2) KiB of free space.

type benchCSVParserSuite struct {
	csvPath   string
	ioWorkers *worker.Pool
}

var _ = Suite(&benchCSVParserSuite{})

func (s *benchCSVParserSuite) setupTest(c *C) {
	s.ioWorkers = worker.NewPool(context.Background(), 5, "bench_csv")

	dir := c.MkDir()
	s.csvPath = filepath.Join(dir, "input.csv")
	file, err := os.Create(s.csvPath)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(file.Close(), IsNil)
	}()
	for i := 0; i < c.N; i++ {
		_, err = file.WriteString("18,1,1,0.3650,GC,BARBARBAR,rw9AOV1AjoI1,50000.00,-10.00,10.00,1,1,djj3Q2XaIPoYVy1FuF,gc80Q2o82Au3C9xv,PYOolSxG3w,DI,265111111,7586538936787184,2020-02-26 20:06:00.193,OE,YCkSPBVqoJ2V5F8zWs87V5XzbaIY70aWCD4dgcB6bjUzCr5wOJCJ2TYH49J7yWyysbudJIxlTAEWSJahY7hswLtTsqyjEkrlsN8iDMAa9Poj29miJ08tnn2G8mL64IlyywvnRGbLbyGvWDdrOSF42RyUFTWVyqlDWc6Gr5wyMPYgvweKemzFDVD3kro5JsmBmJY08EK54nQoyfo2sScyb34zcM9GFo9ZQTwloINfPYQKXQm32m0XvU7jiNmYpFTFJQjdqA825SEvQqMMefG2WG4jVu9UPdhdUjRsFRd0Gw7YPKByOlcuY0eKxT7sAzMKXx2000RR6dqHNXe47oVYd\n")
		c.Assert(err, IsNil)
	}
	c.ResetTimer()
}

func (s *benchCSVParserSuite) BenchmarkReadRowUsingMydumpCSVParser(c *C) {
	s.setupTest(c)

	file, err := os.Open(s.csvPath)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(file.Close(), IsNil)
	}()

	cfg := config.CSVConfig{Separator: ","}
	parser := mydump.NewCSVParser(&cfg, file, 65536, s.ioWorkers)
	parser.Logger.Logger = zap.NewNop()

	rowsCount := 0
	for {
		err := parser.ReadRow()
		if err == nil {
			rowsCount++
			continue
		}
		if errors.Cause(err) == io.EOF {
			return
		}
		c.Fatal(err)
	}
	c.Assert(rowsCount, Equals, c.N)
}

func (s *benchCSVParserSuite) BenchmarkReadRowUsingEncodingCSV(c *C) {
	s.setupTest(c)

	file, err := os.Open(s.csvPath)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(file.Close(), IsNil)
	}()

	csvParser := csv.NewReader(file)

	rowsCount := 0
	for {
		records, err := csvParser.Read()
		if err == nil {
			// for fair comparison, we need to include the cost of conversion to Datum.
			datums := make([]types.Datum, 0, len(records))
			for _, record := range records {
				datums = append(datums, types.NewStringDatum(record))
			}
			rowsCount++
			continue
		}
		if errors.Cause(err) == io.EOF {
			return
		}
		c.Fatal(err)
	}
	c.Assert(rowsCount, Equals, c.N)
}
