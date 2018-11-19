package mydump_test

import (
	"io"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pkg/errors"
)

var _ = Suite(&testMydumpParserSuite{})

type testMydumpParserSuite struct{}

func (s *testMydumpParserSuite) SetUpSuite(c *C)    {}
func (s *testMydumpParserSuite) TearDownSuite(c *C) {}

func (s *testMydumpParserSuite) TestReadRow(c *C) {
	reader := strings.NewReader(
		"/* whatever pragmas */;" +
			"INSERT INTO `namespaced`.`table` (columns, more, columns) VALUES (1, 2, 3),\n(4, 5, 6);" +
			"INSERT `namespaced`.`table` (x,y,z) VALUES (7,8,9);" +
			"insert another_table values (10, 11, 12, '(13)', '(', 14, ')');",
	)

	parser := mydump.NewChunkParser(reader)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row:   []byte("(1, 2, 3)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns, DeepEquals, []byte("(columns, more, columns)"))
	c.Assert(parser.Pos(), Equals, int64(97))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row:   []byte("(4, 5, 6)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns, DeepEquals, []byte("(columns, more, columns)"))
	c.Assert(parser.Pos(), Equals, int64(108))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 3,
		Row:   []byte("(7,8,9)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns, DeepEquals, []byte("(x,y,z)"))
	c.Assert(parser.Pos(), Equals, int64(159))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 4,
		Row:   []byte("(10, 11, 12, '(13)', '(', 14, ')')"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("another_table"))
	c.Assert(parser.Columns, IsNil)
	c.Assert(parser.Pos(), Equals, int64(222))

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpParserSuite) TestReadChunks(c *C) {
	reader := strings.NewReader(`
		INSERT foo VALUES (1,2,3,4),(5,6,7,8),(9,10,11,12);
		INSERT foo VALUES (13,14,15,16),(17,18,19,20),(21,22,23,24),(25,26,27,28);
		INSERT foo VALUES (29,30,31,32),(33,34,35,36);
	`)

	parser := mydump.NewChunkParser(reader)

	chunks, err := parser.ReadChunks(32)
	c.Assert(err, IsNil)
	c.Assert(chunks, DeepEquals, []mydump.Chunk{
		mydump.Chunk{
			Offset:       0,
			EndOffset:    40,
			PrevRowIDMax: 0,
			RowIDMax:     2,
		},
		mydump.Chunk{
			Offset:       40,
			EndOffset:    88,
			PrevRowIDMax: 2,
			RowIDMax:     4,
		},
		mydump.Chunk{
			Offset:       88,
			EndOffset:    130,
			PrevRowIDMax: 4,
			RowIDMax:     7,
		},
		mydump.Chunk{
			Offset:       130,
			EndOffset:    165,
			PrevRowIDMax: 7,
			RowIDMax:     8,
		},
		mydump.Chunk{
			Offset:       165,
			EndOffset:    179,
			PrevRowIDMax: 8,
			RowIDMax:     9,
		},
	})
}
