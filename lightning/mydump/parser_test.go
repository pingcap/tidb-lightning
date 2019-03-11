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

package mydump_test

import (
	"context"
	"io"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/worker"

	"github.com/pingcap/errors"
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

	ioWorkers := worker.NewPool(context.Background(), 5, "test")
	parser := mydump.NewChunkParser(reader, config.ReadBlockSize, ioWorkers)

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 1,
		Row:   []byte("(1, 2, 3)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns(), DeepEquals, []byte("(columns, more, columns)"))
	offset, rowID := parser.Pos()
	c.Assert(offset, Equals, int64(97))
	c.Assert(rowID, Equals, int64(1))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 2,
		Row:   []byte("(4, 5, 6)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns(), DeepEquals, []byte("(columns, more, columns)"))
	offset, rowID = parser.Pos()
	c.Assert(offset, Equals, int64(108))
	c.Assert(rowID, Equals, int64(2))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 3,
		Row:   []byte("(7,8,9)"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("`namespaced`.`table`"))
	c.Assert(parser.Columns(), DeepEquals, []byte("(x,y,z)"))
	offset, rowID = parser.Pos()
	c.Assert(offset, Equals, int64(159))
	c.Assert(rowID, Equals, int64(3))

	c.Assert(parser.ReadRow(), IsNil)
	c.Assert(parser.LastRow(), DeepEquals, mydump.Row{
		RowID: 4,
		Row:   []byte("(10, 11, 12, '(13)', '(', 14, ')')"),
	})
	c.Assert(parser.TableName, DeepEquals, []byte("another_table"))
	c.Assert(parser.Columns(), IsNil)
	offset, rowID = parser.Pos()
	c.Assert(offset, Equals, int64(222))
	c.Assert(rowID, Equals, int64(4))

	c.Assert(errors.Cause(parser.ReadRow()), Equals, io.EOF)
}

func (s *testMydumpParserSuite) TestReadChunks(c *C) {
	reader := strings.NewReader(`
		INSERT foo VALUES (1,2,3,4),(5,6,7,8),(9,10,11,12);
		INSERT foo VALUES (13,14,15,16),(17,18,19,20),(21,22,23,24),(25,26,27,28);
		INSERT foo VALUES (29,30,31,32),(33,34,35,36);
	`)

	ioWorkers := worker.NewPool(context.Background(), 5, "test")
	parser := mydump.NewChunkParser(reader, config.ReadBlockSize, ioWorkers)

	chunks, err := mydump.ReadChunks(parser, 32)
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

func (s *testMydumpParserSuite) TestNestedRow(c *C) {
	reader := strings.NewReader(`
		INSERT INTO exam_detail VALUES
		("123",CONVERT("{}" USING UTF8MB4)),
		("456",CONVERT("{\"a\":4}" USING UTF8MB4)),
		("789",CONVERT("[]" USING UTF8MB4));
	`)

	ioWorkers := worker.NewPool(context.Background(), 5, "test")
	parser := mydump.NewChunkParser(reader, config.ReadBlockSize, ioWorkers)
	chunks, err := mydump.ReadChunks(parser, 96)

	c.Assert(err, IsNil)
	c.Assert(chunks, DeepEquals, []mydump.Chunk{
		{
			Offset:       0,
			EndOffset:    117,
			PrevRowIDMax: 0,
			RowIDMax:     2,
		},
		{
			Offset:       117,
			EndOffset:    156,
			PrevRowIDMax: 2,
			RowIDMax:     3,
		},
	})
}
