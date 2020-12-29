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

package backend

import (
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

func (s *kvSuite) TestMarshal(c *C) {
	nullDatum := types.Datum{}
	nullDatum.SetNull()
	minNotNull := types.Datum{}
	minNotNull.SetMinNotNull()
	encoder := zapcore.NewMapObjectEncoder()
	err := encoder.AddArray("test", rowArrayMarshaler{types.NewStringDatum("1"), nullDatum, minNotNull, types.MaxValueDatum()})
	c.Assert(err, IsNil)
	c.Assert(encoder.Fields["test"], DeepEquals, []interface{}{
		map[string]interface{}{"kind": "string", "val": "1"},
		map[string]interface{}{"kind": "null", "val": "NULL"},
		map[string]interface{}{"kind": "min", "val": "-inf"},
		map[string]interface{}{"kind": "max", "val": "+inf"},
	})

	invalid := types.Datum{}
	invalid.SetInterface(1)
	err = encoder.AddArray("bad-test", rowArrayMarshaler{minNotNull, invalid})
	c.Assert(err, ErrorMatches, "cannot convert.*")
	c.Assert(encoder.Fields["bad-test"], DeepEquals, []interface{}{
		map[string]interface{}{"kind": "min", "val": "-inf"},
	})
}

type mockTable struct {
	table.Table
}

func (mockTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return kv.IntHandle(-1), errors.New("mock error")
}

func (s *kvSuite) TestEncode(c *C) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)

	logger := log.Logger{Logger: zap.NewNop()}
	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}

	// Strict mode
	strictMode, err := NewTableKVEncoder(tbl, &SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	})
	c.Assert(err, IsNil)
	pairs, err := strictMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, ErrorMatches, "failed to cast value as tinyint\\(4\\) for column `c1` \\(#1\\):.*overflows tinyint")
	c.Assert(pairs, IsNil)

	rowsWithPk := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("invalid-pk"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, "failed to cast value as bigint\\(20\\) for column `_tidb_rowid`.*Truncated.*")

	rowsWithPk2 := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("1"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0x2},
		},
	}))

	// Mock add record error
	mockTbl := &mockTable{Table: tbl}
	mockMode, err := NewTableKVEncoder(mockTbl, &SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567891,
	})
	c.Assert(err, IsNil)
	pairs, err = mockMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, "mock error")

	// Non-strict mode
	noneMode, err := NewTableKVEncoder(tbl, &SessionOptions{
		SQLMode:   mysql.ModeNone,
		Timestamp: 1234567892,
		SysVars:   map[string]string{"tidb_row_format_version": "1"},
	})
	c.Assert(err, IsNil)
	pairs, err = noneMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0xfe, 0x1},
		},
	}))
}

func (s *kvSuite) TestEncodeRowFormatV2(c *C) {
	// Test encoding in row format v2, as described in <https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md>.

	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)

	logger := log.Logger{Logger: zap.NewNop()}
	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}

	noneMode, err := NewTableKVEncoder(tbl, &SessionOptions{
		SQLMode:   mysql.ModeNone,
		Timestamp: 1234567892,
		SysVars:   map[string]string{"tidb_row_format_version": "2"},
	})
	c.Assert(err, IsNil)
	pairs, err := noneMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			// the key should be the same as TestEncode()
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{
				0x80,     // version
				0x0,      // flag = 0 = not big
				0x1, 0x0, // number of not null columns = 1
				0x0, 0x0, // number of null columns = 0
				0x1,      // column IDs = [1]
				0x1, 0x0, // not null offsets = [1]
				0x7f, // column version = 127 (10000000 clamped to TINYINT)
			},
		},
	}))
}

func (s *kvSuite) TestEncodeTimestamp(c *C) {
	ty := *types.NewFieldType(mysql.TypeDatetime)
	ty.Flag |= mysql.NotNullFlag
	c1 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c1"),
		State:        model.StatePublic,
		Offset:       0,
		FieldType:    ty,
		DefaultValue: "CURRENT_TIMESTAMP",
		Version:      1,
	}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic}
	tbl, err := tables.TableFromMeta(NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)

	logger := log.Logger{Logger: zap.NewNop()}

	encoder, err := NewTableKVEncoder(tbl, &SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567893,
		SysVars: map[string]string{
			"tidb_row_format_version": "1",
			"time_zone":               "+08:00",
		},
	})
	c.Assert(err, IsNil)
	pairs, err := encoder.Encode(logger, nil, 70, []int{-1, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			Val: []uint8{0x8, 0x2, 0x9, 0x80, 0x80, 0x80, 0xf0, 0xfd, 0x8e, 0xf7, 0xc0, 0x19},
		},
	}))
}

func mockTableInfo(c *C, createSql string) *model.TableInfo {
	parser := parser.New()
	node, err := parser.ParseOneStmt(createSql, "", "")
	c.Assert(err, IsNil)
	sctx := mock.NewContext()
	info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	c.Assert(err, IsNil)
	info.State = model.StatePublic
	return info
}

func (s *kvSuite) TestDefaultAutoRandoms(c *C) {
	tblInfo := mockTableInfo(c, "create table t (id bigint unsigned NOT NULL auto_random primary key, a varchar(100));")
	// seems parser can't parse auto_random properly.
	tblInfo.AutoRandomBits = 5
	tbl, err := tables.TableFromMeta(NewPanickingAllocators(0), tblInfo)
	c.Assert(err, IsNil)
	encoder, err := NewTableKVEncoder(tbl, &SessionOptions{
		SQLMode:        mysql.ModeStrictAllTables,
		Timestamp:      1234567893,
		SysVars:        map[string]string{"tidb_row_format_version": "2"},
		AutoRandomSeed: 456,
	})
	c.Assert(err, IsNil)
	logger := log.Logger{Logger: zap.NewNop()}
	pairs, err := encoder.Encode(logger, []types.Datum{types.NewStringDatum("")}, 70, []int{-1, 0})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46},
			Val: []uint8{0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0},
		},
	}))
	c.Assert(tbl.Allocators(encoder.(*tableKVEncoder).se).Get(autoid.AutoRandomType).Base(), Equals, int64(70))

	pairs, err = encoder.Encode(logger, []types.Datum{types.NewStringDatum("")}, 71, []int{-1, 0})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]common.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x47},
			Val: []uint8{0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0},
		},
	}))
	c.Assert(tbl.Allocators(encoder.(*tableKVEncoder).se).Get(autoid.AutoRandomType).Base(), Equals, int64(71))
}

func (s *kvSuite) TestSplitIntoChunks(c *C) {
	pairs := []common.KvPair{
		{
			Key: []byte{1, 2, 3},
			Val: []byte{4, 5, 6},
		},
		{
			Key: []byte{7, 8},
			Val: []byte{9, 0},
		},
		{
			Key: []byte{1, 2, 3, 4},
			Val: []byte{5, 6, 7, 8},
		},
		{
			Key: []byte{9, 0},
			Val: []byte{1, 2},
		},
	}

	splitBy10 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(10)
	c.Assert(splitBy10, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:2]),
		MakeRowsFromKvPairs(pairs[2:3]),
		MakeRowsFromKvPairs(pairs[3:4]),
	})

	splitBy12 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(12)
	c.Assert(splitBy12, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:2]),
		MakeRowsFromKvPairs(pairs[2:4]),
	})

	splitBy1000 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(1000)
	c.Assert(splitBy1000, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:4]),
	})

	splitBy1 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(1)
	c.Assert(splitBy1, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:1]),
		MakeRowsFromKvPairs(pairs[1:2]),
		MakeRowsFromKvPairs(pairs[2:3]),
		MakeRowsFromKvPairs(pairs[3:4]),
	})
}

func (s *kvSuite) TestClassifyAndAppend(c *C) {
	kvs := MakeRowFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_ryyyyyyyy"),
			Val: []byte("value1"),
		},
		{
			Key: []byte("txxxxxxxx_rwwwwwwww"),
			Val: []byte("value2"),
		},
		{
			Key: []byte("txxxxxxxx_izzzzzzzz"),
			Val: []byte("index1"),
		},
	})

	data := MakeRowsFromKvPairs(nil)
	indices := MakeRowsFromKvPairs(nil)
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	kvs.ClassifyAndAppend(&data, &dataChecksum, &indices, &indexChecksum)

	c.Assert(data, DeepEquals, MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_ryyyyyyyy"),
			Val: []byte("value1"),
		},
		{
			Key: []byte("txxxxxxxx_rwwwwwwww"),
			Val: []byte("value2"),
		},
	}))
	c.Assert(indices, DeepEquals, MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("txxxxxxxx_izzzzzzzz"),
			Val: []byte("index1"),
		},
	}))
	c.Assert(dataChecksum.SumKVS(), Equals, uint64(2))
	c.Assert(indexChecksum.SumKVS(), Equals, uint64(1))
}

type benchSQL2KVSuite struct {
	row     []types.Datum
	colPerm []int
	encoder Encoder
	logger  log.Logger
}

var _ = Suite(&benchSQL2KVSuite{})

func (s *benchSQL2KVSuite) SetUpTest(c *C) {
	// First, create the table info corresponding to TPC-C's "CUSTOMER" table.
	p := parser.New()
	se := mock.NewContext()
	node, err := p.ParseOneStmt(`
		create table bmsql_customer(
			c_w_id         integer not null,
			c_d_id         integer not null,
			c_id           integer not null,
			c_discount     decimal(4,4),
			c_credit       char(2),
			c_last         varchar(16),
			c_first        varchar(16),
			c_credit_lim   decimal(12,2),
			c_balance      decimal(12,2),
			c_ytd_payment  decimal(12,2),
			c_payment_cnt  integer,
			c_delivery_cnt integer,
			c_street_1     varchar(20),
			c_street_2     varchar(20),
			c_city         varchar(20),
			c_state        char(2),
			c_zip          char(9),
			c_phone        char(16),
			c_since        timestamp,
			c_middle       char(2),
			c_data         varchar(500),
			primary key (c_w_id, c_d_id, c_id)
		);
	`, "", "")
	c.Assert(err, IsNil)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 123456)
	c.Assert(err, IsNil)
	tableInfo.State = model.StatePublic

	// Construct the corresponding KV encoder.
	tbl, err := tables.TableFromMeta(NewPanickingAllocators(0), tableInfo)
	c.Assert(err, IsNil)
	s.encoder, err = NewTableKVEncoder(tbl, &SessionOptions{SysVars: map[string]string{"tidb_row_format_version": "2"}})
	s.logger = log.Logger{Logger: zap.NewNop()}

	// Prepare the row to insert.
	s.row = []types.Datum{
		types.NewIntDatum(15),
		types.NewIntDatum(10),
		types.NewIntDatum(3000),
		types.NewStringDatum("0.3646"),
		types.NewStringDatum("GC"),
		types.NewStringDatum("CALLYPRIANTI"),
		types.NewStringDatum("Rg6mDFlVnP5yh"),
		types.NewStringDatum("50000.0"),
		types.NewStringDatum("-10.0"),
		types.NewStringDatum("10.0"),
		types.NewIntDatum(1),
		types.NewIntDatum(0),
		types.NewStringDatum("aJK7CuRnE0NUxNHSX"),
		types.NewStringDatum("Q1rps77cXYoj"),
		types.NewStringDatum("MigXbS6UoUS"),
		types.NewStringDatum("UJ"),
		types.NewStringDatum("638611111"),
		types.NewStringDatum("7743262784364376"),
		types.NewStringDatum("2020-02-05 19:29:58.903970"),
		types.NewStringDatum("OE"),
		types.NewStringDatum("H5p3dpjp7uu8n1l3j0o1buecfV6FngNNgftpNALDhOzJaSzMCMlrQwXuvLAFPIFg215D3wAYB62kiixIuasfbD729oq8TwgKzPPsx8kHE1b4AdhHwpCml3ELKiwuNGQl7CcBQOiq6aFEMMHzjGwQyXwGey0wutjp2KP3Nd4qj3FHtmHbsD8cJ0pH9TswNmdQBgXsFPZeJJhsG3rTimQpS9Tmn3vNeI9fFas3ClDZuQtBjqoTJlyzmBIYT8HeV3TuS93TNFDaXZpQqh8HsvlPq4uTTLOO9CguiY29zlSmIjkZYtva3iscG3YDOQVLeGpP9dtqEJwlRvJ4oe9jWkvRMlCeslSNEuzLxjUBtJBnGRFAzJF6RMlIWCkdCpIhcnIy3jUEsxTuiAU3hsZxUjLg2dnOG62h5qR"),
	}
	s.colPerm = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, -1}
}

// Run `go test github.com/pingcap/tidb-lightning/lightning/backend -check.b -test.v` to get benchmark result.
func (s *benchSQL2KVSuite) BenchmarkSQL2KV(c *C) {
	for i := 0; i < c.N; i++ {
		rows, err := s.encoder.Encode(s.logger, s.row, 1, s.colPerm)
		c.Assert(err, IsNil)
		c.Assert(rows, HasLen, 2)
	}
}
