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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	kvenc "github.com/pingcap/tidb/util/kvencoder"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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

func (mockTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	return -1, errors.New("mock error")
}

func (s *kvSuite) TestEncode(c *C) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false}
	tbl := tables.MockTableFromMeta(tblInfo)

	logger := log.Logger{Logger: zap.NewNop()}
	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}

	// Strict mode
	strictMode := NewTableKVEncoder(tbl, mysql.ModeStrictAllTables)
	pairs, err := strictMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, ErrorMatches, "failed to cast `10000000` as tinyint\\(4\\) for column `c1` \\(#1\\):.*overflows tinyint")
	c.Assert(pairs, IsNil)

	rowsWithPk := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("invalid-pk"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, "failed to cast `invalid-pk` as bigint\\(20\\) for column `_tidb_rowid`.*Data Truncated")

	rowsWithPk2 := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("1"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]kvenc.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0x2},
		},
	}))

	// Mock add record error
	mockTbl := &mockTable{Table: tbl}
	mockMode := NewTableKVEncoder(mockTbl, mysql.ModeStrictAllTables)
	pairs, err = mockMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, "mock error")

	// Non-strict mode
	noneMode := NewTableKVEncoder(tbl, mysql.ModeNone)
	pairs, err = noneMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, kvPairs([]kvenc.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0xfe, 0x1},
		},
	}))
}

func (s *kvSuite) TestSplitIntoChunks(c *C) {
	pairs := []kvenc.KvPair{
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

	splitBy10 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(10, false)
	c.Assert(splitBy10, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:2]),
		MakeRowsFromKvPairs(pairs[2:3]),
		MakeRowsFromKvPairs(pairs[3:4]),
	})

	splitBy12 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(12, false)
	c.Assert(splitBy12, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:2]),
		MakeRowsFromKvPairs(pairs[2:4]),
	})

	splitBy1000 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(1000, false)
	c.Assert(splitBy1000, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:4]),
	})

	splitBy1 := MakeRowsFromKvPairs(pairs).SplitIntoChunks(1, false)
	c.Assert(splitBy1, DeepEquals, []Rows{
		MakeRowsFromKvPairs(pairs[0:1]),
		MakeRowsFromKvPairs(pairs[1:2]),
		MakeRowsFromKvPairs(pairs[2:3]),
		MakeRowsFromKvPairs(pairs[3:4]),
	})
}

func (s *kvSuite) TestClassifyAndAppend(c *C) {
	kvs := MakeRowFromKvPairs([]kvenc.KvPair{
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

	c.Assert(data, DeepEquals, MakeRowsFromKvPairs([]kvenc.KvPair{
		{
			Key: []byte("txxxxxxxx_ryyyyyyyy"),
			Val: []byte("value1"),
		},
		{
			Key: []byte("txxxxxxxx_rwwwwwwww"),
			Val: []byte("value2"),
		},
	}))
	c.Assert(indices, DeepEquals, MakeRowsFromKvPairs([]kvenc.KvPair{
		{
			Key: []byte("txxxxxxxx_izzzzzzzz"),
			Val: []byte("index1"),
		},
	}))
	c.Assert(dataChecksum.SumKVS(), Equals, uint64(2))
	c.Assert(indexChecksum.SumKVS(), Equals, uint64(1))
}

