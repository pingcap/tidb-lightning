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

package kv

import (
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	kvenc "github.com/pingcap/tidb/util/kvencoder"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
		map[string]interface{}{"col": 0, "kind": "string", "val": "1"},
		map[string]interface{}{"col": 1, "kind": "null", "val": "NULL"},
		map[string]interface{}{"col": 2, "kind": "min", "val": "-inf"},
		map[string]interface{}{"col": 3, "kind": "max", "val": "+inf"},
	})

	invalid := types.Datum{}
	invalid.SetInterface(1)
	err = encoder.AddArray("bad-test", rowArrayMarshaler{minNotNull, invalid})
	c.Assert(err, ErrorMatches, "cannot convert.*")
	c.Assert(encoder.Fields["bad-test"], DeepEquals, []interface{}{
		map[string]interface{}{"col": 0, "kind": "min", "val": "-inf"},
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
	c.Assert(err, ErrorMatches, ".*overflows tinyint")
	c.Assert(pairs, IsNil)

	rowsWithPk := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("invalid-pk"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, ".*Data Truncated")

	rowsWithPk2 := []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("1"),
	}
	pairs, err = strictMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, []kvenc.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0x2},
		},
	})

	// Mock add record error
	mockTbl := &mockTable{Table: tbl}
	mockMode := NewTableKVEncoder(mockTbl, mysql.ModeStrictAllTables)
	pairs, err = mockMode.Encode(logger, rowsWithPk2, 2, []int{0, 1})
	c.Assert(err, ErrorMatches, "mock error")

	// Non-strict mode
	noneMode := NewTableKVEncoder(tbl, mysql.ModeNone)
	pairs, err = noneMode.Encode(logger, rows, 1, []int{0, 1})
	c.Assert(err, IsNil)
	c.Assert(pairs, DeepEquals, []kvenc.KvPair{
		{
			Key: []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val: []uint8{0x8, 0x2, 0x8, 0xfe, 0x1},
		},
	})
}
