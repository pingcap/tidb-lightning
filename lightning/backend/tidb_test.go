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

package backend_test

import (
	"context"
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var _ = Suite(&mysqlSuite{})

type mysqlSuite struct {
	dbHandle *sql.DB
	mockDB   sqlmock.Sqlmock
	backend  kv.Backend
}

func (s *mysqlSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	s.dbHandle = db
	s.mockDB = mock
	s.backend = kv.NewTiDBBackend(db, config.ReplaceOnDup)
}

func (s *mysqlSuite) TearDownTest(c *C) {
	s.backend.Close()
	c.Assert(s.mockDB.ExpectationsWereMet(), IsNil)
}

func (s *mysqlSuite) TestWriteRowsReplaceOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QREPLACE INTO `foo`.`bar`(`a`,`b`,`c`,`d`,`e`,`f`,`g`,`h`,`i`,`j`,`k`,`l`,`m`,`n`,`o`) VALUES(18446744073709551615,-9223372036854775808,0,NULL,7.5,5e-324,1.7976931348623157e+308,0,'甲乙丙\\r\\n\\0\\Z''\"\\\\`',0x000000abcdef,2557891634,'12.5',51)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	engine, err := s.backend.OpenEngine(ctx, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := s.backend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := s.backend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder := s.backend.NewEncoder(nil, 0, 1234567890)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewUintDatum(18446744073709551615),
		types.NewIntDatum(-9223372036854775808),
		types.NewUintDatum(0),
		types.Datum{},
		types.NewFloat32Datum(7.5),
		types.NewFloat64Datum(5e-324),
		types.NewFloat64Datum(1.7976931348623157e+308),
		types.NewFloat64Datum(-0.0),
		types.NewStringDatum("甲乙丙\r\n\x00\x26'\"\\`"),
		types.NewBinaryLiteralDatum(types.NewBinaryLiteralFromUint(0xabcdef, 6)),
		types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(0x98765432, 4)),
		types.NewDecimalDatum(types.NewDecFromFloatForTest(12.5)),
		types.NewMysqlEnumDatum(types.Enum{Name: "ENUM_NAME", Value: 51}),
	}, 1, nil)
	c.Assert(err, IsNil)
	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	err = engine.WriteRows(ctx, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"}, dataRows)
	c.Assert(err, IsNil)
}

func (s *mysqlSuite) TestWriteRowsIgnoreOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QINSERT IGNORE INTO `foo`.`bar`(`a`) VALUES(1)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	ignoreBackend := kv.NewTiDBBackend(s.dbHandle, config.IgnoreOnDup)
	engine, err := ignoreBackend.OpenEngine(ctx, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := ignoreBackend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := ignoreBackend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder := ignoreBackend.NewEncoder(nil, 0)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
	}, 1, nil)
	c.Assert(err, IsNil)
	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	err = engine.WriteRows(ctx, []string{"a"}, dataRows)
	c.Assert(err, IsNil)
}

func (s *mysqlSuite) TestWriteRowsErrorOnDup(c *C) {
	s.mockDB.
		ExpectExec("\\QINSERT INTO `foo`.`bar`(`a`) VALUES(1)\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	logger := log.L()

	ignoreBackend := kv.NewTiDBBackend(s.dbHandle, config.ErrorOnDup)
	engine, err := ignoreBackend.OpenEngine(ctx, "`foo`.`bar`", 1)
	c.Assert(err, IsNil)

	dataRows := ignoreBackend.MakeEmptyRows()
	dataChecksum := verification.MakeKVChecksum(0, 0, 0)
	indexRows := ignoreBackend.MakeEmptyRows()
	indexChecksum := verification.MakeKVChecksum(0, 0, 0)

	encoder := ignoreBackend.NewEncoder(nil, 0)
	row, err := encoder.Encode(logger, []types.Datum{
		types.NewIntDatum(1),
	}, 1, nil)
	c.Assert(err, IsNil)
	row.ClassifyAndAppend(&dataRows, &dataChecksum, &indexRows, &indexChecksum)

	err = engine.WriteRows(ctx, []string{"a"}, dataRows)
	c.Assert(err, IsNil)
}
