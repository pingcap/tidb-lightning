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

package restore

import (
	"context"
	"net/http"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
)

var _ = Suite(&tidbSuite{})

type tidbSuite struct {
	mockDB  sqlmock.Sqlmock
	handler http.Handler
	timgr   *TiDBManager
}

func TestTiDB(t *testing.T) {
	TestingT(t)
}

func (s *tidbSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	s.mockDB = mock
	defaultSQLMode, err := tmysql.GetSQLMode(tmysql.DefaultSQLMode)
	c.Assert(err, IsNil)

	s.timgr = NewTiDBManagerWithDB(db, defaultSQLMode)
}

func (s *tidbSuite) TearDownTest(c *C) {
	s.timgr.Close()
	c.Assert(s.mockDB.ExpectationsWereMet(), IsNil)
}

func (s *tidbSuite) TestCreateTableIfNotExistsStmt(c *C) {
	createTableIfNotExistsStmt := func(createTable, tableName string) string {
		res, err := s.timgr.createTableIfNotExistsStmt(createTable, tableName)
		c.Assert(err, IsNil)
		return res
	}

	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `foo`(`bar` TINYINT(1));", "foo"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `foo` (`bar` TINYINT(1));",
	)

	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS `foo`(`bar` TINYINT(1));", "foo"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `foo` (`bar` TINYINT(1));",
	)

	// case insensitive
	c.Assert(
		createTableIfNotExistsStmt("/* cOmmEnt */ creAte tablE `fOo`(`bar` TinyinT(1));", "fOo"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `fOo` (`bar` TINYINT(1));",
	)

	c.Assert(
		createTableIfNotExistsStmt("/* coMMenT */ crEatE tAble If not EXISts `FoO`(`bAR` tiNyInT(1));", "FoO"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `FoO` (`bAR` TINYINT(1));",
	)

	// only one "CREATE TABLE" is replaced
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE');", "foo"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `foo` (`bar` INT(1) COMMENT 'CREATE TABLE');",
	)

	// upper case becomes shorter
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `ſ`(`ı` TINYINT(1));", "ſ"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `ſ` (`ı` TINYINT(1));",
	)

	// upper case becomes longer
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `ɑ`(`ȿ` TINYINT(1));", "ɑ"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `ɑ` (`ȿ` TINYINT(1));",
	)

	// non-utf-8
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `\xcc\xcc\xcc`(`\xdd\xdd\xdd` TINYINT(1));", "\xcc\xcc\xcc"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `\xcc\xcc\xcc` (`ÝÝÝ` TINYINT(1));",
	)

	// renaming a table
	c.Assert(
		createTableIfNotExistsStmt("create table foo(x int);", "ba`r"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `ba``r` (`x` INT);",
	)

	// conditional comments
	c.Assert(
		createTableIfNotExistsStmt(`
			/*!40101 SET NAMES binary*/;
			/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
			CREATE TABLE x.y (z double) ENGINE=InnoDB AUTO_INCREMENT=8343230 DEFAULT CHARSET=utf8;
		`, "m"),
		Equals,
		"SET NAMES 'binary';SET @@SESSION.`FOREIGN_KEY_CHECKS`=0;CREATE TABLE IF NOT EXISTS `m` (`z` DOUBLE) ENGINE = InnoDB AUTO_INCREMENT = 8343230 DEFAULT CHARACTER SET = UTF8;",
	)
}

func (s *tidbSuite) TestInitSchema(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("USE `db`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	s.mockDB.
		ExpectExec("\\QCREATE TABLE IF NOT EXISTS `t1` (`a` INT PRIMARY KEY,`b` VARCHAR(200));\\E").
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mockDB.
		ExpectExec("\\QSET @@SESSION.`FOREIGN_KEY_CHECKS`=0;CREATE TABLE IF NOT EXISTS `t2` (`xx` TEXT) AUTO_INCREMENT = 11203;\\E").
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mockDB.
		ExpectClose()

	s.mockDB.MatchExpectationsInOrder(false) // maps are unordered.
	err := s.timgr.InitSchema(ctx, "db", map[string]string{
		"t1": "create table t1 (a int primary key, b varchar(200));",
		"t2": "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;CREATE TABLE `db`.`t2` (xx TEXT) AUTO_INCREMENT=11203;",
	})
	s.mockDB.MatchExpectationsInOrder(true)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestInitSchemaSyntaxError(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("USE `db`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	s.mockDB.
		ExpectClose()

	err := s.timgr.InitSchema(ctx, "db", map[string]string{
		"t1": "create table `t1` with invalid syntax;",
	})
	c.Assert(err, NotNil)
}

func (s *tidbSuite) TestInitSchemaUnsupportedSchemaError(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("USE `db`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	s.mockDB.
		ExpectExec("CREATE TABLE IF NOT EXISTS `t1`.*").
		WillReturnError(&mysql.MySQLError{
			Number:  tmysql.ErrTooBigFieldlength,
			Message: "Column length too big",
		})
	s.mockDB.
		ExpectClose()

	err := s.timgr.InitSchema(ctx, "db", map[string]string{
		"t1": "create table `t1` (a VARCHAR(999999999));",
	})
	c.Assert(err, ErrorMatches, ".*Column length too big.*")
}

func (s *tidbSuite) TestDropTable(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("DROP TABLE `db`.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := s.timgr.DropTable(ctx, "`db`.`table`")
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestLoadSchemaInfo(c *C) {
	ctx := context.Background()

	// Prepare the mock reply.
	nodes, _, err := s.timgr.parser.Parse(
		"CREATE TABLE `t1` (`a` INT PRIMARY KEY);"+
			"CREATE TABLE `t2` (`b` VARCHAR(20), `c` BOOL, KEY (`b`, `c`))",
		"", "")
	c.Assert(err, IsNil)
	tableInfos := make([]*model.TableInfo, 0, len(nodes))
	sctx := mock.NewContext()
	for i, node := range nodes {
		c.Assert(node, FitsTypeOf, &ast.CreateTableStmt{})
		info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), int64(i+100))
		c.Assert(err, IsNil)
		info.State = model.StatePublic
		tableInfos = append(tableInfos, info)
	}

	loaded, err := s.timgr.LoadSchemaInfo(ctx, []*mydump.MDDatabaseMeta{{Name: "db"}}, func(schema string) ([]*model.TableInfo, error) {
		c.Assert(schema, Equals, "db")
		return tableInfos, nil
	})
	c.Assert(err, IsNil)
	c.Assert(loaded, DeepEquals, map[string]*checkpoints.TidbDBInfo{
		"db": {
			Name: "db",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t1": {
					ID:   100,
					Name: "t1",
					Core: tableInfos[0],
				},
				"t2": {
					ID:   101,
					Name: "t2",
					Core: tableInfos[1],
				},
			},
		},
	})
}

func (s *tidbSuite) TestLoadSchemaInfoMissing(c *C) {
	ctx := context.Background()

	_, err := s.timgr.LoadSchemaInfo(ctx, []*mydump.MDDatabaseMeta{{Name: "asdjalsjdlas"}}, func(schema string) ([]*model.TableInfo, error) {
		return nil, errors.Errorf("[schema:1049]Unknown database '%s'", schema)
	})
	c.Assert(err, ErrorMatches, ".*Unknown database.*")
}

func (s *tidbSuite) TestGetGCLifetime(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	s.mockDB.
		ExpectClose()

	res, err := ObtainGCLifeTime(ctx, s.timgr.db)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, "10m")
}

func (s *tidbSuite) TestSetGCLifetime(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("12m").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := UpdateGCLifeTime(ctx, s.timgr.db, "12m")
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestAlterAutoInc(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_INCREMENT=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoIncrement(ctx, s.timgr.db, "`db`.`table`", 12345)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestAlterAutoRandom(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_RANDOM_BASE=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoRandom(ctx, s.timgr.db, "`db`.`table`", 12345)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestObtainRowFormatVersionSucceed(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectQuery("\\QSELECT @@tidb_row_format_version\\E").
		WillReturnRows(sqlmock.NewRows([]string{"@@tidb_row_format_version"}).AddRow("2"))
	s.mockDB.
		ExpectClose()

	version := ObtainRowFormatVersion(ctx, s.timgr.db)
	c.Assert(version, Equals, "2")
}

func (s *tidbSuite) TestObtainRowFormatVersionFailure(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectQuery("\\QSELECT @@tidb_row_format_version\\E").
		WillReturnError(errors.New("ERROR 1193 (HY000): Unknown system variable 'tidb_row_format_version'"))
	s.mockDB.
		ExpectClose()

	version := ObtainRowFormatVersion(ctx, s.timgr.db)
	c.Assert(version, Equals, "1")
}
