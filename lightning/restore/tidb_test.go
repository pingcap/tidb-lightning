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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
)

var _ = Suite(&tidbSuite{})

type tidbSuite struct{}

func TestTiDB(t *testing.T) {
	TestingT(t)
}

func (s *tidbSuite) TestCreateTableIfNotExistsStmt(c *C) {
	timgr := &TiDBManager{parser: parser.New()}
	createTableIfNotExistsStmt := func(createTable, tableName string) string {
		res, err := timgr.createTableIfNotExistsStmt(createTable, tableName)
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
