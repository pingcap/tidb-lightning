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
)

var _ = Suite(&tidbSuite{})

type tidbSuite struct{}

func TestTiDB(t *testing.T) {
	TestingT(t)
}

func (s *tidbSuite) TestCreateTableIfNotExistsStmt(c *C) {
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `foo`(`bar` TINYINT(1));"),
		Equals,
		"CREATE TABLE IF NOT EXISTS  `foo`(`bar` TINYINT(1));",
	)

	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS `foo`(`bar` TINYINT(1));"),
		Equals,
		"CREATE TABLE IF NOT EXISTS `foo`(`bar` TINYINT(1));",
	)

	// case insensitive
	c.Assert(
		createTableIfNotExistsStmt("/* cOmmEnt */ creAte tablE `fOo`(`bar` TinyinT(1));"),
		Equals,
		"/* cOmmEnt */ creAte tablE IF NOT EXISTS  `fOo`(`bar` TinyinT(1));",
	)

	c.Assert(
		createTableIfNotExistsStmt("/* coMMenT */ crEatE tAble If not EXISts `FoO`(`bAR` tiNyInT(1));"),
		Equals,
		"/* coMMenT */ crEatE tAble If not EXISts `FoO`(`bAR` tiNyInT(1));",
	)

	// only one "CREATE TABLE" is replaced
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE');"),
		Equals,
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS  `foo`(`bar` INT(1) COMMENT 'CREATE TABLE');"),
	)

	// upper case becomes shorter
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `ſ`(`ı` TINYINT(1));"),
		Equals,
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS  `ſ`(`ı` TINYINT(1));"),
	)

	// upper case becomes longer
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `ɑ`(`ȿ` TINYINT(1));"),
		Equals,
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS  `ɑ`(`ȿ` TINYINT(1));"),
	)

	// non-utf-8
	c.Assert(
		createTableIfNotExistsStmt("CREATE TABLE `\xcc\xcc\xcc`(`\xdd\xdd\xdd` TINYINT(1));"),
		Equals,
		createTableIfNotExistsStmt("CREATE TABLE IF NOT EXISTS  `\xcc\xcc\xcc`(`\xdd\xdd\xdd` TINYINT(1));"),
	)
}
