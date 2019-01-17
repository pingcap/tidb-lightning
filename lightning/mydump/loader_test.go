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
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
	md "github.com/pingcap/tidb-lightning/lightning/mydump"
)

var _ = Suite(&testMydumpLoaderSuite{})

func TestMydumps(t *testing.T) {
	TestingT(t)
}

type testMydumpLoaderSuite struct {
	cfg *config.Config
}

func (s *testMydumpLoaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpLoaderSuite) TearDownSuite(c *C) {}

func (s *testMydumpLoaderSuite) SetUpTest(c *C) {
	s.cfg = &config.Config{Mydumper: config.MydumperRuntime{SourceDir: c.MkDir()}}
}

func (s *testMydumpLoaderSuite) TestLoader(c *C) {
	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./not-exists"}}
	_, err := md.NewMyDumpLoader(cfg)
	c.Assert(err, NotNil)

	cfg = &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./examples"}}
	mdl, err := md.NewMyDumpLoader(cfg)
	c.Assert(err, IsNil)

	dbMetas := mdl.GetDatabases()
	c.Assert(len(dbMetas), Equals, 1)
	dbMeta := dbMetas[0]
	c.Assert(dbMeta.Name, Equals, "mocker_test")
	c.Assert(len(dbMeta.Tables), Equals, 4)

	expected := []struct {
		name      string
		dataFiles int
	}{
		{name: "i", dataFiles: 1},
		{name: "report_case_high_risk", dataFiles: 1},
		{name: "tbl_autoid", dataFiles: 1},
		{name: "tbl_multi_index", dataFiles: 1},
	}

	for i, table := range expected {
		c.Assert(dbMeta.Tables[i].Name, Equals, table.name)
		c.Assert(len(dbMeta.Tables[i].DataFiles), Equals, table.dataFiles)
	}
}

func (s *testMydumpLoaderSuite) TestEmptyDB(c *C) {
	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `missing \{schema\}-schema-create\.sql.*`)
}

func (s *testMydumpLoaderSuite) TestDuplicatedDB(c *C) {
	/*
		path/
			a/
				db-schema-create.sql
			b/
				db-schema-create.sql
	*/
	dir := s.cfg.Mydumper.SourceDir
	err := os.Mkdir(path.Join(dir, "a"), 0755)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "a", "db-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = os.Mkdir(path.Join(dir, "b"), 0755)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "b", "db-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid database schema file, duplicated item - .*/db-schema-create\.sql`)
}

func (s *testMydumpLoaderSuite) TestTableNoHostDB(c *C) {
	/*
		path/
			notdb-schema-create.sql
			db.tbl-schema.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(path.Join(dir, "notdb-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.tbl-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, cannot find db - .*/db.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestDuplicatedTable(c *C) {
	/*
		path/
			db-schema-create.sql
			a/
				db.tbl-schema.sql
			b/
				db.tbl-schema.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(path.Join(dir, "db-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = os.Mkdir(path.Join(dir, "a"), 0755)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "a", "db.tbl-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = os.Mkdir(path.Join(dir, "b"), 0755)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "b", "db.tbl-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, duplicated item - .*/db.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostDB(c *C) {
	/*
		path/
			notdb-schema-create.sql
			db.tbl.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(path.Join(dir, "notdb-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.tbl.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host db - .*/db.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostTable(c *C) {
	/*
		path/
			db-schema-create.sql
			db.tbl.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(path.Join(dir, "db-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.tbl.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host table - .*/db.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataWithoutSchema(c *C) {
	dir := s.cfg.Mydumper.SourceDir
	p := path.Join(dir, "db.tbl.sql")
	err := ioutil.WriteFile(p, nil, 0644)
	c.Assert(err, IsNil)

	s.cfg.Mydumper.NoSchema = true

	mdl, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: "",
		Tables: []*md.MDTableMeta{{
			DB:         "db",
			Name:       "tbl",
			SchemaFile: "",
			DataFiles:  []string{p},
		}},
	}})
}

func (s *testMydumpLoaderSuite) TestTablesWithDots(c *C) {
	dir := s.cfg.Mydumper.SourceDir

	pDBSchema := path.Join(dir, "db-schema-create.sql")
	err := ioutil.WriteFile(pDBSchema, nil, 0644)
	c.Assert(err, IsNil)

	pT1Schema := path.Join(dir, "db.tbl.with.dots-schema.sql")
	err = ioutil.WriteFile(pT1Schema, nil, 0644)
	c.Assert(err, IsNil)

	pT1Data := path.Join(dir, "db.tbl.with.dots.0001.sql")
	err = ioutil.WriteFile(pT1Data, nil, 0644)
	c.Assert(err, IsNil)

	pT2Schema := path.Join(dir, "db.0002-schema.sql")
	err = ioutil.WriteFile(pT2Schema, nil, 0644)
	c.Assert(err, IsNil)

	pT2Data := path.Join(dir, "db.0002.sql")
	err = ioutil.WriteFile(pT2Data, nil, 0644)
	c.Assert(err, IsNil)

	// insert some tables with file name structures which we're going to ignore.
	err = ioutil.WriteFile(path.Join(dir, "db.v-schema-view.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.v-schema-trigger.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.v-schema-post.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "db-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)

	mdl, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: pDBSchema,
		Tables: []*md.MDTableMeta{
			{
				DB:         "db",
				Name:       "0002",
				SchemaFile: pT2Schema,
				DataFiles:  []string{pT2Data},
			},
			{
				DB:         "db",
				Name:       "tbl.with.dots",
				SchemaFile: pT1Schema,
				DataFiles:  []string{pT1Data},
			},
		},
	}})
}
