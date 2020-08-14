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
	"path/filepath"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
	md "github.com/pingcap/tidb-lightning/lightning/mydump"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
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

func newConfigWithSourceDir(sourceDir string) *config.Config {
	return &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir:        sourceDir,
			Filter:           []string{"*.*"},
			DefaultFileRules: true,
		},
	}
}

func (s *testMydumpLoaderSuite) SetUpTest(c *C) {
	s.cfg = newConfigWithSourceDir(c.MkDir())
}

func (s *testMydumpLoaderSuite) touch(c *C, filename ...string) string {
	components := make([]string, len(filename)+1)
	components = append(components, s.cfg.Mydumper.SourceDir)
	components = append(components, filename...)
	path := filepath.Join(components...)
	err := ioutil.WriteFile(path, nil, 0644)
	c.Assert(err, IsNil)
	return path
}

func (s *testMydumpLoaderSuite) mkdir(c *C, dirname string) {
	path := filepath.Join(s.cfg.Mydumper.SourceDir, dirname)
	err := os.Mkdir(path, 0755)
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestLoader(c *C) {
	cfg := newConfigWithSourceDir("./not-exists")
	_, err := md.NewMyDumpLoader(cfg)
	c.Assert(err, NotNil)

	cfg = newConfigWithSourceDir("./examples")
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
		{name: "tbl_multi_index", dataFiles: 1},
		{name: "tbl_autoid", dataFiles: 1},
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
	s.mkdir(c, "a")
	s.touch(c, "a", "db-schema-create.sql")
	s.mkdir(c, "b")
	s.touch(c, "b", "db-schema-create.sql")

	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid database schema file, duplicated item - .*[/\\]db-schema-create\.sql`)
}

func (s *testMydumpLoaderSuite) TestTableNoHostDB(c *C) {
	/*
		path/
			notdb-schema-create.sql
			db.tbl-schema.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(filepath.Join(dir, "notdb-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(filepath.Join(dir, "db.tbl-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, cannot find db 'db' - .*[/\\]db\.tbl-schema\.sql`)
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

	s.touch(c, "db-schema-create.sql")
	s.mkdir(c, "a")
	s.touch(c, "a", "db.tbl-schema.sql")
	s.mkdir(c, "b")
	s.touch(c, "b", "db.tbl-schema.sql")

	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, duplicated item - .*[/\\]db\.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostDB(c *C) {
	/*
		path/
			notdb-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "notdb-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host db 'db' - .*[/\\]db\.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostTable(c *C) {
	/*
		path/
			db-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host table 'tbl' - .*[/\\]db\.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataWithoutSchema(c *C) {
	dir := s.cfg.Mydumper.SourceDir
	p := filepath.Join(dir, "db.tbl.sql")
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
	pDBSchema := s.touch(c, "db-schema-create.sql")
	pT1Schema := s.touch(c, "db.tbl.with.dots-schema.sql")
	pT1Data := s.touch(c, "db.tbl.with.dots.0001.sql")
	pT2Schema := s.touch(c, "db.0002-schema.sql")
	pT2Data := s.touch(c, "db.0002.sql")

	// insert some tables with file name structures which we're going to ignore.
	s.touch(c, "db.v-schema-view.sql")
	s.touch(c, "db.v-schema-trigger.sql")
	s.touch(c, "db.v-schema-post.sql")
	s.touch(c, "db.sql")
	s.touch(c, "db-schema.sql")

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

func (s *testMydumpLoaderSuite) TestRouter(c *C) {
	s.cfg.Routes = []*router.TableRule{
		{
			SchemaPattern: "a*",
			TablePattern:  "t*",
			TargetSchema:  "b",
			TargetTable:   "u",
		},
		{
			SchemaPattern: "c*",
			TargetSchema:  "c",
		},
	}

	/*
		path/
			a0-schema-create.sql
			a0.t0-schema.sql
			a0.t0.1.sql
			a0.t1-schema.sql
			a0.t1.1.sql
			a1-schema-create.sql
			a1.s1-schema.sql
			a1.s1.1.schema.sql
			a1.t2-schema.sql
			a1.t2.1.sql
			c0-schema-create.sql
			c0.t3-schema.sql
			c0.t3.1.sql
			d0-schema-create.sql
	*/

	pA0SchemaCreate := s.touch(c, "a0-schema-create.sql")
	pA0T0Schema := s.touch(c, "a0.t0-schema.sql")
	pA0T0Data := s.touch(c, "a0.t0.1.sql")
	_ = s.touch(c, "a0.t1-schema.sql")
	pA0T1Data := s.touch(c, "a0.t1.1.sql")

	pA1SchemaCreate := s.touch(c, "a1-schema-create.sql")
	pA1S1Schema := s.touch(c, "a1.s1-schema.sql")
	pA1S1Data := s.touch(c, "a1.s1.1.sql")
	_ = s.touch(c, "a1.t2-schema.sql")
	pA1T2Data := s.touch(c, "a1.t2.1.sql")

	pC0SchemaCreate := s.touch(c, "c0-schema-create.sql")
	pC0T3Schema := s.touch(c, "c0.t3-schema.sql")
	pC0T3Data := s.touch(c, "c0.t3.1.sql")

	pD0SchemaCreate := s.touch(c, "d0-schema-create.sql")

	mdl, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{
		{
			Name:       "a1",
			SchemaFile: pA1SchemaCreate,
			Tables: []*md.MDTableMeta{
				{
					DB:         "a1",
					Name:       "s1",
					SchemaFile: pA1S1Schema,
					DataFiles:  []string{pA1S1Data},
				},
			},
		},
		{
			Name:       "d0",
			SchemaFile: pD0SchemaCreate,
		},
		{
			Name:       "b",
			SchemaFile: pA0SchemaCreate,
			Tables: []*md.MDTableMeta{
				{
					DB:         "b",
					Name:       "u",
					SchemaFile: pA0T0Schema,
					DataFiles:  []string{pA0T0Data, pA0T1Data, pA1T2Data},
				},
			},
		},
		{
			Name:       "c",
			SchemaFile: pC0SchemaCreate,
			Tables: []*md.MDTableMeta{
				{
					DB:         "c",
					Name:       "t3",
					SchemaFile: pC0T3Schema,
					DataFiles:  []string{pC0T3Data},
				},
			},
		},
	})
}

func (s *testMydumpLoaderSuite) TestBadRouterRule(c *C) {
	s.cfg.Routes = []*router.TableRule{{
		SchemaPattern: "a*b",
		TargetSchema:  "ab",
	}}

	_, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, ErrorMatches, `.*pattern a\*b not valid`)
}

func (s *testMydumpLoaderSuite) TestFileRouting(c *C) {
	s.cfg.Mydumper.DefaultFileRules = false
	s.cfg.Mydumper.FileRouters = []*config.FileRouteRule{
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z0-9_]+)/schema\.sql$`,
			Schema:  "$1",
			Type:    "schema-schema",
		},
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z0-9]+)/([a-z0-9_]+)-table\.sql$`,
			Schema:  "$1",
			Table:   "$2",
			Type:    "table-schema",
		},
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z][a-z0-9_]*)/([a-z]+)[0-9]*(?:\.([0-9]+))?\.(sql|csv)$`,
			Schema:  "$1",
			Table:   "$2",
			Type:    "$4",
		},
		{
			Pattern: `^(?:[^./]*/)*([a-z]+)(?:\.([0-9]+))?\.(sql|csv)$`,
			Schema:  "d2",
			Table:   "$1",
			Type:    "$3",
		},
	}

	s.mkdir(c, "d1")
	s.mkdir(c, "d2")
	d1Schema := s.touch(c, "d1/schema.sql")
	d1TestTable := s.touch(c, "d1/test-table.sql")
	d1TestData0 := s.touch(c, "d1/test0.sql")
	d1TestData1 := s.touch(c, "d1/test1.sql")
	d1TestData2 := s.touch(c, "d1/test2.001.sql")
	_ = s.touch(c, "d1/t1-schema-create.sql")
	d2Schema := s.touch(c, "d2/schema.sql")
	d2TestTable := s.touch(c, "d2/abc-table.sql")
	d2AbcData0 := s.touch(c, "abc.1.sql")

	mdl, err := md.NewMyDumpLoader(s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{
		{
			Name:       "d1",
			SchemaFile: d1Schema,
			Tables: []*md.MDTableMeta{
				{
					DB:         "d1",
					Name:       "test",
					SchemaFile: d1TestTable,
					DataFiles:  []string{d1TestData0, d1TestData1, d1TestData2},
				},
			},
		},
		{
			Name:       "d2",
			SchemaFile: d2Schema,
			Tables: []*md.MDTableMeta{
				{
					DB:         "d2",
					Name:       "abc",
					SchemaFile: d2TestTable,
					DataFiles:  []string{d2AbcData0},
				},
			},
		},
	})
}
