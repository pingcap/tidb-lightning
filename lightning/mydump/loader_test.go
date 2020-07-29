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

package mydump

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
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
			SourceDir: sourceDir,
			Filter:    []string{"*.*"},
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
	ctx := context.Background()
	cfg := newConfigWithSourceDir("./not-exists")
	_, err := NewMyDumpLoader(ctx, cfg)
	c.Assert(err, NotNil)

	cfg = newConfigWithSourceDir("./examples")
	mdl, err := NewMyDumpLoader(ctx, cfg)
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
	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `missing \{schema\}-schema-create\.sql.*`)
}

func (s *testMydumpLoaderSuite) TestDuplicatedDB(c *C) {
	/*
		Path/
			a/
				db-schema-create.sql
			b/
				db-schema-create.sql
	*/
	s.mkdir(c, "a")
	s.touch(c, "a", "db-schema-create.sql")
	s.mkdir(c, "b")
	s.touch(c, "b", "db-schema-create.sql")

	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid database schema file, duplicated item - .*[/\\]db-schema-create\.sql`)
}

func (s *testMydumpLoaderSuite) TestTableNoHostDB(c *C) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl-schema.sql
	*/

	dir := s.cfg.Mydumper.SourceDir
	err := ioutil.WriteFile(filepath.Join(dir, "notdb-schema-create.sql"), nil, 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(filepath.Join(dir, "db.tbl-schema.sql"), nil, 0644)
	c.Assert(err, IsNil)

	_, err = NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, cannot find db - .*[/\\]db\.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestDuplicatedTable(c *C) {
	/*
		Path/
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

	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, duplicated item - .*[/\\]db\.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostDB(c *C) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "notdb-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host db - .*[/\\]db\.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataNoHostTable(c *C) {
	/*
		Path/
			db-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid data file, miss host table - .*[/\\]db\.tbl\.sql`)
}

func (s *testMydumpLoaderSuite) TestDataWithoutSchema(c *C) {
	dir := s.cfg.Mydumper.SourceDir
	p := filepath.Join(dir, "db.tbl.sql")
	err := ioutil.WriteFile(p, nil, 0644)
	c.Assert(err, IsNil)

	s.cfg.Mydumper.NoSchema = true

	mdl, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: "",
		Tables: []*MDTableMeta{{
			DB:         "db",
			Name:       "tbl",
			SchemaFile: fileInfo{},
			DataFiles:  []fileInfo{{Path: p, Size: 0}},
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

	mdl, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: pDBSchema,
		Tables: []*MDTableMeta{
			{
				DB:         "db",
				Name:       "0002",
				SchemaFile: fileInfo{Path: pT2Schema},
				DataFiles:  []fileInfo{{Path: pT2Data, Size: 0}},
			},
			{
				DB:         "db",
				Name:       "tbl.with.dots",
				SchemaFile: fileInfo{Path: pT1Schema},
				DataFiles:  []fileInfo{{Path: pT1Data, Size: 0}},
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
		Path/
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

	mdl, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)

	c.Assert(mdl.GetDatabases(), DeepEquals, []*MDDatabaseMeta{
		{
			Name:       "a1",
			SchemaFile: pA1SchemaCreate,
			Tables: []*MDTableMeta{
				{
					DB:         "a1",
					Name:       "s1",
					SchemaFile: fileInfo{Path: pA1S1Schema},
					DataFiles:  []fileInfo{{Path: pA1S1Data}},
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
			Tables: []*MDTableMeta{
				{
					DB:         "b",
					Name:       "u",
					SchemaFile: fileInfo{Path: pA0T0Schema},
					DataFiles:  []fileInfo{{Path: pA0T0Data}, {Path: pA0T1Data}, {Path: pA1T2Data}},
				},
			},
		},
		{
			Name:       "c",
			SchemaFile: pC0SchemaCreate,
			Tables: []*MDTableMeta{
				{
					DB:         "c",
					Name:       "t3",
					SchemaFile: fileInfo{Path: pC0T3Schema},
					DataFiles:  []fileInfo{{Path: pC0T3Data}},
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

	s.touch(c, "a1b-schema-create.sql")

	_, err := NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `.*pattern a\*b not valid`)
}
