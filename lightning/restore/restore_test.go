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
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/glue"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/ddl"
	tmock "github.com/pingcap/tidb/util/mock"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	. "github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/verification"
	"github.com/pingcap/tidb-lightning/lightning/worker"
	"github.com/pingcap/tidb-lightning/mock"
)

var _ = Suite(&restoreSuite{})

type restoreSuite struct{}

func (s *restoreSuite) TestNewTableRestore(c *C) {
	testCases := []struct {
		name       string
		createStmt string
	}{
		{"t1", "CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)"},
		// {"t2", "CREATE TABLE `t2` (`c1` varchar(30000) NOT NULL)"}, // no longer able to create this kind of table.
		{"t3", "CREATE TABLE `t3-a` (`c1-a` varchar(5) NOT NULL)"},
	}

	p := parser.New()
	se := tmock.NewContext()

	dbInfo := &TidbDBInfo{Name: "mockdb", Tables: map[string]*TidbTableInfo{}}
	for i, tc := range testCases {
		node, err := p.ParseOneStmt(tc.createStmt, "utf8mb4", "utf8mb4_bin")
		c.Assert(err, IsNil)
		tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(i+1))
		c.Assert(err, IsNil)
		tableInfo.State = model.StatePublic

		dbInfo.Tables[tc.name] = &TidbTableInfo{
			Name: tc.name,
			Core: tableInfo,
		}
	}

	for _, tc := range testCases {
		tableInfo := dbInfo.Tables[tc.name]
		tableName := common.UniqueTable("mockdb", tableInfo.Name)
		tr, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &TableCheckpoint{})
		c.Assert(tr, NotNil)
		c.Assert(err, IsNil)
	}
}

func (s *restoreSuite) TestNewTableRestoreFailure(c *C) {
	tableInfo := &TidbTableInfo{
		Name: "failure",
		Core: &model.TableInfo{},
	}
	dbInfo := &TidbDBInfo{Name: "mockdb", Tables: map[string]*TidbTableInfo{
		"failure": tableInfo,
	}}
	tableName := common.UniqueTable("mockdb", "failure")

	_, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &TableCheckpoint{})
	c.Assert(err, ErrorMatches, `failed to tables\.TableFromMeta.*`)
}

func (s *restoreSuite) TestErrorSummaries(c *C) {
	logger, buffer := log.MakeTestLogger()

	es := makeErrorSummaries(logger)
	es.record("first", errors.New("a1 error"), CheckpointStatusAnalyzed)
	es.record("second", errors.New("b2 error"), CheckpointStatusAllWritten)
	es.emitLog()

	lines := buffer.Lines()
	sort.Strings(lines[1:])
	c.Assert(lines, DeepEquals, []string{
		`{"$lvl":"ERROR","$msg":"tables failed to be imported","count":2}`,
		`{"$lvl":"ERROR","$msg":"-","table":"first","status":"analyzed","error":"a1 error"}`,
		`{"$lvl":"ERROR","$msg":"-","table":"second","status":"written","error":"b2 error"}`,
	})
}

func (s *restoreSuite) TestVerifyCheckpoint(c *C) {
	dir := c.MkDir()
	cpdb := checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))
	defer cpdb.Close()
	ctx := context.Background()

	actualReleaseVersion := common.ReleaseVersion
	defer func() {
		common.ReleaseVersion = actualReleaseVersion
	}()

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	c.Assert(err, IsNil)
	c.Assert(taskCp, IsNil)

	newCfg := func() *config.Config {
		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "/data"
		cfg.TaskID = 123
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "127.0.0.1:2379"
		cfg.TikvImporter.Addr = "127.0.0.1:8287"
		cfg.TikvImporter.SortedKVDir = "/tmp/sorted-kv"

		return cfg
	}

	err = cpdb.Initialize(ctx, newCfg(), map[string]*checkpoints.TidbDBInfo{})
	c.Assert(err, IsNil)

	adjustFuncs := map[string]func(cfg *config.Config){
		"tikv-importer.backend": func(cfg *config.Config) {
			cfg.TikvImporter.Backend = "local"
		},
		"tikv-importer.addr": func(cfg *config.Config) {
			cfg.TikvImporter.Addr = "128.0.0.1:8287"
		},
		"mydumper.data-source-dir": func(cfg *config.Config) {
			cfg.Mydumper.SourceDir = "/tmp/test"
		},
		"tidb.host": func(cfg *config.Config) {
			cfg.TiDB.Host = "192.168.0.1"
		},
		"tidb.port": func(cfg *config.Config) {
			cfg.TiDB.Port = 5000
		},
		"tidb.pd-addr": func(cfg *config.Config) {
			cfg.TiDB.PdAddr = "127.0.0.1:3379"
		},
		"version": func(cfg *config.Config) {
			common.ReleaseVersion = "some newer version"
		},
	}

	// default mode, will return error
	taskCp, err = cpdb.TaskCheckpoint(ctx)
	c.Assert(err, IsNil)
	for conf, fn := range adjustFuncs {
		cfg := newCfg()
		fn(cfg)
		err := verifyCheckpoint(cfg, taskCp)
		if conf == "version" {
			common.ReleaseVersion = actualReleaseVersion
			c.Assert(err, ErrorMatches, "lightning version is 'some newer version', but checkpoint was created at '"+actualReleaseVersion+"'.*")
		} else {
			c.Assert(err, ErrorMatches, fmt.Sprintf("config '%s' value '.*' different from checkpoint value .*", conf))
		}
	}

	for conf, fn := range adjustFuncs {
		if conf == "tikv-importer.backend" {
			continue
		}
		cfg := newCfg()
		cfg.App.CheckRequirements = false
		fn(cfg)
		err := cpdb.Initialize(context.Background(), cfg, map[string]*checkpoints.TidbDBInfo{})
		c.Assert(err, IsNil)
	}
}

var _ = Suite(&tableRestoreSuite{})

type tableRestoreSuiteBase struct {
	tr  *TableRestore
	cfg *config.Config

	tableInfo *TidbTableInfo
	dbInfo    *TidbDBInfo
	tableMeta *mydump.MDTableMeta

	store storage.ExternalStorage
}

type tableRestoreSuite struct {
	tableRestoreSuiteBase
}

func (s *tableRestoreSuiteBase) SetUpSuite(c *C) {
	// Produce a mock table info

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()
	node, err := p.ParseOneStmt(`
		CREATE TABLE "table" (
			a INT,
			b INT,
			c INT,
			KEY (b)
		)
	`, "", "")
	c.Assert(err, IsNil)
	core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
	c.Assert(err, IsNil)
	core.State = model.StatePublic

	s.tableInfo = &TidbTableInfo{Name: "table", DB: "db", Core: core}
	s.dbInfo = &TidbDBInfo{
		Name:   "db",
		Tables: map[string]*TidbTableInfo{"table": s.tableInfo},
	}

	// Write some sample SQL dump

	fakeDataDir := c.MkDir()

	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)
	s.store = store

	fakeDataFilesCount := 6
	fakeDataFilesContent := []byte("INSERT INTO `table` VALUES (1, 2, 3);")
	c.Assert(len(fakeDataFilesContent), Equals, 37)
	fakeDataFiles := make([]mydump.FileInfo, 0, fakeDataFilesCount)
	for i := 1; i <= fakeDataFilesCount; i++ {
		fakeFileName := fmt.Sprintf("db.table.%d.sql", i)
		fakeDataPath := filepath.Join(fakeDataDir, fakeFileName)
		err = ioutil.WriteFile(fakeDataPath, fakeDataFilesContent, 0644)
		c.Assert(err, IsNil)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{TableName: filter.Table{"db", "table"}, FileMeta: mydump.SourceFileMeta{Path: fakeFileName, Type: mydump.SourceTypeSQL, SortKey: fmt.Sprintf("%d", i), FileSize: 37}})
	}

	fakeCsvContent := []byte("1,2,3\r\n4,5,6\r\n")
	csvName := "db.table.99.csv"
	err = ioutil.WriteFile(filepath.Join(fakeDataDir, csvName), fakeCsvContent, 0644)
	c.Assert(err, IsNil)
	fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{TableName: filter.Table{"db", "table"}, FileMeta: mydump.SourceFileMeta{Path: csvName, Type: mydump.SourceTypeCSV, SortKey: "99", FileSize: 14}})

	s.tableMeta = &mydump.MDTableMeta{
		DB:         "db",
		Name:       "table",
		TotalSize:  222,
		SchemaFile: mydump.FileInfo{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: mydump.SourceFileMeta{Path: "db.table-schema.sql", Type: mydump.SourceTypeTableSchema}},
		DataFiles:  fakeDataFiles,
	}
}

func (s *tableRestoreSuiteBase) SetUpTest(c *C) {
	// Collect into the test TableRestore structure
	var err error
	s.tr, err = NewTableRestore("`db`.`table`", s.tableMeta, s.dbInfo, s.tableInfo, &TableCheckpoint{})
	c.Assert(err, IsNil)

	s.cfg = config.NewConfig()
	s.cfg.Mydumper.BatchSize = 111
	s.cfg.App.TableConcurrency = 2
}

func (s *tableRestoreSuite) TestPopulateChunks(c *C) {
	failpoint.Enable("github.com/pingcap/tidb-lightning/lightning/restore/PopulateChunkTimestamp", "return(1234567897)")
	defer failpoint.Disable("github.com/pingcap/tidb-lightning/lightning/restore/PopulateChunkTimestamp")

	cp := &TableCheckpoint{
		Engines: make(map[int32]*EngineCheckpoint),
	}

	rc := &RestoreController{cfg: s.cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: s.store}
	err := s.tr.populateChunks(context.Background(), rc, cp)
	c.Assert(err, IsNil)
	c.Assert(cp.Engines, DeepEquals, map[int32]*EngineCheckpoint{
		-1: {
			Status: CheckpointStatusLoaded,
		},
		0: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 0,
						RowIDMax:     7, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 7,
						RowIDMax:     14,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[2].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[2].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 14,
						RowIDMax:     21,
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[3].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[3].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 21,
						RowIDMax:     28,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[4].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[4].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 28,
						RowIDMax:     35,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[5].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[5].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 35,
						RowIDMax:     42,
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[6].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[6].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 42,
						RowIDMax:     46,
					},
					Timestamp: 1234567897,
				},
			},
		},
	})

	// set csv header to true, this will cause check columns fail
	s.cfg.Mydumper.CSV.Header = true
	s.cfg.Mydumper.StrictFormat = true
	regionSize := s.cfg.Mydumper.MaxRegionSize
	s.cfg.Mydumper.MaxRegionSize = 5
	err = s.tr.populateChunks(context.Background(), rc, cp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `.*unknown columns in header \[1 2 3\]`)
	s.cfg.Mydumper.MaxRegionSize = regionSize
	s.cfg.Mydumper.CSV.Header = false
}

func (s *tableRestoreSuite) TestPopulateChunksCSVHeader(c *C) {
	fakeDataDir := c.MkDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)

	fakeDataFiles := make([]mydump.FileInfo, 0)

	fakeCsvContents := []string{
		// small full header
		"a,b,c\r\n1,2,3\r\n",
		// small partial header
		"b,c\r\n2,3\r\n",
		// big full header
		"a,b,c\r\n90000,80000,700000\r\n1000,2000,3000\r\n11,22,33\r\n3,4,5\r\n",
		// big full header unordered
		"c,a,b\r\n,1000,2000,3000\r\n11,22,33\r\n1000,2000,404\r\n3,4,5\r\n90000,80000,700000\r\n7999999,89999999,9999999\r\n",
		// big partial header
		"b,c\r\n2000001,30000001\r\n35231616,462424626\r\n62432,434898934\r\n",
	}
	total := 0
	for i, s := range fakeCsvContents {
		csvName := fmt.Sprintf("db.table.%02d.csv", i)
		err := ioutil.WriteFile(filepath.Join(fakeDataDir, csvName), []byte(s), 0644)
		c.Assert(err, IsNil)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
			TableName: filter.Table{"db", "table"},
			FileMeta:  mydump.SourceFileMeta{Path: csvName, Type: mydump.SourceTypeCSV, SortKey: fmt.Sprintf("%02d", i), FileSize: int64(len(s))},
		})
		total += len(s)
	}
	tableMeta := &mydump.MDTableMeta{
		DB:         "db",
		Name:       "table",
		TotalSize:  int64(total),
		SchemaFile: mydump.FileInfo{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: mydump.SourceFileMeta{Path: "db.table-schema.sql", Type: mydump.SourceTypeTableSchema}},
		DataFiles:  fakeDataFiles,
	}

	failpoint.Enable("github.com/pingcap/tidb-lightning/lightning/restore/PopulateChunkTimestamp", "return(1234567897)")
	defer failpoint.Disable("github.com/pingcap/tidb-lightning/lightning/restore/PopulateChunkTimestamp")

	cp := &TableCheckpoint{
		Engines: make(map[int32]*EngineCheckpoint),
	}

	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 100
	cfg.Mydumper.MaxRegionSize = 40

	cfg.Mydumper.CSV.Header = true
	cfg.Mydumper.StrictFormat = true
	rc := &RestoreController{cfg: cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: store}

	tr, err := NewTableRestore("`db`.`table`", tableMeta, s.dbInfo, s.tableInfo, &TableCheckpoint{})
	c.Assert(err, IsNil)
	c.Assert(tr.populateChunks(context.Background(), rc, cp), IsNil)

	c.Assert(cp.Engines, DeepEquals, map[int32]*EngineCheckpoint{
		-1: {
			Status: CheckpointStatusLoaded,
		},
		0: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:      ChunkCheckpointKey{Path: tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 0,
						RowIDMax:     4, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      ChunkCheckpointKey{Path: tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    10,
						PrevRowIDMax: 4,
						RowIDMax:     7,
					},
					Timestamp: 1234567897,
				},
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    52,
						PrevRowIDMax: 7,
						RowIDMax:     20,
						Columns:      []string{"a", "b", "c"},
					},

					Timestamp: 1234567897,
				},
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 52},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       52,
						EndOffset:    60,
						PrevRowIDMax: 20,
						RowIDMax:     22,
						Columns:      []string{"a", "b", "c"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    48,
						PrevRowIDMax: 22,
						RowIDMax:     35,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 48},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       48,
						EndOffset:    101,
						PrevRowIDMax: 35,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 101},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       101,
						EndOffset:    102,
						PrevRowIDMax: 48,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 4},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       4,
						EndOffset:    59,
						PrevRowIDMax: 48,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key:               ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 59},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       59,
						EndOffset:    60,
						PrevRowIDMax: 61,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
	})
}

func (s *tableRestoreSuite) TestGetColumnsNames(c *C) {
	c.Assert(getColumnNames(s.tableInfo.Core, []int{0, 1, 2, -1}), DeepEquals, []string{"a", "b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 0, 2, -1}), DeepEquals, []string{"b", "a", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, 1, -1}), DeepEquals, []string{"b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{0, 1, -1, -1}), DeepEquals, []string{"a", "b"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, -1, 0, -1}), DeepEquals, []string{"c", "a"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, -1, -1}), DeepEquals, []string{"b"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 2, 3, 0}), DeepEquals, []string{"_tidb_rowid", "a", "b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 0, 2, 3}), DeepEquals, []string{"b", "a", "c", "_tidb_rowid"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, 2, 1}), DeepEquals, []string{"b", "_tidb_rowid", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{2, -1, 0, 1}), DeepEquals, []string{"c", "_tidb_rowid", "a"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 1, -1, 0}), DeepEquals, []string{"_tidb_rowid", "b"})
}

func (s *tableRestoreSuite) TestInitializeColumns(c *C) {
	ccp := &ChunkCheckpoint{}
	c.Assert(s.tr.initializeColumns(nil, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{0, 1, 2, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"b", "c", "a"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 0, 1, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"b"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{-1, 0, -1, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"_tidb_rowid", "b", "a", "c"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 1, 3, 0})

	ccp.ColumnPermutation = nil
	err := s.tr.initializeColumns([]string{"_tidb_rowid", "b", "a", "c", "d"}, ccp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `unknown columns in header \[d\]`)

	ccp.ColumnPermutation = nil
	err = s.tr.initializeColumns([]string{"e", "b", "c", "d"}, ccp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `unknown columns in header \[e d\]`)
}

func (s *tableRestoreSuite) TestCompareChecksumSuccess(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM.*").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	err = s.tr.compareChecksum(ctx, verification.MakeKVChecksum(1234567, 12345, 1234567890))
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

}

func (s *tableRestoreSuite) TestCompareChecksumFailure(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM TABLE `db`\\.`table`").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	err = s.tr.compareChecksum(ctx, verification.MakeKVChecksum(9876543, 54321, 1357924680))
	c.Assert(err, ErrorMatches, "checksum mismatched.*")

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestAnalyzeTable(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectExec("ANALYZE TABLE `db`\\.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := context.Background()
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	c.Assert(err, IsNil)
	g := glue.NewExternalTiDBGlue(db, defaultSQLMode)
	err = s.tr.analyzeTable(ctx, g)
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestImportKVSuccess(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &RestoreController{saveCpCh: chptCh}
	go func() {
		for range chptCh {
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil)

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine, rc, 1)
	c.Assert(err, IsNil)
}

func (s *tableRestoreSuite) TestImportKVFailure(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &RestoreController{saveCpCh: chptCh}
	go func() {
		for range chptCh {
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID).
		Return(errors.Annotate(context.Canceled, "fake import error"))

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine, rc, 1)
	c.Assert(err, ErrorMatches, "fake import error.*")
}

var _ = Suite(&chunkRestoreSuite{})

type chunkRestoreSuite struct {
	tableRestoreSuiteBase
	cr *chunkRestore
}

func (s *chunkRestoreSuite) SetUpTest(c *C) {
	s.tableRestoreSuiteBase.SetUpTest(c)

	ctx := context.Background()
	w := worker.NewPool(ctx, 5, "io")

	chunk := ChunkCheckpoint{
		Key:      ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
		FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    37,
			PrevRowIDMax: 18,
			RowIDMax:     36,
		},
	}

	var err error
	s.cr, err = newChunkRestore(context.Background(), 1, s.cfg, &chunk, w, s.store, nil)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TearDownTest(c *C) {
	s.cr.close()
}

func (s *chunkRestoreSuite) TestDeliverLoopCancel(c *C) {
	rc := &RestoreController{backend: kv.NewMockImporter(nil, "")}

	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	go cancel()
	_, err := s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, nil, nil, rc)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *chunkRestoreSuite) TestDeliverLoopEmptyData(c *C) {
	ctx := context.Background()

	// Open two mock engines.

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil).Times(2)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(2048)).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	dataWriter, err := dataEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, s.tr.tableName, -1)
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)

	// Deliver nothing.

	cfg := &config.Config{}
	rc := &RestoreController{cfg: cfg, backend: importer}

	kvsCh := make(chan []deliveredKVs, 1)
	kvsCh <- []deliveredKVs{}
	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TestDeliverLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	mockCols := []string{"c1", "c2"}

	// Open two mock engines.

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil).Times(2)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(2048)).Return(mockWriter, nil).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, s.tr.tableName, -1)
	c.Assert(err, IsNil)

	dataWriter, err := dataEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)

	// Set up the expected API calls to the data engine...

	mockWriter.EXPECT().
		AppendRows(ctx, s.tr.tableName, mockCols, gomock.Any(), kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_ryyyyyyyy"),
				Val: []byte("value1"),
			},
			{
				Key: []byte("txxxxxxxx_rwwwwwwww"),
				Val: []byte("value2"),
			},
		})).
		Return(nil)

	// ... and the index engine.
	//
	// Note: This test assumes data engine is written before the index engine.

	mockWriter.EXPECT().
		AppendRows(ctx, s.tr.tableName, mockCols, gomock.Any(), kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_izzzzzzzz"),
				Val: []byte("index1"),
			},
		})).
		Return(nil)

	// Now actually start the delivery loop.

	saveCpCh := make(chan saveCp, 2)
	go func() {
		kvsCh <- []deliveredKVs{{
			kvs: kv.MakeRowFromKvPairs([]common.KvPair{
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
			}),
			columns: mockCols,
			offset:  12,
			rowID:   76,
		},
		}
		kvsCh <- []deliveredKVs{}
		close(kvsCh)
	}()

	cfg := &config.Config{}
	rc := &RestoreController{cfg: cfg, saveCpCh: saveCpCh, backend: importer}

	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
	c.Assert(s.cr.chunk.Chunk.Offset, Equals, int64(12))
	c.Assert(s.cr.chunk.Chunk.PrevRowIDMax, Equals, int64(76))
	c.Assert(s.cr.chunk.Checksum.SumKVS(), Equals, uint64(3))
}

func (s *chunkRestoreSuite) TestEncodeLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567895,
	})
	c.Assert(err, IsNil)
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, IsNil)
	c.Assert(kvsCh, HasLen, 2)

	kvs := <-kvsCh
	c.Assert(kvs, HasLen, 1)
	c.Assert(kvs[0].kvs, HasLen, 2)
	c.Assert(kvs[0].rowID, Equals, int64(19))
	c.Assert(kvs[0].offset, Equals, int64(36))

	kvs = <-kvsCh
	c.Assert(len(kvs), Equals, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567896,
	})
	c.Assert(err, IsNil)

	go cancel()
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopForcedError(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567897,
	})
	c.Assert(err, IsNil)

	// close the chunk so reading it will result in the "file already closed" error.
	s.cr.parser.Close()

	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, `in file .*[/\\]?db\.table\.2\.sql:0 at offset 0:.*file already closed`)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverErrored(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567898,
	})
	c.Assert(err, IsNil)

	go func() {
		deliverCompleteCh <- deliverResult{
			err: errors.New("fake deliver error"),
		}
	}()
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, "fake deliver error")
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopColumnsMismatch(c *C) {
	dir := c.MkDir()
	fileName := "db.table.000.csv"
	err := ioutil.WriteFile(filepath.Join(dir, fileName), []byte("1,2,3,4\r\n4,5,6,7\r\n"), 0644)
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	ctx := context.Background()
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}

	reader, err := store.Open(ctx, fileName)
	c.Assert(err, IsNil)
	w := worker.NewPool(ctx, 5, "io")
	p := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, false)

	err = s.cr.parser.Close()
	c.Assert(err, IsNil)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTiDBBackend(nil, config.ReplaceOnDup).NewEncoder(
		s.tr.encTable,
		&kv.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
		})
	c.Assert(err, IsNil)

	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, "in file db.table.2.sql:0 at offset 8: column count mismatch, expected 3, got 4")
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestRestore(c *C) {
	ctx := context.Background()

	// Open two mock engines

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	mockDataWriter := mock.NewMockImportKV_WriteEngineClient(controller)
	mockIndexWriter := mock.NewMockImportKV_WriteEngineClient(controller)
	importer := kv.NewMockImporter(mockClient, "127.0.0.1:2379")

	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)

	dataEngine, err := importer.OpenEngine(ctx, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, s.tr.tableName, -1)
	c.Assert(err, IsNil)
	dataWriter, err := dataEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, 2048)
	c.Assert(err, IsNil)

	// Expected API sequence
	// (we don't care about the actual content, this would be checked in the integrated tests)

	mockClient.EXPECT().WriteEngine(ctx).Return(mockDataWriter, nil)
	mockDataWriter.EXPECT().Send(gomock.Any()).Return(nil)
	mockDataWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		c.Assert(req.GetBatch().GetMutations(), HasLen, 1)
		return nil
	})
	mockDataWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	mockClient.EXPECT().WriteEngine(ctx).Return(mockIndexWriter, nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).Return(nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		c.Assert(req.GetBatch().GetMutations(), HasLen, 1)
		return nil
	})
	mockIndexWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	// Now actually start the restore loop.

	saveCpCh := make(chan saveCp, 2)
	err = s.cr.restore(ctx, s.tr, 0, dataWriter, indexWriter, &RestoreController{
		cfg:      s.cfg,
		saveCpCh: saveCpCh,
		backend:  importer,
		pauser:   DeliverPauser,
	})
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
}

var _ = Suite(&restoreSchemaSuite{})

type restoreSchemaSuite struct {
	ctx        context.Context
	rc         *RestoreController
	controller *gomock.Controller
}

func (s *restoreSchemaSuite) SetUpSuite(c *C) {
	ctx := context.Background()
	fakeDataDir := c.MkDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)
	// restore database schema file
	fakeDBName := "fakedb"
	// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}-schema-create.sql'
	fakeFileName := fmt.Sprintf("%s-schema-create.sql", fakeDBName)
	err = store.Write(ctx, fakeFileName, []byte(fmt.Sprintf("CREATE DATABASE %s;", fakeDBName)))
	c.Assert(err, IsNil)
	// restore table schema files
	fakeTableFilesCount := 8
	for i := 1; i <= fakeTableFilesCount; i++ {
		fakeTableName := fmt.Sprintf("tbl%d", i)
		// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}.{table}-schema.sql'
		fakeFileName := fmt.Sprintf("%s.%s-schema.sql", fakeDBName, fakeTableName)
		fakeFileContent := []byte(fmt.Sprintf("CREATE TABLE %s(i TINYINT);", fakeTableName))
		err = store.Write(ctx, fakeFileName, fakeFileContent)
		c.Assert(err, IsNil)
	}
	// restore view schema files
	fakeViewFilesCount := 8
	for i := 1; i <= fakeViewFilesCount; i++ {
		fakeViewName := fmt.Sprintf("tbl%d", i)
		// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}.{table}-schema-view.sql'
		fakeFileName := fmt.Sprintf("%s.%s-schema-view.sql", fakeDBName, fakeViewName)
		fakeFileContent := []byte(fmt.Sprintf("CREATE ALGORITHM=UNDEFINED VIEW `%s` (`i`) AS SELECT `i` FROM `%s`.`%s`;", fakeViewName, fakeDBName, fmt.Sprintf("tbl%d", i)))
		err = store.Write(ctx, fakeFileName, fakeFileContent)
		c.Assert(err, IsNil)
	}
	config := config.NewConfig()
	config.Mydumper.NoSchema = false
	config.Mydumper.DefaultFileRules = true
	config.Mydumper.CharacterSet = "utf8mb4"
	config.App.RegionConcurrency = 8
	mydumpLoader, err := mydump.NewMyDumpLoaderWithStore(ctx, config, store)
	c.Assert(err, IsNil)
	s.rc = &RestoreController{
		cfg:           config,
		store:         store,
		dbMetas:       mydumpLoader.GetDatabases(),
		checkpointsDB: &checkpoints.NullCheckpointsDB{},
	}
}

func (s *restoreSchemaSuite) SetUpTest(c *C) {
	s.controller, s.ctx = gomock.WithContext(context.Background(), c)
	mockBackend := mock.NewMockBackend(s.controller)
	// We don't care the execute results of those
	mockBackend.EXPECT().
		FetchRemoteTableModels(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(make([]*model.TableInfo, 0), nil)
	s.rc.backend = kv.MakeBackend(mockBackend)
	mockSQLExecutor := mock.NewMockSQLExecutor(s.controller)
	mockSQLExecutor.EXPECT().
		ExecuteWithLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil)
	mockSession := mock.NewMockSession(s.controller)
	mockSession.EXPECT().
		Close().
		AnyTimes().
		Return()
	mockSession.EXPECT().
		Execute(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil, nil)
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSQLExecutor().
		AnyTimes().
		Return(mockSQLExecutor)
	mockTiDBGlue.EXPECT().
		GetSession(gomock.Any()).
		AnyTimes().
		Return(mockSession, nil)
	mockTiDBGlue.EXPECT().
		OwnsSQLExecutor().
		AnyTimes().
		Return(true)
	parser := parser.New()
	mockTiDBGlue.EXPECT().
		GetParser().
		AnyTimes().
		Return(parser)
	s.rc.tidbGlue = mockTiDBGlue
}

func (s *restoreSchemaSuite) TearDownTest(c *C) {
	s.rc.Close()
	s.controller.Finish()
}

func (s *restoreSchemaSuite) TestRestoreSchemaSuccessful(c *C) {
	err := s.rc.restoreSchema(s.ctx)
	c.Assert(err, IsNil)
}

func (s *restoreSchemaSuite) TestRestoreSchemaFailed(c *C) {
	injectErr := errors.New("Somthing wrong")
	mockSession := mock.NewMockSession(s.controller)
	mockSession.EXPECT().
		Close().
		AnyTimes().
		Return()
	mockSession.EXPECT().
		Execute(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil, injectErr)
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSession(gomock.Any()).
		AnyTimes().
		Return(mockSession, nil)
	s.rc.tidbGlue = mockTiDBGlue
	err := s.rc.restoreSchema(s.ctx)
	c.Assert(err, NotNil)
	c.Assert(errors.ErrorEqual(err, injectErr), IsTrue)
}

func (s *restoreSchemaSuite) TestRestoreSchemaContextCancel(c *C) {
	childCtx, cancel := context.WithCancel(s.ctx)
	mockSession := mock.NewMockSession(s.controller)
	mockSession.EXPECT().
		Close().
		AnyTimes().
		Return()
	mockSession.EXPECT().
		Execute(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(context.Context, string) { cancel() }).
		Return(nil, nil)
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSession(gomock.Any()).
		AnyTimes().
		Return(mockSession, nil)
	s.rc.tidbGlue = mockTiDBGlue
	err := s.rc.restoreSchema(childCtx)
	cancel()
	c.Assert(err, NotNil)
	c.Assert(err, Equals, childCtx.Err())
}
