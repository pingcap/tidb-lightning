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
	"database/sql"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pd "github.com/tikv/pd/client"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/ddl"
	tmock "github.com/pingcap/tidb/util/mock"
	uuid "github.com/satori/go.uuid"

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

func MockDoChecksumCtx(db *sql.DB) context.Context {
	ctx := context.Background()
	manager := newTiDBChecksumExecutor(db)
	return context.WithValue(ctx, &checksumManagerKey, manager)
}

func (s *restoreSuite) TestDoChecksum(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("test", "t", 8520875019404689597, 7296873, 357601387),
		)
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	checksum, err := DoChecksum(ctx, db, &TidbTableInfo{DB: "test", Name: "t"})
	c.Assert(err, IsNil)
	c.Assert(*checksum, DeepEquals, RemoteChecksum{
		Schema:     "test",
		Table:      "t",
		Checksum:   8520875019404689597,
		TotalKVs:   7296873,
		TotalBytes: 357601387,
	})

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *restoreSuite) TestDoChecksumParallel(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
			WillDelayFor(100 * time.Millisecond).
			WillReturnRows(
				sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
					AddRow("test", "t", 8520875019404689597, 7296873, 357601387),
			)
	}
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)

	// db.Close() will close all connections from its idle pool, set it 1 to expect one close
	db.SetMaxIdleConns(1)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			checksum, err := DoChecksum(ctx, db, &TidbTableInfo{DB: "test", Name: "t"})
			c.Assert(err, IsNil)
			c.Assert(*checksum, DeepEquals, RemoteChecksum{
				Schema:     "test",
				Table:      "t",
				Checksum:   8520875019404689597,
				TotalKVs:   7296873,
				TotalBytes: 357601387,
			})
		}()
	}
	wg.Wait()

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *restoreSuite) TestIncreaseGCLifeTimeFail(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	for i := 0; i < 5; i++ {
		mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
			WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
		mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
			WithArgs("100h0m0s").
			WillReturnError(errors.Annotate(context.Canceled, "update gc error"))
	}
	// This recover GC Life Time SQL should not be executed in DoChecksum
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			_, err = DoChecksum(ctx, db, &TidbTableInfo{DB: "test", Name: "t"})
			c.Assert(err, ErrorMatches, "update GC lifetime failed: update gc error: context canceled")
			wg.Done()
		}()
	}
	wg.Wait()

	_, err = db.Exec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E", "10m")
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *restoreSuite) TestDoChecksumWithErrorAndLongOriginalLifetime(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("300h"))
	mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
		WillReturnError(errors.Annotate(context.Canceled, "mock syntax error"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("300h").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	_, err = DoChecksum(ctx, db, &TidbTableInfo{DB: "test", Name: "t"})
	c.Assert(err, ErrorMatches, "compute remote checksum failed: mock syntax error.*")

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
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
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{TableName: filter.Table{"db", "table"}, FileMeta: mydump.SourceFileMeta{Path: fakeFileName, Type: mydump.SourceTypeSQL, SortKey: fmt.Sprintf("%d", i)}, Size: 37})
	}

	fakeCsvContent := []byte("1,2,3\r\n4,5,6\r\n")
	csvName := "db.table.99.csv"
	err = ioutil.WriteFile(filepath.Join(fakeDataDir, csvName), fakeCsvContent, 0644)
	c.Assert(err, IsNil)
	fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{TableName: filter.Table{"db", "table"}, FileMeta: mydump.SourceFileMeta{Path: csvName, Type: mydump.SourceTypeCSV, SortKey: "99"}, Size: 14})

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
	err = s.tr.compareChecksum(ctx, db, verification.MakeKVChecksum(1234567, 12345, 1234567890))
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
	err = s.tr.compareChecksum(ctx, db, verification.MakeKVChecksum(9876543, 54321, 1357924680))
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
	err = s.tr.analyzeTable(ctx, db)
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestImportKVSuccess(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)

	ctx := context.Background()
	engineUUID := uuid.NewV4()

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
	err = s.tr.importKV(ctx, closedEngine)
	c.Assert(err, IsNil)
}

func (s *tableRestoreSuite) TestImportKVFailure(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := kv.MakeBackend(mockBackend)

	ctx := context.Background()
	engineUUID := uuid.NewV4()

	mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID).
		Return(errors.Annotate(context.Canceled, "fake import error"))

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine)
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
	mockBackend.EXPECT().MaxChunkSize().Return(10000).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, s.tr.tableName, -1)
	c.Assert(err, IsNil)

	// Deliver nothing.

	cfg := &config.Config{}
	rc := &RestoreController{cfg: cfg, backend: importer}

	kvsCh := make(chan []deliveredKVs, 1)
	kvsCh <- []deliveredKVs{}
	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataEngine, indexEngine, rc)
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
	mockBackend.EXPECT().MaxChunkSize().Return(10000).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, s.tr.tableName, -1)
	c.Assert(err, IsNil)

	// Set up the expected API calls to the data engine...

	mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), s.tr.tableName, mockCols, gomock.Any(), kv.MakeRowsFromKvPairs([]common.KvPair{
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

	mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), s.tr.tableName, mockCols, gomock.Any(), kv.MakeRowsFromKvPairs([]common.KvPair{
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

	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataEngine, indexEngine, rc)
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
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:          s.cfg.TiDB.SQLMode,
		Timestamp:        1234567895,
		RowFormatVersion: "1",
	})
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
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
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:          s.cfg.TiDB.SQLMode,
		Timestamp:        1234567896,
		RowFormatVersion: "1",
	})

	go cancel()
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopForcedError(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:          s.cfg.TiDB.SQLMode,
		Timestamp:        1234567897,
		RowFormatVersion: "1",
	})

	// close the chunk so reading it will result in the "file already closed" error.
	s.cr.parser.Close()

	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, `in file .*[/\\]?db\.table\.2\.sql:0 at offset 0:.*file already closed`)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverErrored(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:          s.cfg.TiDB.SQLMode,
		Timestamp:        1234567898,
		RowFormatVersion: "1",
	})

	go func() {
		deliverCompleteCh <- deliverResult{
			err: errors.New("fake deliver error"),
		}
	}()
	cfg := config.NewConfig()
	rc := &RestoreController{pauser: DeliverPauser, cfg: cfg}
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, "fake deliver error")
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
	err = s.cr.restore(ctx, s.tr, 0, dataEngine, indexEngine, &RestoreController{
		cfg:      s.cfg,
		saveCpCh: saveCpCh,
		backend:  importer,
		pauser:   DeliverPauser,
	})
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
}

type testPDClient struct {
	pd.Client
}

func (c *testPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return 0, nil
}

type gcTTLManagerSuite struct{}

var _ = Suite(&gcTTLManagerSuite{})

func (s *gcTTLManagerSuite) TestGcTTLManager(c *C) {
	manager := gcTTLManager{pdClient: &testPDClient{}}
	ctx := context.Background()

	for i := uint64(1); i <= 5; i++ {
		err := manager.addOneJob(ctx, fmt.Sprintf("test%d", i), i)
		c.Assert(err, IsNil)
		c.Assert(manager.currentTs, Equals, uint64(1))
	}

	manager.removeOneJob("test2")
	c.Assert(manager.currentTs, Equals, uint64(1))

	manager.removeOneJob("test1")
	c.Assert(manager.currentTs, Equals, uint64(3))

	manager.removeOneJob("test3")
	c.Assert(manager.currentTs, Equals, uint64(4))

	manager.removeOneJob("test4")
	c.Assert(manager.currentTs, Equals, uint64(5))

	manager.removeOneJob("test5")
	c.Assert(manager.currentTs, Equals, uint64(0))
}
