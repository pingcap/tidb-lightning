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
	// "encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"sort"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/kv"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/verification"
	"github.com/pingcap/tidb-lightning/lightning/worker"
	"github.com/pingcap/tidb-lightning/mock"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/util/kvencoder"
	tmock "github.com/pingcap/tidb/util/mock"
	"github.com/satori/go.uuid"
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
			core: tableInfo,
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
		core: &model.TableInfo{},
	}
	dbInfo := &TidbDBInfo{Name: "mockdb", Tables: map[string]*TidbTableInfo{
		"failure": tableInfo,
	}}
	tableName := common.UniqueTable("mockdb", "failure")

	_, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &TableCheckpoint{})
	c.Assert(err, ErrorMatches, `failed to tables\.TableFromMeta.*`)
}

func (s *restoreSuite) TestErrorSummaries(c *C) {
	logger, buffer := makeTestLogger()

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

func (s *restoreSuite) TestDoChecksum(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

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

	ctx := context.Background()
	checksum, err := DoChecksum(ctx, db, "`test`.`t`")
	c.Assert(err, IsNil)
	c.Assert(*checksum, DeepEquals, RemoteChecksum{
		Schema:     "test",
		Table:      "t",
		Checksum:   8520875019404689597,
		TotalKVs:   7296873,
		TotalBytes: 357601387,
	})
}

func (s *restoreSuite) TestDoChecksumWithErrorAndLongOriginalLifetime(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("300h"))
	mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
		WillReturnError(errors.Annotate(context.Canceled, "mock syntax error"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("300h").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	_, err = DoChecksum(ctx, db, "`test`.`t`")
	c.Assert(err, ErrorMatches, "compute remote checksum failed: mock syntax error.*")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	mock.ExpectClose()
}

func (s *restoreSuite) TestSetSessionConcurrencyVars(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mock.ExpectExec(
		`SET\s+`+
			`SESSION tidb_build_stats_concurrency = \?,\s+`+
			`SESSION tidb_distsql_scan_concurrency = \?,\s+`+
			`SESSION tidb_index_serial_scan_concurrency = \?,\s+`+
			`SESSION tidb_checksum_table_concurrency = \?`).
		WithArgs(123, 456, 789, 543).
		WillReturnResult(sqlmock.NewResult(1, 4))

	ctx := context.Background()
	setSessionConcurrencyVars(ctx, db, config.DBStore{
		BuildStatsConcurrency:      123,
		DistSQLScanConcurrency:     456,
		IndexSerialScanConcurrency: 789,
		ChecksumTableConcurrency:   543,
	})

	c.Assert(mock.ExpectationsWereMet(), IsNil)
	mock.ExpectClose()
}

func (s *restoreSuite) TestWriteToEngineWithNothing(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	importer := kv.NewMockImporter(mockClient, "127.0.0.1:2379")

	ctx := context.Background()

	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)

	engine, err := importer.OpenEngine(ctx, "`db`.`table`", -1)
	c.Assert(err, IsNil)

	err = writeToEngine(ctx, engine, []kvenc.KvPair{})
	c.Assert(err, IsNil)
}

func (s *restoreSuite) TestWriteToEngine(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	importer := kv.NewMockImporter(mockClient, "127.0.0.1:2379")
	mockWriteStream := mock.NewMockImportKV_WriteEngineClient(controller)

	ctx := context.Background()

	// The expected API call to the importer

	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).Return(mockWriteStream, nil)
	mockWriteStream.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
			c.Assert(req.GetHead(), NotNil)
			return nil
		})
	mockWriteStream.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
			batch := req.GetBatch()
			c.Assert(batch, NotNil)
			c.Assert(batch.Mutations, DeepEquals, []*import_kvpb.Mutation{
				{
					Op:    import_kvpb.Mutation_Put,
					Key:   []byte("k1"),
					Value: []byte("v1"),
				},
				{
					Op:    import_kvpb.Mutation_Put,
					Key:   []byte("k2"),
					Value: []byte("v2"),
				},
			})
			return nil
		})
	mockWriteStream.EXPECT().CloseAndRecv().Return(nil, nil)

	// Actually run the test function

	engine, err := importer.OpenEngine(ctx, "`db`.`table`", -1)
	c.Assert(err, IsNil)

	err = writeToEngine(ctx, engine, []kvenc.KvPair{
		{Key: []byte("k1"), Val: []byte("v1")},
		{Key: []byte("k2"), Val: []byte("v2")},
	})
	c.Assert(err, IsNil)
}

var _ = Suite(&tableRestoreSuite{})

type tableRestoreSuite struct {
	tr  *TableRestore
	cfg *config.Config

	tableInfo *TidbTableInfo
	dbInfo    *TidbDBInfo
	tableMeta *mydump.MDTableMeta
}

func (s *tableRestoreSuite) SetUpSuite(c *C) {
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

	s.tableInfo = &TidbTableInfo{Name: "table", core: core}
	s.dbInfo = &TidbDBInfo{
		Name:   "db",
		Tables: map[string]*TidbTableInfo{"table": s.tableInfo},
	}

	// Write some sample SQL dump

	fakeDataDir := c.MkDir()
	fakeDataFilesCount := 6
	fakeDataFilesContent := []byte("INSERT INTO `table` VALUES (1, 2, 3);")
	c.Assert(len(fakeDataFilesContent), Equals, 37)
	fakeDataFiles := make([]string, 0, fakeDataFilesCount)
	for i := 1; i <= fakeDataFilesCount; i++ {
		fakeDataPath := path.Join(fakeDataDir, fmt.Sprintf("db.table.%d.sql", i))
		err = ioutil.WriteFile(fakeDataPath, fakeDataFilesContent, 0644)
		c.Assert(err, IsNil)
		fakeDataFiles = append(fakeDataFiles, fakeDataPath)
	}

	s.tableMeta = &mydump.MDTableMeta{
		DB:         "db",
		Name:       "table",
		TotalSize:  222,
		SchemaFile: path.Join(fakeDataDir, "db.table-schema.sql"),
		DataFiles:  fakeDataFiles,
	}
}

func (s *tableRestoreSuite) SetUpTest(c *C) {
	// Collect into the test TableRestore structure
	var err error
	s.tr, err = NewTableRestore("`db`.`table`", s.tableMeta, s.dbInfo, s.tableInfo, &TableCheckpoint{})
	c.Assert(err, IsNil)

	s.cfg = config.NewConfig()
	s.cfg.Mydumper.BatchSize = 111
	s.cfg.App.TableConcurrency = 2
}

func (s *tableRestoreSuite) TestPopulateChunks(c *C) {
	cp := &TableCheckpoint{
		Engines: make(map[int32]*EngineCheckpoint),
	}
	err := s.tr.populateChunks(s.cfg, cp)
	c.Assert(err, IsNil)

	c.Assert(cp.Engines, DeepEquals, map[int32]*EngineCheckpoint{
		-1: {
			Status: CheckpointStatusLoaded,
		},
		0: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[0], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 0,
						RowIDMax:     18,
					},
				},
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 18,
						RowIDMax:     36,
					},
				},
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[2], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 36,
						RowIDMax:     54,
					},
				},
			},
		},
		1: {
			Status: CheckpointStatusLoaded,
			Chunks: []*ChunkCheckpoint{
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[3], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 54,
						RowIDMax:     72,
					},
				},
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[4], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 72,
						RowIDMax:     90,
					},
				},
				{
					Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[5], Offset: 0},
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 90,
						RowIDMax:     108,
					},
				},
			},
		},
	})
}

func (s *tableRestoreSuite) TestInitializeColumns(c *C) {
	ccp := &ChunkCheckpoint{}
	s.tr.initializeColumns(nil, ccp)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{0, 1, 2, -1})

	ccp.ColumnPermutation = nil
	s.tr.initializeColumns([]string{"b", "c", "a"}, ccp)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 0, 1, -1})

	ccp.ColumnPermutation = nil
	s.tr.initializeColumns([]string{"b"}, ccp)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{-1, 0, -1, -1})

	ccp.ColumnPermutation = nil
	s.tr.initializeColumns([]string{"_tidb_rowid", "b", "a", "c"}, ccp)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 1, 3, 0})
}

func (s *tableRestoreSuite) TestCompareChecksumSuccess(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

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

	ctx := context.Background()
	err = s.tr.compareChecksum(ctx, db, verification.MakeKVChecksum(1234567, 12345, 1234567890))
	c.Assert(err, IsNil)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
	mock.ExpectClose()
}

func (s *tableRestoreSuite) TestCompareChecksumFailure(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

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

	ctx := context.Background()
	err = s.tr.compareChecksum(ctx, db, verification.MakeKVChecksum(9876543, 54321, 1357924680))
	c.Assert(err, ErrorMatches, "checksum mismatched.*")

	c.Assert(mock.ExpectationsWereMet(), IsNil)
	mock.ExpectClose()
}

func (s *tableRestoreSuite) TestAnalyzeTable(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mock.ExpectExec("ANALYZE TABLE `db`\\.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	err = s.tr.analyzeTable(ctx, db)
	c.Assert(err, IsNil)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
	mock.ExpectClose()
}

func (s *tableRestoreSuite) TestImportKVSuccess(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	importer := kv.NewMockImporter(mockClient, "127.0.0.1:2379")

	ctx := context.Background()
	engineUUID := uuid.NewV4()

	mockClient.EXPECT().
		CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: engineUUID.Bytes()}).
		Return(nil, nil)
	mockClient.EXPECT().
		ImportEngine(ctx, &import_kvpb.ImportEngineRequest{Uuid: engineUUID.Bytes(), PdAddr: "127.0.0.1:2379"}).
		Return(nil, nil)
	mockClient.EXPECT().
		CleanupEngine(ctx, &import_kvpb.CleanupEngineRequest{Uuid: engineUUID.Bytes()}).
		Return(nil, nil)

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine)
	c.Assert(err, IsNil)
}

func (s *tableRestoreSuite) TestImportKVFailure(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	importer := kv.NewMockImporter(mockClient, "127.0.0.1:2379")

	ctx := context.Background()
	engineUUID := uuid.NewV4()

	mockClient.EXPECT().
		CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: engineUUID.Bytes()}).
		Return(nil, nil)
	mockClient.EXPECT().
		ImportEngine(ctx, &import_kvpb.ImportEngineRequest{Uuid: engineUUID.Bytes(), PdAddr: "127.0.0.1:2379"}).
		Return(nil, errors.Annotate(context.Canceled, "fake import error"))

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine)
	c.Assert(err, ErrorMatches, "fake import error.*")
}

var _ = Suite(&chunkRestoreSuite{})

type chunkRestoreSuite struct {
	tableRestoreSuite
	cr *chunkRestore
}

func (s *chunkRestoreSuite) SetUpTest(c *C) {
	s.tableRestoreSuite.SetUpTest(c)

	ctx := context.Background()
	w := worker.NewPool(ctx, 5, "io")

	chunk := ChunkCheckpoint{
		Key: ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1], Offset: 0},
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    37,
			PrevRowIDMax: 18,
			RowIDMax:     36,
		},
	}

	var err error
	s.cr, err = newChunkRestore(1, s.cfg, &chunk, w)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TearDownTest(c *C) {
	s.cr.close()
}

func (s *chunkRestoreSuite) TestDeliverLoopCancel(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan deliveredKVs)
	go cancel()
	_, err := s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, nil, nil, nil)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *chunkRestoreSuite) TestDeliverLoopEmptyData(c *C) {
	ctx := context.Background()
	kvsCh := make(chan deliveredKVs, 1)
	kvsCh <- deliveredKVs{}
	_, err := s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, nil, nil, nil)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TestDeliverLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan deliveredKVs)

	// Open two mock engines.

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

	// Set up the expected API calls to the data engine...

	mockClient.EXPECT().WriteEngine(ctx).Return(mockDataWriter, nil)
	mockDataWriter.EXPECT().Send(gomock.Any()).Return(nil) // skip check write head
	mockDataWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		batch := req.GetBatch()
		c.Assert(batch, NotNil)
		c.Assert(batch.Mutations, DeepEquals, []*import_kvpb.Mutation{
			{
				Op:    import_kvpb.Mutation_Put,
				Key:   []byte("txxxxxxxx_ryyyyyyyy"),
				Value: []byte("value1"),
			},
			{
				Op:    import_kvpb.Mutation_Put,
				Key:   []byte("txxxxxxxx_rwwwwwwww"),
				Value: []byte("value2"),
			},
		})
		return nil
	})
	mockDataWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	// ... and the index engine.
	//
	// Note: This test assumes data engine is written before the index engine.

	mockClient.EXPECT().WriteEngine(ctx).Return(mockIndexWriter, nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).Return(nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		batch := req.GetBatch()
		c.Assert(batch, NotNil)
		c.Assert(batch.Mutations, DeepEquals, []*import_kvpb.Mutation{
			{
				Op:    import_kvpb.Mutation_Put,
				Key:   []byte("txxxxxxxx_izzzzzzzz"),
				Value: []byte("index1"),
			},
		})
		return nil
	})
	mockIndexWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	// Now actually start the delivery loop.

	saveCpCh := make(chan saveCp, 2)
	go func() {
		kvsCh <- deliveredKVs{
			kvs: []kvenc.KvPair{
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
			},
			offset: 12,
			rowID:  76,
		}
		kvsCh <- deliveredKVs{}
		close(kvsCh)
	}()

	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataEngine, indexEngine, &RestoreController{saveCpCh: saveCpCh})
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
	c.Assert(s.cr.chunk.Chunk.Offset, Equals, int64(12))
	c.Assert(s.cr.chunk.Chunk.PrevRowIDMax, Equals, int64(76))
	c.Assert(s.cr.chunk.Checksum.SumKVS(), Equals, uint64(3))
}

func (s *chunkRestoreSuite) TestEncodeLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, s.cfg.TiDB.SQLMode)

	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh)
	c.Assert(err, IsNil)
	c.Assert(kvsCh, HasLen, 2)

	firstKVs := <-kvsCh
	c.Assert(firstKVs.kvs, HasLen, 2)
	c.Assert(firstKVs.rowID, Equals, int64(19))
	c.Assert(firstKVs.offset, Equals, int64(36))

	secondKVs := <-kvsCh
	c.Assert(secondKVs.kvs, HasLen, 0)
	c.Assert(secondKVs.rowID, Equals, int64(19))
	c.Assert(secondKVs.offset, Equals, int64(36))
}

func (s *chunkRestoreSuite) TestEncodeLoopCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, s.cfg.TiDB.SQLMode)

	go cancel()
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverErrored(c *C) {
	ctx := context.Background()
	kvsCh := make(chan deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder := kv.NewTableKVEncoder(s.tr.encTable, s.cfg.TiDB.SQLMode)

	go func() {
		deliverCompleteCh <- deliverResult{
			err: errors.New("fake deliver error"),
		}
	}()
	_, _, err := s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh)
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
	})
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
}
