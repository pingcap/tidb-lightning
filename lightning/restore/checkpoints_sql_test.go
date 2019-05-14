package restore_test

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var _ = Suite(&cpSQLSuite{})

type cpSQLSuite struct {
	db   *sql.DB
	mock sqlmock.Sqlmock
	cpdb *restore.MySQLCheckpointsDB
}

func (s *cpSQLSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.db = db
	s.mock = mock

	// 1. create the checkpoints database.

	s.mock.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `mock-schema`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.
		ExpectExec("CREATE TABLE IF NOT EXISTS `mock-schema`\\.table_v\\d+ .+").
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mock.
		ExpectExec("CREATE TABLE IF NOT EXISTS `mock-schema`\\.engine_v\\d+ .+").
		WillReturnResult(sqlmock.NewResult(3, 1))
	s.mock.
		ExpectExec("CREATE TABLE IF NOT EXISTS `mock-schema`\\.chunk_v\\d+ .+").
		WillReturnResult(sqlmock.NewResult(4, 1))

	cpdb, err := restore.NewMySQLCheckpointsDB(context.Background(), s.db, "mock-schema")
	c.Assert(err, IsNil)
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)
	s.cpdb = cpdb
}

func (s *cpSQLSuite) TearDownTest(c *C) {
	s.mock.ExpectClose()
	c.Assert(s.cpdb.Close(), IsNil)
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)
}

func (s *cpSQLSuite) TestNormalOperations(c *C) {
	ctx := context.Background()
	cpdb := s.cpdb

	// 2. initialize with checkpoint data.

	s.mock.ExpectBegin()
	initializeStmt := s.mock.
		ExpectPrepare("INSERT INTO `mock-schema`\\.table_v\\d+")
	initializeStmt.ExpectExec().
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "`db1`.`t1`", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(5, 1))
	initializeStmt.ExpectExec().
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "`db1`.`t2`", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(6, 1))
	initializeStmt.ExpectExec().
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "`db2`.`t3`", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(7, 1))
	s.mock.ExpectCommit()

	s.mock.MatchExpectationsInOrder(false)
	err := cpdb.Initialize(ctx, map[string]*restore.TidbDBInfo{
		"db1": {
			Name: "db1",
			Tables: map[string]*restore.TidbTableInfo{
				"t1": {Name: "t1"},
				"t2": {Name: "t2"},
			},
		},
		"db2": {
			Name: "db2",
			Tables: map[string]*restore.TidbTableInfo{
				"t3": {Name: "t3"},
			},
		},
	})
	s.mock.MatchExpectationsInOrder(true)
	c.Assert(err, IsNil)
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)

	// 3. set some checkpoints

	s.mock.ExpectBegin()
	insertEngineStmt := s.mock.
		ExpectPrepare("REPLACE INTO `mock-schema`\\.engine_v\\d+ .+")
	insertEngineStmt.
		ExpectExec().
		WithArgs("`db1`.`t2`", 0, 30).
		WillReturnResult(sqlmock.NewResult(8, 1))
	insertEngineStmt.
		ExpectExec().
		WithArgs("`db1`.`t2`", -1, 30).
		WillReturnResult(sqlmock.NewResult(9, 1))
	insertChunkStmt := s.mock.
		ExpectPrepare("REPLACE INTO `mock-schema`\\.chunk_v\\d+ .+")
	insertChunkStmt.
		ExpectExec().
		WithArgs("`db1`.`t2`", 0, "/tmp/path/1.sql", 0, 12, 102400, 1, 5000).
		WillReturnResult(sqlmock.NewResult(10, 1))
	s.mock.ExpectCommit()

	s.mock.MatchExpectationsInOrder(false)
	err = cpdb.InsertEngineCheckpoints(ctx, "`db1`.`t2`", map[int32]*restore.EngineCheckpoint{
		0: {
			Status: restore.CheckpointStatusLoaded,
			Chunks: []*restore.ChunkCheckpoint{{
				Key: restore.ChunkCheckpointKey{
					Path:   "/tmp/path/1.sql",
					Offset: 0,
				},
				Chunk: mydump.Chunk{
					Offset:       12,
					EndOffset:    102400,
					PrevRowIDMax: 1,
					RowIDMax:     5000,
				},
			}},
		},
		-1: {
			Status: restore.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	s.mock.MatchExpectationsInOrder(true)
	c.Assert(err, IsNil)
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)

	// 4. update some checkpoints

	cpd := restore.NewTableCheckpointDiff()
	scm := restore.StatusCheckpointMerger{
		EngineID: 0,
		Status:   restore.CheckpointStatusImported,
	}
	scm.MergeInto(cpd)
	scm = restore.StatusCheckpointMerger{
		EngineID: restore.WholeTableEngineID,
		Status:   restore.CheckpointStatusAllWritten,
	}
	scm.MergeInto(cpd)
	rcm := restore.RebaseCheckpointMerger{
		AllocBase: 132861,
	}
	rcm.MergeInto(cpd)
	ccm := restore.ChunkCheckpointMerger{
		EngineID: 0,
		Key:      restore.ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0},
		Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
		Pos:      55904,
		RowID:    681,
	}
	ccm.MergeInto(cpd)

	s.mock.ExpectBegin()
	s.mock.
		ExpectPrepare("UPDATE `mock-schema`\\.chunk_v\\d+ SET pos = .+").
		ExpectExec().
		WithArgs(
			55904, 681, 4491, 586, 486070148917,
			"`db1`.`t2`", 0, "/tmp/path/1.sql", 0,
		).
		WillReturnResult(sqlmock.NewResult(11, 1))
	s.mock.
		ExpectPrepare("UPDATE `mock-schema`\\.table_v\\d+ SET alloc_base = .+").
		ExpectExec().
		WithArgs(132861, "`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(12, 1))
	s.mock.
		ExpectPrepare("UPDATE `mock-schema`\\.engine_v\\d+ SET status = .+").
		ExpectExec().
		WithArgs(120, "`db1`.`t2`", 0).
		WillReturnResult(sqlmock.NewResult(13, 1))
	s.mock.
		ExpectPrepare("UPDATE `mock-schema`\\.table_v\\d+ SET status = .+").
		ExpectExec().
		WithArgs(60, "`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(14, 1))
	s.mock.ExpectCommit()

	s.mock.MatchExpectationsInOrder(false)
	cpdb.Update(map[string]*restore.TableCheckpointDiff{"`db1`.`t2`": cpd})
	s.mock.MatchExpectationsInOrder(true)
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)

	// 5. get back the checkpoints

	s.mock.ExpectBegin()
	s.mock.
		ExpectQuery("SELECT .+ FROM `mock-schema`\\.engine_v\\d+").
		WithArgs("`db1`.`t2`").
		WillReturnRows(
			sqlmock.NewRows([]string{"engine_id", "status"}).
				AddRow(0, 120).
				AddRow(-1, 30),
		)
	s.mock.
		ExpectQuery("SELECT (?s:.+) FROM `mock-schema`\\.chunk_v\\d+").
		WithArgs("`db1`.`t2`").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"engine_id", "path", "offset", "columns",
				"pos", "end_offset", "prev_rowid_max", "rowid_max",
				"kvc_bytes", "kvc_kvs", "kvc_checksum",
			}).
				AddRow(
					0, "/tmp/path/1.sql", 0, "[]",
					55904, 102400, 681, 5000,
					4491, 586, 486070148917,
				),
		)
	s.mock.
		ExpectQuery("SELECT .+ FROM `mock-schema`\\.table_v\\d+").
		WithArgs("`db1`.`t2`").
		WillReturnRows(
			sqlmock.NewRows([]string{"status", "alloc_base"}).
				AddRow(60, 132861),
		)
	s.mock.ExpectCommit()

	cp, err := cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &restore.TableCheckpoint{
		Status:    restore.CheckpointStatusAllWritten,
		AllocBase: 132861,
		Engines: map[int32]*restore.EngineCheckpoint{
			-1: {Status: restore.CheckpointStatusLoaded},
			0: {
				Status: restore.CheckpointStatusImported,
				Chunks: []*restore.ChunkCheckpoint{{
					Key: restore.ChunkCheckpointKey{
						Path:   "/tmp/path/1.sql",
						Offset: 0,
					},
					ColumnPermutation: []int{},
					Chunk: mydump.Chunk{
						Offset:       55904,
						EndOffset:    102400,
						PrevRowIDMax: 681,
						RowIDMax:     5000,
					},
					Checksum: verification.MakeKVChecksum(4491, 586, 486070148917),
				}},
			},
		},
	})
	c.Assert(s.mock.ExpectationsWereMet(), IsNil)
}

func (s *cpSQLSuite) TestRemoveAllCheckpoints(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.chunk_v\\d+ WHERE table_name IN").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 5))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.engine_v\\d+ WHERE table_name IN").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 3))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.table_v\\d+ WHERE node_id = \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 2))
	s.mock.ExpectCommit()

	err := s.cpdb.RemoveCheckpoint(context.Background(), "all")
	c.Assert(err, IsNil)
}

func (s *cpSQLSuite) TestRemoveOneCheckpoint(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.chunk_v\\d+ WHERE table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 4))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.engine_v\\d+ WHERE table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 2))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.table_v\\d+ WHERE table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()

	err := s.cpdb.RemoveCheckpoint(context.Background(), "`db1`.`t2`")
	c.Assert(err, IsNil)
}

func (s *cpSQLSuite) TestIgnoreAllErrorCheckpoints(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectExec("UPDATE `mock-schema`\\.engine_v\\d+ SET status = 30 WHERE node_id = \\? AND status <= 25").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(5, 3))
	s.mock.
		ExpectExec("UPDATE `mock-schema`\\.table_v\\d+ SET status = 30 WHERE node_id = \\? AND status <= 25").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(6, 2))
	s.mock.ExpectCommit()

	err := s.cpdb.IgnoreErrorCheckpoint(context.Background(), "all")
	c.Assert(err, IsNil)
}

func (s *cpSQLSuite) TestIgnoreOneErrorCheckpoint(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectExec("UPDATE `mock-schema`\\.engine_v\\d+ SET status = 30 WHERE table_name = \\? AND status <= 25").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(5, 2))
	s.mock.
		ExpectExec("UPDATE `mock-schema`\\.table_v\\d+ SET status = 30 WHERE table_name = \\? AND status <= 25").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(6, 1))
	s.mock.ExpectCommit()

	err := s.cpdb.IgnoreErrorCheckpoint(context.Background(), "`db1`.`t2`")
	c.Assert(err, IsNil)
}

func (s *cpSQLSuite) TestDestroyAllErrorCheckpoints(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectQuery("SELECT (?s:.+)node_id = \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(
			sqlmock.NewRows([]string{"table_name", "__min__", "__max__"}).
				AddRow("`db1`.`t2`", -1, 0),
		)
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.chunk_v\\d+ WHERE table_name IN .+ node_id = \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 5))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.engine_v\\d+ WHERE table_name IN .+ node_id = \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 3))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.table_v\\d+ WHERE node_id = \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 2))
	s.mock.ExpectCommit()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(context.Background(), "all")
	c.Assert(err, IsNil)
	c.Assert(dtc, DeepEquals, []restore.DestroyedTableCheckpoint{{
		TableName:   "`db1`.`t2`",
		MinEngineID: -1,
		MaxEngineID: 0,
	}})
}

func (s *cpSQLSuite) TestDestroyOneErrorCheckpoints(c *C) {
	s.mock.ExpectBegin()
	s.mock.
		ExpectQuery("SELECT (?s:.+)table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnRows(
			sqlmock.NewRows([]string{"table_name", "__min__", "__max__"}).
				AddRow("`db1`.`t2`", -1, 0),
		)
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.chunk_v\\d+ WHERE .+table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 4))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.engine_v\\d+ WHERE .+table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 2))
	s.mock.
		ExpectExec("DELETE FROM `mock-schema`\\.table_v\\d+ WHERE table_name = \\?").
		WithArgs("`db1`.`t2`").
		WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(context.Background(), "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(dtc, DeepEquals, []restore.DestroyedTableCheckpoint{{
		TableName:   "`db1`.`t2`",
		MinEngineID: -1,
		MaxEngineID: 0,
	}})
}

func (s *cpSQLSuite) TestDump(c *C) {
	ctx := context.Background()
	t := time.Unix(1555555555, 0).UTC()

	s.mock.
		ExpectQuery("SELECT (?s:.+) FROM `mock-schema`\\.chunk_v\\d+").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"table_name", "path", "offset", "columns",
				"pos", "end_offset", "prev_rowid_max", "rowid_max",
				"kvc_bytes", "kvc_kvs", "kvc_checksum",
				"create_time", "update_time",
			}).AddRow(
				"`db1`.`t2`", "/tmp/path/1.sql", 0, "[]",
				55904, 102400, 681, 5000,
				4491, 586, 486070148917,
				t, t,
			),
		)

	var csvBuilder strings.Builder
	err := s.cpdb.DumpChunks(ctx, &csvBuilder)
	c.Assert(err, IsNil)
	c.Assert(csvBuilder.String(), Equals,
		"table_name,path,offset,columns,pos,end_offset,prev_rowid_max,rowid_max,kvc_bytes,kvc_kvs,kvc_checksum,create_time,update_time\n"+
			"`db1`.`t2`,/tmp/path/1.sql,0,[],55904,102400,681,5000,4491,586,486070148917,2019-04-18 02:45:55 +0000 UTC,2019-04-18 02:45:55 +0000 UTC\n",
	)

	s.mock.
		ExpectQuery("SELECT .+ FROM `mock-schema`\\.engine_v\\d+").
		WillReturnRows(
			sqlmock.NewRows([]string{"table_name", "engine_id", "status", "create_time", "update_time"}).
				AddRow("`db1`.`t2`", -1, 30, t, t).
				AddRow("`db1`.`t2`", 0, 120, t, t),
		)

	csvBuilder.Reset()
	err = s.cpdb.DumpEngines(ctx, &csvBuilder)
	c.Assert(err, IsNil)
	c.Assert(csvBuilder.String(), Equals,
		"table_name,engine_id,status,create_time,update_time\n"+
			"`db1`.`t2`,-1,30,2019-04-18 02:45:55 +0000 UTC,2019-04-18 02:45:55 +0000 UTC\n"+
			"`db1`.`t2`,0,120,2019-04-18 02:45:55 +0000 UTC,2019-04-18 02:45:55 +0000 UTC\n",
	)

	s.mock.
		ExpectQuery("SELECT .+ FROM `mock-schema`\\.table_v\\d+").
		WillReturnRows(
			sqlmock.NewRows([]string{"node_id", "session", "table_name", "hash", "status", "alloc_base", "create_time", "update_time"}).
				AddRow(0, 1555555555, "`db1`.`t2`", 0, 90, 132861, t, t),
		)

	csvBuilder.Reset()
	err = s.cpdb.DumpTables(ctx, &csvBuilder)
	c.Assert(err, IsNil)
	c.Assert(csvBuilder.String(), Equals,
		"node_id,session,table_name,hash,status,alloc_base,create_time,update_time\n"+
			"0,1555555555,`db1`.`t2`,0,90,132861,2019-04-18 02:45:55 +0000 UTC,2019-04-18 02:45:55 +0000 UTC\n",
	)
}
