package restore_test

import (
	"context"
	"path"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var _ = Suite(&cpFileSuite{})

type cpFileSuite struct {
	path string
	cpdb *restore.FileCheckpointsDB
}

func (s *cpFileSuite) SetUpTest(c *C) {
	dir := c.MkDir()
	s.cpdb = restore.NewFileCheckpointsDB(path.Join(dir, "cp.pb"))

	ctx := context.Background()
	cpdb := s.cpdb

	// 2. initialize with checkpoint data.

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
	c.Assert(err, IsNil)

	// 3. set some checkpoints

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
	c.Assert(err, IsNil)

	err = cpdb.InsertEngineCheckpoints(ctx, "`db2`.`t3`", map[int32]*restore.EngineCheckpoint{
		-1: {
			Status: restore.CheckpointStatusLoaded,
			Chunks: nil,
		},
	})
	c.Assert(err, IsNil)

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

	cpdb.Update(map[string]*restore.TableCheckpointDiff{"`db1`.`t2`": cpd})
}

func (s *cpFileSuite) TearDownTest(c *C) {
	c.Assert(s.cpdb.Close(), IsNil)
}

func (s *cpFileSuite) setInvalidStatus() {
	cpd := restore.NewTableCheckpointDiff()
	scm := restore.StatusCheckpointMerger{
		EngineID: -1,
		Status:   restore.CheckpointStatusAllWritten,
	}
	scm.SetInvalid()
	scm.MergeInto(cpd)

	s.cpdb.Update(map[string]*restore.TableCheckpointDiff{
		"`db1`.`t2`": cpd,
		"`db2`.`t3`": cpd,
	})
}

func (s *cpFileSuite) TestGet(c *C) {
	ctx := context.Background()

	// 5. get back the checkpoints

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &restore.TableCheckpoint{
		Status:    restore.CheckpointStatusAllWritten,
		AllocBase: 132861,
		Engines: map[int32]*restore.EngineCheckpoint{
			-1: {
				Status: restore.CheckpointStatusLoaded,
				Chunks: []*restore.ChunkCheckpoint{},
			},
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

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &restore.TableCheckpoint{
		Status: restore.CheckpointStatusLoaded,
		Engines: map[int32]*restore.EngineCheckpoint{
			-1: {
				Status: restore.CheckpointStatusLoaded,
				Chunks: []*restore.ChunkCheckpoint{},
			},
		},
	})

	cp, err = s.cpdb.Get(ctx, "`db3`.`not-exists`")
	c.Assert(err, IsNil)
	c.Assert(cp, DeepEquals, &restore.TableCheckpoint{
		Engines: make(map[int32]*restore.EngineCheckpoint),
	})
}

func (s *cpFileSuite) TestRemoveAllCheckpoints(c *C) {
	ctx := context.Background()

	err := s.cpdb.RemoveCheckpoint(ctx, "all")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)
}

func (s *cpFileSuite) TestRemoveOneCheckpoint(c *C) {
	ctx := context.Background()

	err := s.cpdb.RemoveCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusLoaded)
}

func (s *cpFileSuite) TestIgnoreAllErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	err := s.cpdb.IgnoreErrorCheckpoint(ctx, "all")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusLoaded)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusLoaded)
}

func (s *cpFileSuite) TestIgnoreOneErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	err := s.cpdb.IgnoreErrorCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusLoaded)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusAllWritten/10)
}

func (s *cpFileSuite) TestDestroyAllErrorCheckpoints(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(ctx, "all")
	c.Assert(err, IsNil)
	sort.Slice(dtc, func(i, j int) bool { return dtc[i].TableName < dtc[j].TableName })
	c.Assert(dtc, DeepEquals, []restore.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
		{
			TableName:   "`db2`.`t3`",
			MinEngineID: -1,
			MaxEngineID: -1,
		},
	})

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)
}

func (s *cpFileSuite) TestDestroyOneErrorCheckpoint(c *C) {
	ctx := context.Background()

	s.setInvalidStatus()

	dtc, err := s.cpdb.DestroyErrorCheckpoint(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(dtc, DeepEquals, []restore.DestroyedTableCheckpoint{
		{
			TableName:   "`db1`.`t2`",
			MinEngineID: -1,
			MaxEngineID: 0,
		},
	})

	cp, err := s.cpdb.Get(ctx, "`db1`.`t2`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusMissing)

	cp, err = s.cpdb.Get(ctx, "`db2`.`t3`")
	c.Assert(err, IsNil)
	c.Assert(cp.Status, Equals, restore.CheckpointStatusAllWritten/10)
}
