package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

var _ = Suite(&checkpointSuite{})

type checkpointSuite struct {
}

func (s *checkpointSuite) TestMergeStatusCheckpoint(c *C) {
	cpd := NewTableCheckpointDiff()

	m := StatusCheckpointMerger{EngineID: 0, Status: CheckpointStatusImported}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasStatus: false,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	})

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusLoaded}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasStatus: false,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	})

	m = StatusCheckpointMerger{EngineID: WholeTableEngineID, Status: CheckpointStatusClosed}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusClosed,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	})

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusAllWritten}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusClosed,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusImported,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusAllWritten,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	})
}

func (s *checkpointSuite) TestMergeInvalidStatusCheckpoint(c *C) {
	cpd := NewTableCheckpointDiff()

	m := StatusCheckpointMerger{EngineID: 0, Status: CheckpointStatusLoaded}
	m.MergeInto(cpd)

	m = StatusCheckpointMerger{EngineID: -1, Status: CheckpointStatusAllWritten}
	m.SetInvalid()
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasStatus: true,
		status:    CheckpointStatusAllWritten / 10,
		engines: map[int32]engineCheckpointDiff{
			0: {
				hasStatus: true,
				status:    CheckpointStatusLoaded,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
			-1: {
				hasStatus: true,
				status:    CheckpointStatusAllWritten / 10,
				chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
			},
		},
	})
}

func (s *checkpointSuite) TestMergeChunkCheckpoint(c *C) {
	cpd := NewTableCheckpointDiff()

	key := ChunkCheckpointKey{Path: "/tmp/path/1.sql", Offset: 0}

	m := ChunkCheckpointMerger{
		EngineID: 2,
		Key:      key,
		Checksum: verification.MakeKVChecksum(700, 15, 1234567890),
		Pos:      1055,
		RowID:    31,
	}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1055,
						rowID:    31,
						checksum: verification.MakeKVChecksum(700, 15, 1234567890),
					},
				},
			},
		},
	})

	m = ChunkCheckpointMerger{
		EngineID: 2,
		Key:      key,
		Checksum: verification.MakeKVChecksum(800, 20, 1357924680),
		Pos:      1080,
		RowID:    42,
	}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		engines: map[int32]engineCheckpointDiff{
			2: {
				chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
					key: {
						pos:      1080,
						rowID:    42,
						checksum: verification.MakeKVChecksum(800, 20, 1357924680),
					},
				},
			},
		},
	})
}

func (s *checkpointSuite) TestRebaseCheckpoint(c *C) {
	cpd := NewTableCheckpointDiff()

	m := RebaseCheckpointMerger{AllocBase: 10000}
	m.MergeInto(cpd)

	c.Assert(cpd, DeepEquals, &TableCheckpointDiff{
		hasRebase: true,
		allocBase: 10000,
		engines:   make(map[int32]engineCheckpointDiff),
	})
}
