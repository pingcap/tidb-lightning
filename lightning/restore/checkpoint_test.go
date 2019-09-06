package restore

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&checkpointSuite{})

type checkpointSuite struct{}

func (s *splitKVSuite) TestCheckpointMarshallUnmarshall(c *C) {
	path := "/tmp/test-chkp"
	fileChkp := NewFileCheckpointsDB(path)
	fileChkp.checkpoints.Checkpoints["a"] = &TableCheckpointModel{
		Status:  uint32(CheckpointStatusLoaded),
		Engines: map[int32]*EngineCheckpointModel{},
	}
	fileChkp.Close()

	fileChkp2 := NewFileCheckpointsDB(path)
	// if not recover empty map explicitly, it will become nil
	c.Assert(fileChkp2.checkpoints.Checkpoints["a"].Engines, NotNil)
}
