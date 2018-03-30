package verification_test

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb-lightning/ingest/verification"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

type testKVChcksumSuite struct{}

func (s *testKVChcksumSuite) SetUpSuite(c *C)    {}
func (s *testKVChcksumSuite) TearDownSuite(c *C) {}

var _ = Suite(&testKVChcksumSuite{})

func TestKVChcksum(t *testing.T) {
	TestingT(t)
}

func uint64NotEqual(a uint64, b uint64) bool { return a != b }

func (s *testKVChcksumSuite) TestChcksum(c *C) {
	checksum := verification.NewKVChecksum(0)
	c.Assert(checksum.Sum(), Equals, uint64(0))

	// checksum on nothing
	checksum.Update([]kvec.KvPair{})
	c.Assert(checksum.Sum(), Equals, uint64(0))

	checksum.Update(nil)
	c.Assert(checksum.Sum(), Equals, uint64(0))

	// checksum on real data
	excpectChecksum := uint64(4850203904608948940)

	kvs := []kvec.KvPair{
		kvec.KvPair{
			Key: []byte("Cop"),
			Val: []byte("PingCAP"),
		},
		kvec.KvPair{
			Key: []byte("Introduction"),
			Val: []byte("Inspired by Google Spanner/F1, PingCAP develops TiDB."),
		},
	}

	checksum.Update(kvs)

	var kvBytes uint64
	for _, kv := range kvs {
		kvBytes += uint64(len(kv.Key) + len(kv.Val))
	}
	c.Assert(checksum.SumSize(), Equals, kvBytes)
	c.Assert(checksum.SumKVS(), Equals, uint64(len(kvs)))
	c.Assert(checksum.Sum(), Equals, excpectChecksum)

	// recompute on same key-value
	checksum.Update(kvs)
	c.Assert(checksum.SumSize(), Equals, kvBytes<<1)
	c.Assert(checksum.SumKVS(), Equals, uint64(len(kvs))<<1)
	c.Assert(uint64NotEqual(checksum.Sum(), excpectChecksum), IsTrue)
}
