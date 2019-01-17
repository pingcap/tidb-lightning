package worker_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

type testWorkerPool struct{}

func (s *testWorkerPool) SetUpSuite(c *C)    {}
func (s *testWorkerPool) TearDownSuite(c *C) {}

var _ = Suite(&testWorkerPool{})

func TestNewRestoreWorkerPool(t *testing.T) {
	TestingT(t)
}

func (s *testWorkerPool) TestApplyRecycle(c *C) {
	pool := worker.NewPool(context.Background(), 3, "test")

	w1, w2, w3 := pool.Apply(), pool.Apply(), pool.Apply()
	c.Assert(w1.ID, Equals, int64(1))
	c.Assert(w2.ID, Equals, int64(2))
	c.Assert(w3.ID, Equals, int64(3))
	c.Assert(pool.HasWorker(), Equals, false)

	pool.Recycle(w3)
	c.Assert(pool.HasWorker(), Equals, true)
	c.Assert(pool.Apply(), Equals, w3)
	pool.Recycle(w2)
	c.Assert(pool.Apply(), Equals, w2)
	pool.Recycle(w1)
	c.Assert(pool.Apply(), Equals, w1)

	c.Assert(pool.HasWorker(), Equals, false)
}
