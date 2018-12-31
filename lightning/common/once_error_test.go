package common_test

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
)

func TestCommon(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&onceErrorSuite{})

type onceErrorSuite struct{}

func (s *onceErrorSuite) TestOnceError(c *C) {
	var err common.OnceError

	c.Assert(err.Get(), IsNil)

	err.Set("tag", nil)
	c.Assert(err.Get(), IsNil)

	e := errors.New("1")
	err.Set("tag", e)
	c.Assert(err.Get(), Equals, e)

	e2 := errors.New("2")
	err.Set("tag", e2)
	c.Assert(err.Get(), Equals, e) // e, not e2.

	err.Set("tag", nil)
	c.Assert(err.Get(), Equals, e)

	ch := make(chan struct{})
	go func() {
		err.Set("tag", nil)
		ch <- struct{}{}
	}()
	<-ch
	c.Assert(err.Get(), Equals, e)
}
