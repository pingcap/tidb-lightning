package mydump_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/ingest/config"
	md "github.com/pingcap/tidb-lightning/ingest/mydump"
)

var _ = Suite(&testMydumpLoaderSuite{})

func TestLoader(t *testing.T) {
	TestingT(t)
}

type testMydumpLoaderSuite struct{}

func (s *testMydumpLoaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpLoaderSuite) TearDownSuite(c *C) {}

func (s *testMydumpLoaderSuite) TestLoader(c *C) {
	cfg := &config.Config{SourceDir: "./examples"}
	mdl := md.NewMyDumpLoader(cfg)
	dbMeta := mdl.GetTree()

	c.Assert(dbMeta.Name, Equals, "mocker_test")
	c.Assert(len(dbMeta.Tables), Equals, 2)

	for _, table := range []string{"tbl_multi_index", "tbl_autoid"} {
		c.Assert(dbMeta.Tables[table].Name, Equals, table)
	}

	c.Assert(len(dbMeta.Tables["tbl_autoid"].DataFiles), Equals, 1)
	c.Assert(len(dbMeta.Tables["tbl_multi_index"].DataFiles), Equals, 1)
}
