package mydump_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
	md "github.com/pingcap/tidb-lightning/lightning/mydump"
)

var _ = Suite(&testMydumpLoaderSuite{})

func TestMydumps(t *testing.T) {
	TestingT(t)
}

type testMydumpLoaderSuite struct{}

func (s *testMydumpLoaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpLoaderSuite) TearDownSuite(c *C) {}

func (s *testMydumpLoaderSuite) TestLoader(c *C) {
	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./not-exists"}}
	mdl, err := md.NewMyDumpLoader(cfg)
	c.Assert(err, NotNil)

	cfg = &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./examples"}}
	mdl, err = md.NewMyDumpLoader(cfg)
	c.Assert(err, IsNil)

	dbMeta := mdl.GetDatabases()["mocker_test"]
	c.Assert(len(dbMeta.Tables), Equals, 4)

	for _, table := range []string{"tbl_multi_index", "tbl_autoid"} {
		c.Assert(dbMeta.Tables[table].Name, Equals, table)
	}

	c.Assert(len(dbMeta.Tables["tbl_autoid"].DataFiles), Equals, 1)
	c.Assert(len(dbMeta.Tables["tbl_multi_index"].DataFiles), Equals, 1)
}
