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

	dbMetas := mdl.GetDatabases()
	c.Assert(len(dbMetas), Equals, 1)
	dbMeta := dbMetas[0]
	c.Assert(dbMeta.Name, Equals, "mocker_test")
	c.Assert(len(dbMeta.Tables), Equals, 4)

	expected := []struct {
		name      string
		dataFiles int
	}{
		{name: "i", dataFiles: 1},
		{name: "report_case_high_risk", dataFiles: 1},
		{name: "tbl_autoid", dataFiles: 1},
		{name: "tbl_multi_index", dataFiles: 1},
	}

	for i, table := range expected {
		c.Assert(dbMeta.Tables[i].Name, Equals, table.name)
		c.Assert(len(dbMeta.Tables[i].DataFiles), Equals, table.dataFiles)
	}
}
