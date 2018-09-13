package restore

import (
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
)

var _ = Suite(&checkReqSuite{})

type checkReqSuite struct{}

func (s *checkReqSuite) TestExtractTiDBVersion(c *C) {
	vers, err := extractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	vers, err = extractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.4"))

	vers, err = extractTiDBVersion("5.7.10-TiDB-v2.0.7")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.7"))

	vers, err = extractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	_, err = extractTiDBVersion("")
	c.Assert(err, NotNil)

	_, err = extractTiDBVersion("8.0.12")
	c.Assert(err, NotNil)

	_, err = extractTiDBVersion("not-a-valid-version")
	c.Assert(err, NotNil)
}
