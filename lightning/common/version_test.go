// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common_test

import (
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"

	"github.com/pingcap/tidb-lightning/lightning/common"
)

func (s *utilSuite) TestVersion(c *C) {
	common.ReleaseVersion = "ReleaseVersion"
	common.BuildTS = "BuildTS"
	common.GitHash = "GitHash"
	common.GitBranch = "GitBranch"
	common.GoVersion = "GoVersion"

	version := common.GetRawInfo()
	c.Assert(version, Equals, `Release Version: ReleaseVersion
Git Commit Hash: GitHash
Git Branch: GitBranch
UTC Build Time: BuildTS
Go Version: GoVersion
`)
	common.PrintInfo("test", func() {
		common.ReleaseVersion = "None"
		common.BuildTS = "None"
		common.GitHash = "None"
		common.GitBranch = "None"
		common.GoVersion = "None"
	})

	version = common.GetRawInfo()
	c.Assert(version, Equals, `Release Version: None
Git Commit Hash: None
Git Branch: None
UTC Build Time: None
Go Version: None
`)
}

func (s *utilSuite) TestExtractTiDBVersion(c *C) {
	vers, err := common.ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	vers, err = common.ExtractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.4"))

	vers, err = common.ExtractTiDBVersion("5.7.10-TiDB-v2.0.7")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.7"))

	vers, err = common.ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = common.ExtractTiDBVersion("5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.0-beta"))

	vers, err = common.ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5"))

	vers, err = common.ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = common.ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	_, err = common.ExtractTiDBVersion("")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = common.ExtractTiDBVersion("8.0.12")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = common.ExtractTiDBVersion("not-a-valid-version")
	c.Assert(err, NotNil)
}
