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
