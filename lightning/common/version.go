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

package common

import (
	"fmt"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
)

// Version information.
var (
	ReleaseVersion = "None"
	BuildTS        = "None"
	GitHash        = "None"
	GitBranch      = "None"
	GoVersion      = "None"
)

// GetRawInfo do what its name tells
func GetRawInfo() string {
	var info string
	info += fmt.Sprintf("Release Version: %s\n", ReleaseVersion)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", GoVersion)
	return info
}

// PrintInfo prints some information of the app, like git hash, binary build time, etc.
func PrintInfo(app string, callback func()) {
	oldLevel := log.SetLevel(zap.InfoLevel)
	defer log.SetLevel(oldLevel)

	log.L().Info("Welcome to "+app,
		zap.String("Release Version", ReleaseVersion),
		zap.String("Git Commit Hash", GitHash),
		zap.String("Git Branch", GitBranch),
		zap.String("UTC Build Time", BuildTS),
		zap.String("Go Version", GoVersion),
	)

	if callback != nil {
		callback()
	}
}
