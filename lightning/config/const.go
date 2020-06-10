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

package config

const (
	_K = int64(1 << 10)
	_M = _K << 10
	_G = _M << 10

	// mydumper
	ReadBlockSize   int64 = 64 * _K
	MinRegionSize   int64 = 256 * _M
	MaxRegionSize   int64 = 256 * _M
	SplitRegionSize int64 = 96 * _M

	BufferSizeScale = 5

	defaultMaxAllowedPacket = 64 * 1024 * 1024
)
