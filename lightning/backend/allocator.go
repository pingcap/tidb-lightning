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

package backend

import (
	"sync/atomic"

	"github.com/pingcap/tidb/meta/autoid"
)

// PanickingAllocator is an ID allocator which panics on all operations except Rebase
type PanickingAllocator struct {
	autoid.Allocator
	base int64
}

// NewPanickingAllocator creates a new PanickingAllocator.
func NewPanickingAllocator(base int64) *PanickingAllocator {
	return &PanickingAllocator{base: base}
}

// Rebase implements the autoid.Allocator interface
func (alloc *PanickingAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	// CAS
	for {
		oldBase := atomic.LoadInt64(&alloc.base)
		if newBase <= oldBase {
			break
		}
		if atomic.CompareAndSwapInt64(&alloc.base, oldBase, newBase) {
			break
		}
	}
	return nil
}

// Base implements the autoid.Allocator interface
func (alloc *PanickingAllocator) Base() int64 {
	return atomic.LoadInt64(&alloc.base)
}
