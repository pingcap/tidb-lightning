package kv

import "sync/atomic"

// PanickingAllocator is an ID allocator which panics on all operations except Rebase
type PanickingAllocator struct {
	base int64
}

func NewPanickingAllocator(base int64) *PanickingAllocator {
	return &PanickingAllocator{base: base}
}

func (alloc *PanickingAllocator) Alloc(int64) (int64, error) {
	panic("unexpected Alloc() call")
}

func (alloc *PanickingAllocator) Reset(newBase int64) {
	panic("unexpected Reset() call")
}

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

func (alloc *PanickingAllocator) Base() int64 {
	return atomic.LoadInt64(&alloc.base)
}

func (alloc *PanickingAllocator) End() int64 {
	panic("unexpected End() call")
}

func (alloc *PanickingAllocator) NextGlobalAutoID(tableID int64) (int64, error) {
	panic("unexpected NextGlobalAutoID() call")
}
