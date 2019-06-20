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
	"context"
	"sync"
	"sync/atomic"
)

// The implementation is based on https://github.com/golang/sync/blob/master/semaphore/semaphore.go

// Pauser is a type which could allow multiple goroutines to wait on demand,
// similar to a gate or traffic light.
type Pauser struct {
	isPaused uint32
	mu       sync.Mutex
	waiters  map[chan<- struct{}]struct{}
}

// NewPauser returns an initialized pauser.
func NewPauser() *Pauser {
	return &Pauser{
		waiters: make(map[chan<- struct{}]struct{}, 32),
	}
}

func (p *Pauser) set(isPaused bool) {
	var newPaused uint32
	if isPaused {
		newPaused = 1
	}

	oldPaused := atomic.SwapUint32(&p.isPaused, newPaused)
	if oldPaused == 0 || isPaused {
		return
	}

	// extract all waiters, then notify them we changed from "Paused" to "Not Paused".
	p.mu.Lock()
	allWaiters := p.waiters
	p.waiters = make(map[chan<- struct{}]struct{}, len(allWaiters))
	p.mu.Unlock()

	for waiter := range allWaiters {
		close(waiter)
	}
}

// Pause causes all calls to Wait() to block.
func (p *Pauser) Pause() {
	p.set(true)
}

// Resume causes all calls to Wait() to continue.
func (p *Pauser) Resume() {
	p.set(false)
}

// IsPaused gets whether the current state is paused or not.
func (p *Pauser) IsPaused() bool {
	return atomic.LoadUint32(&p.isPaused) != 0
}

// Wait blocks the current goroutine if the current state is paused, until the
// pauser itself is resumed at least once.
//
// If `ctx` is done, this method will also unblock immediately, and return the
// context error.
func (p *Pauser) Wait(ctx context.Context) error {
	if !p.IsPaused() {
		return nil
	}

	waiter := make(chan struct{})

	p.mu.Lock()
	p.waiters[waiter] = struct{}{}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		p.mu.Lock()
		delete(p.waiters, waiter)
		p.mu.Unlock()
		return err

	case <-waiter:
		return nil
	}
}
