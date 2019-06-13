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

import (
	"container/list"
	"context"
	"sync"
)

// ConfigList is a goroutine-safe FIFO list of *Config, which supports removal
// from the middle. The list is not expected to be very long.
type ConfigList struct {
	cond      *sync.Cond
	nextID    uint32
	taskIDMap map[uint32]*list.Element
	nodes     list.List
}

// NewConfigList creates a new ConfigList instance.
func NewConfigList() *ConfigList {
	return &ConfigList{
		cond:      sync.NewCond(new(sync.Mutex)),
		taskIDMap: make(map[uint32]*list.Element),
	}
}

// Push adds a configuration to the end of the list. The field `cfg.TaskID` will
// be modified to include a unique ID to identify this task.
func (cl *ConfigList) Push(cfg *Config) {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	id := cl.nextID
	cl.nextID++
	cfg.TaskID = id
	cl.taskIDMap[id] = cl.nodes.PushBack(cfg)
	cl.cond.Broadcast()
}

// Pop removes a configuration from the front of the list. If the list is empty,
// this method will block until either another goroutines calls Push() or the
// input context expired.
//
// If the context expired, the error field will contain the error from context.
func (cl *ConfigList) Pop(ctx context.Context) (*Config, error) {
	res := make(chan *Config)

	go func() {
		cl.cond.L.Lock()
		defer cl.cond.L.Unlock()
		for {
			if front := cl.nodes.Front(); front != nil {
				cfg := front.Value.(*Config)
				delete(cl.taskIDMap, cfg.TaskID)
				cl.nodes.Remove(front)
				res <- cfg
				break
			}
			cl.cond.Wait()
		}
	}()

	select {
	case cfg := <-res:
		return cfg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Remove removes a task from the list given its task ID. Returns true if a task
// is successfully removed, false if the task ID did not exist.
func (cl *ConfigList) Remove(taskID uint32) bool {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return false
	}
	delete(cl.taskIDMap, taskID)
	cl.nodes.Remove(element)
	return true
}

// Get obtains a task from the list given its task ID. If the task ID did not
// exist, the returned bool field will be false.
func (cl *ConfigList) Get(taskID uint32) (*Config, bool) {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return nil, false
	}
	return element.Value.(*Config), true
}

// AllIDs returns a list of all task IDs in the list.
func (cl *ConfigList) AllIDs() []uint32 {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	res := make([]uint32, 0, len(cl.taskIDMap))
	for taskID := range cl.taskIDMap {
		res = append(res, taskID)
	}
	return res
}
