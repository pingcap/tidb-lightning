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

package backend_test

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"

	kvpb "github.com/pingcap/kvproto/pkg/import_kvpb"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/mock"
)

type importerSuite struct {
	controller *gomock.Controller
	mockClient *mock.MockImportKVClient
	mockWriter *mock.MockImportKV_WriteEngineClient
	ctx        context.Context
	engineUUID []byte
	engine     *kv.OpenedEngine
	kvPairs    kv.Rows
}

var _ = Suite(&importerSuite{})

const testPDAddr = "pd-addr:2379"

// FIXME: Cannot use the real SetUpTest/TearDownTest to set up the mock
// otherwise the mock error will be ignored.

func (s *importerSuite) setUpTest(c *C) {
	s.controller = gomock.NewController(c)
	s.mockClient = mock.NewMockImportKVClient(s.controller)
	s.mockWriter = mock.NewMockImportKV_WriteEngineClient(s.controller)
	importer := kv.NewMockImporter(s.mockClient, testPDAddr)

	s.ctx = context.Background()
	engineUUID := uuid.MustParse("7e3f3a3c-67ce-506d-af34-417ec138fbcb")
	s.engineUUID = engineUUID[:]
	s.kvPairs = kv.MakeRowsFromKvPairs([]common.KvPair{
		{
			Key: []byte("k1"),
			Val: []byte("v1"),
		},
		{
			Key: []byte("k2"),
			Val: []byte("v2"),
		},
	})

	s.mockClient.EXPECT().
		OpenEngine(s.ctx, &import_kvpb.OpenEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)

	var err error
	s.engine, err = importer.OpenEngine(s.ctx, "`db`.`table`", -1)
	c.Assert(err, IsNil)
}

func (s *importerSuite) tearDownTest() {
	s.controller.Finish()
}

func (s *importerSuite) TestWriteRows(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(&import_kvpb.WriteEngineRequest{
			Chunk: &import_kvpb.WriteEngineRequest_Head{
				Head: &import_kvpb.WriteHead{Uuid: s.engineUUID},
			},
		}).
		Return(nil)
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch().GetMutations(), DeepEquals, []*import_kvpb.Mutation{
				{Op: import_kvpb.Mutation_Put, Key: []byte("k1"), Value: []byte("v1")},
				{Op: import_kvpb.Mutation_Put, Key: []byte("k2"), Value: []byte("v2")},
			})
			return nil
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, nil).
		After(batchSendCall)

	err := s.engine.WriteRows(s.ctx, nil, s.kvPairs)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestWriteHeadSendFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetHead(), NotNil)
			return errors.Annotate(context.Canceled, "fake unrecoverable write head error")
		})
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(headSendCall)

	err := s.engine.WriteRows(s.ctx, nil, s.kvPairs)
	c.Assert(err, ErrorMatches, "fake unrecoverable write head error.*")
}

func (s *importerSuite) TestWriteBatchSendFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetHead(), NotNil)
			return nil
		})
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return errors.Annotate(context.Canceled, "fake unrecoverable write batch error")
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(batchSendCall)

	err := s.engine.WriteRows(s.ctx, nil, s.kvPairs)
	c.Assert(err, ErrorMatches, "fake unrecoverable write batch error.*")
}

func (s *importerSuite) TestWriteCloseFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockClient.EXPECT().WriteEngine(s.ctx).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetHead(), NotNil)
			return nil
		})
	batchSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return nil
		}).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable close stream error")).
		After(batchSendCall)

	err := s.engine.WriteRows(s.ctx, nil, s.kvPairs)
	c.Assert(err, ErrorMatches, "fake unrecoverable close stream error.*")
}

func (s *importerSuite) TestCloseImportCleanupEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockClient.EXPECT().
		CloseEngine(s.ctx, &import_kvpb.CloseEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		ImportEngine(s.ctx, &import_kvpb.ImportEngineRequest{Uuid: s.engineUUID, PdAddr: testPDAddr}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		CleanupEngine(s.ctx, &import_kvpb.CleanupEngineRequest{Uuid: s.engineUUID}).
		Return(nil, nil)

	engine, err := s.engine.Close(s.ctx)
	c.Assert(err, IsNil)
	err = engine.Import(s.ctx)
	c.Assert(err, IsNil)
	err = engine.Cleanup(s.ctx)
	c.Assert(err, IsNil)
}

func BenchmarkMutationAlloc(b *testing.B) {
	var g *kvpb.Mutation
	for i := 0; i < b.N; i++ {
		m := &kvpb.Mutation{
			Op:    kvpb.Mutation_Put,
			Key:   nil,
			Value: nil,
		}
		g = m
	}

	var _ = g
}

func BenchmarkMutationPool(b *testing.B) {
	p := sync.Pool{
		New: func() interface{} {
			return &kvpb.Mutation{}
		},
	}
	var g *kvpb.Mutation

	for i := 0; i < b.N; i++ {
		m := p.Get().(*kvpb.Mutation)
		m.Op = kvpb.Mutation_Put
		m.Key = nil
		m.Value = nil

		g = m

		p.Put(m)
	}

	var _ = g
}
