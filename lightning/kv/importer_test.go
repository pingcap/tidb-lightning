package kv

import (
	"context"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	. "github.com/pingcap/tidb-lightning/mock"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/satori/go.uuid"
)

type importerSuite struct {
	controller *gomock.Controller
	mockClient *MockImportKVClient
	mockWriter *MockImportKV_WriteEngineClient
	importer   *Importer
}

var _ = Suite(&importerSuite{})

const testPDAddr = "pd-addr:2379"

// FIXME: Cannot use the real SetUpTest/TearDownTest to set up the mock
// otherwise the mock error will be ignored.

func (s *importerSuite) setUpTest(c *C) {
	s.controller = gomock.NewController(c)
	s.mockClient = NewMockImportKVClient(s.controller)
	s.mockWriter = NewMockImportKV_WriteEngineClient(s.controller)
	s.importer = NewMockImporter(s.mockClient, testPDAddr)
}

func (s *importerSuite) tearDownTest() {
	s.controller.Finish()
}

func (s *importerSuite) TestSwitchMode(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().
		SwitchMode(ctx, &import_kvpb.SwitchModeRequest{
			PdAddr:  testPDAddr,
			Request: &import_sstpb.SwitchModeRequest{Mode: import_sstpb.SwitchMode_Import},
		}).
		Return(nil, nil)

	err := s.importer.SwitchMode(ctx, import_sstpb.SwitchMode_Import)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestCompact(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().
		CompactCluster(ctx, &import_kvpb.CompactClusterRequest{
			PdAddr:  testPDAddr,
			Request: &import_sstpb.CompactRequest{OutputLevel: -1},
		}).
		Return(nil, nil)

	err := s.importer.Compact(ctx, -1)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestOpenCloseImportCleanUpEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.FromStringOrNil("902efee3-a3f9-53d4-8c82-f12fb1900cd1").Bytes()

	openCall := s.mockClient.EXPECT().
		OpenEngine(ctx, &import_kvpb.OpenEngineRequest{Uuid: engineUUID}).
		Return(nil, nil)
	closeCall := s.mockClient.EXPECT().
		CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: engineUUID}).
		Return(nil, nil).
		After(openCall)
	importCall := s.mockClient.EXPECT().
		ImportEngine(ctx, &import_kvpb.ImportEngineRequest{Uuid: engineUUID, PdAddr: testPDAddr}).
		Return(nil, nil).
		After(closeCall)
	s.mockClient.EXPECT().
		CleanupEngine(ctx, &import_kvpb.CleanupEngineRequest{Uuid: engineUUID}).
		Return(nil, nil).
		After(importCall)

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	closedEngine, err := engine.Close(ctx)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestUnsafeCloseEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.FromStringOrNil("7e3f3a3c-67ce-506d-af34-417ec138fbcb").Bytes()

	closeCall := s.mockClient.EXPECT().
		CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: engineUUID}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		CleanupEngine(ctx, &import_kvpb.CleanupEngineRequest{Uuid: engineUUID}).
		Return(nil, nil).
		After(closeCall)

	closedEngine, err := s.importer.UnsafeCloseEngine(ctx, "`db`.`table`", -1)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestUnsafeCloseEngineWithUUID(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.FromStringOrNil("f1240229-79e0-4d8d-bda0-a211bf493796")

	closeCall := s.mockClient.EXPECT().
		CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: engineUUID.Bytes()}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		CleanupEngine(ctx, &import_kvpb.CleanupEngineRequest{Uuid: engineUUID.Bytes()}).
		Return(nil, nil).
		After(closeCall)

	closedEngine, err := s.importer.UnsafeCloseEngineWithUUID(ctx, "some_tag", engineUUID)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestWriteEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.FromStringOrNil("902efee3-a3f9-53d4-8c82-f12fb1900cd1").Bytes()
	commitTS := uint64(1557763041)

	s.mockClient.EXPECT().
		OpenEngine(ctx, &import_kvpb.OpenEngineRequest{Uuid: engineUUID}).
		Return(nil, nil)
	s.mockClient.EXPECT().
		WriteEngine(ctx).
		Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(&import_kvpb.WriteEngineRequest{
			Chunk: &import_kvpb.WriteEngineRequest_Head{
				Head: &import_kvpb.WriteHead{Uuid: engineUUID},
			},
		}).
		Return(nil)
	batchSendCall := s.mockWriter.EXPECT().
		Send(&import_kvpb.WriteEngineRequest{
			Chunk: &import_kvpb.WriteEngineRequest_Batch{
				Batch: &import_kvpb.WriteBatch{
					CommitTs: commitTS,
					Mutations: []*import_kvpb.Mutation{
						{Op: import_kvpb.Mutation_Put, Key: []byte("k1"), Value: []byte("v1")},
						{Op: import_kvpb.Mutation_Put, Key: []byte("k2"), Value: []byte("v2")},
					},
				},
			},
		}).
		Return(nil).
		After(headSendCall)
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, nil).
		After(batchSendCall)

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	engine.ts = commitTS
	stream, err := engine.NewWriteStream(ctx)
	c.Assert(err, IsNil)
	err = stream.Put([]kvenc.KvPair{
		{Key: []byte("k1"), Val: []byte("v1")},
		{Key: []byte("k2"), Val: []byte("v2")},
	})
	c.Assert(err, IsNil)
	err = stream.Close()
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestOpenEngineFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).
		Return(nil, errors.New("fake unrecoverable open error"))

	_, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, ErrorMatches, "fake unrecoverable open error")
}

func (s *importerSuite) TestOpenEngineIgnorableError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).
		Return(nil, errors.New("FileExists"))

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	c.Assert(engine, NotNil)
}

func (s *importerSuite) TestWriteEngineFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).
		Return(nil, errors.New("fake unrecoverable write error"))

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	_, err = engine.NewWriteStream(ctx)
	c.Assert(err, ErrorMatches, "fake unrecoverable write error")
}

func (s *importerSuite) TestWriteHeadSendFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetHead(), NotNil)
			return errors.New("fake unrecoverable write head error")
		})
	s.mockWriter.EXPECT().
		CloseAndRecv().
		Return(nil, errors.New("fake unrecoverable close stream error")).
		After(headSendCall)

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	_, err = engine.NewWriteStream(ctx)
	c.Assert(err, ErrorMatches, "fake unrecoverable write head error")
}

func (s *importerSuite) TestWriteBatchSendFailedNoRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).Return(s.mockWriter, nil)

	headSendCall := s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetHead(), NotNil)
			return nil
		})
	s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return errors.Annotate(context.Canceled, "fake unrecoverable write batch error")
		}).
		After(headSendCall)

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	stream, err := engine.NewWriteStream(ctx)
	c.Assert(err, IsNil)
	err = stream.Put([]kvenc.KvPair{{Key: []byte("key"), Val: []byte("value")}})
	c.Assert(err, ErrorMatches, "fake unrecoverable write batch error.*")
}

func (s *importerSuite) TestWriteBatchSendFailedWithRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).Return(s.mockWriter, nil)

	s.mockWriter.EXPECT().Send(gomock.Any()).Return(nil)
	s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return errors.New("fake recoverable write batch error")
		}).
		MinTimes(2)

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	stream, err := engine.NewWriteStream(ctx)
	c.Assert(err, IsNil)
	err = stream.Put([]kvenc.KvPair{{Key: []byte("key"), Val: []byte("value")}})
	c.Assert(err, ErrorMatches, "fake recoverable write batch error")
}

func (s *importerSuite) TestWriteBatchSendRecovered(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().WriteEngine(ctx, gomock.Any()).Return(s.mockWriter, nil)

	s.mockWriter.EXPECT().Send(gomock.Any()).Return(nil)
	s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return errors.New("fake recoverable write batch error")
		})
	s.mockWriter.EXPECT().
		Send(gomock.Any()).
		DoAndReturn(func(x *import_kvpb.WriteEngineRequest) error {
			c.Assert(x.GetBatch(), NotNil)
			return nil
		})

	engine, err := s.importer.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	stream, err := engine.NewWriteStream(ctx)
	c.Assert(err, IsNil)
	err = stream.Put([]kvenc.KvPair{{Key: []byte("key"), Val: []byte("value")}})
	c.Assert(err, IsNil)
}

func (s *importerSuite) TestImportFailedNoRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(nil, errors.Annotate(context.Canceled, "fake unrecoverable import error"))

	closedEngine, err := s.importer.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, ErrorMatches, "fake unrecoverable import error.*")
}

func (s *importerSuite) TestImportFailedWithRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(nil, errors.New("fake recoverable import error")).
		MinTimes(2)

	closedEngine, err := s.importer.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, ErrorMatches, ".*fake recoverable import error")
}

func (s *importerSuite) TestImportFailedRecovered(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockClient.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil, nil)
	s.mockClient.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(nil, errors.New("fake recoverable import error"))
	s.mockClient.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(nil, nil)

	closedEngine, err := s.importer.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, IsNil)
}
