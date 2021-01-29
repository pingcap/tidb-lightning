package backend_test

import (
	"context"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"

	kv "github.com/pingcap/tidb-lightning/lightning/backend"
	"github.com/pingcap/tidb-lightning/mock"
)

type backendSuite struct {
	controller  *gomock.Controller
	mockBackend *mock.MockBackend
	backend     kv.Backend
}

var _ = Suite(&backendSuite{})

// FIXME: Cannot use the real SetUpTest/TearDownTest to set up the mock
// otherwise the mock error will be ignored.

func (s *backendSuite) setUpTest(c *C) {
	s.controller = gomock.NewController(c)
	s.mockBackend = mock.NewMockBackend(s.controller)
	s.backend = kv.MakeBackend(s.mockBackend)
}

func (s *backendSuite) tearDownTest() {
	s.controller.Finish()
}

func (s *backendSuite) TestOpenCloseImportCleanUpEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("902efee3-a3f9-53d4-8c82-f12fb1900cd1")

	openCall := s.mockBackend.EXPECT().
		OpenEngine(ctx, engineUUID).
		Return(nil)
	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil).
		After(openCall)
	importCall := s.mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID).
		Return(nil).
		After(closeCall)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(importCall)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	closedEngine, err := engine.Close(ctx)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestUnsafeCloseEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("7e3f3a3c-67ce-506d-af34-417ec138fbcb")

	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(closeCall)

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, "`db`.`table`", -1)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestUnsafeCloseEngineWithUUID(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("f1240229-79e0-4d8d-bda0-a211bf493796")

	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, engineUUID).
		Return(nil)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(closeCall)

	closedEngine, err := s.backend.UnsafeCloseEngineWithUUID(ctx, "some_tag", engineUUID)
	c.Assert(err, IsNil)
	err = closedEngine.Cleanup(ctx)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestWriteEngine(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("902efee3-a3f9-53d4-8c82-f12fb1900cd1")

	rows1 := mock.NewMockRows(s.controller)
	rows2 := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().
		OpenEngine(ctx, engineUUID).
		Return(nil)

	mockWriter := mock.NewMockEngineWriter(s.controller)
	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(kv.LocalMemoryTableSize)).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, "`db`.`table`", []string{"c1", "c2"}, gomock.Any(), rows1).
		Return(nil)
	mockWriter.EXPECT().Close().Return(nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, "`db`.`table`", []string{"c1", "c2"}, gomock.Any(), rows2).
		Return(nil)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, []string{"c1", "c2"}, rows1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, []string{"c1", "c2"}, rows2)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestWriteToEngineWithNothing(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	emptyRows := mock.NewMockRows(s.controller)
	writer := mock.NewMockEngineWriter(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil)
	writer.EXPECT().AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), emptyRows).Return(nil)
	writer.EXPECT().Close().Return(nil)
	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(kv.LocalMemoryTableSize)).Return(writer, nil)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, nil, emptyRows)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestOpenEngineFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).
		Return(errors.New("fake unrecoverable open error"))

	_, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, ErrorMatches, "fake unrecoverable open error")
}

func (s *backendSuite) TestWriteEngineFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	rows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil)
	mockWriter := mock.NewMockEngineWriter(s.controller)
	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(kv.LocalMemoryTableSize)).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable write error"))
	mockWriter.EXPECT().Close().Return(nil)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, nil, rows)
	c.Assert(err, ErrorMatches, "fake unrecoverable write error.*")
}

func (s *backendSuite) TestWriteBatchSendFailedWithRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	rows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil)
	mockWriter := mock.NewMockEngineWriter(s.controller)
	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), int64(kv.LocalMemoryTableSize)).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(errors.New("fake recoverable write batch error")).
		MinTimes(1)
	mockWriter.EXPECT().Close().Return(nil).MinTimes(1)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, nil, rows)
	c.Assert(err, ErrorMatches, ".*fake recoverable write batch error")
}

func (s *backendSuite) TestImportFailedNoRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable import error"))

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, ErrorMatches, "fake unrecoverable import error.*")
}

func (s *backendSuite) TestImportFailedWithRetry(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(errors.New("fake recoverable import error")).
		MinTimes(2)
	s.mockBackend.EXPECT().RetryImportDelay().Return(time.Duration(0)).AnyTimes()

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, ErrorMatches, ".*fake recoverable import error")
}

func (s *backendSuite) TestImportFailedRecovered(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(errors.New("fake recoverable import error"))
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any()).
		Return(nil)
	s.mockBackend.EXPECT().RetryImportDelay().Return(time.Duration(0)).AnyTimes()

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = closedEngine.Import(ctx)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestClose(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockBackend.EXPECT().Close().Return()

	s.backend.Close()
}

func (s *backendSuite) TestMakeEmptyRows(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	rows := mock.NewMockRows(s.controller)
	s.mockBackend.EXPECT().MakeEmptyRows().Return(rows)

	c.Assert(s.mockBackend.MakeEmptyRows(), Equals, rows)
}

func (s *backendSuite) TestNewEncoder(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	encoder := mock.NewMockEncoder(s.controller)
	options := &kv.SessionOptions{SQLMode: mysql.ModeANSIQuotes, Timestamp: 1234567890}
	s.mockBackend.EXPECT().NewEncoder(nil, options).Return(encoder, nil)

	realEncoder, err := s.mockBackend.NewEncoder(nil, options)
	c.Assert(realEncoder, Equals, encoder)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestCheckDiskQuota(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	uuid1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid3 := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	uuid5 := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	uuid7 := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	uuid9 := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	fileSizes := []kv.EngineFileSize{
		{
			UUID:        uuid1,
			DiskSize:    1000,
			MemSize:     0,
			IsImporting: false,
		},
		{
			UUID:        uuid3,
			DiskSize:    2000,
			MemSize:     1000,
			IsImporting: true,
		},
		{
			UUID:        uuid5,
			DiskSize:    1500,
			MemSize:     3500,
			IsImporting: false,
		},
		{
			UUID:        uuid7,
			DiskSize:    0,
			MemSize:     7000,
			IsImporting: true,
		},
		{
			UUID:        uuid9,
			DiskSize:    4500,
			MemSize:     4500,
			IsImporting: false,
		},
	}

	s.mockBackend.EXPECT().EngineFileSizes().Return(fileSizes).Times(4)

	// No quota exceeded
	le, iple, ds, ms := s.backend.CheckDiskQuota(30000)
	c.Assert(le, HasLen, 0)
	c.Assert(iple, Equals, 0)
	c.Assert(ds, Equals, int64(9000))
	c.Assert(ms, Equals, int64(16000))

	// Quota exceeded, the largest one is out
	le, iple, ds, ms = s.backend.CheckDiskQuota(20000)
	c.Assert(le, DeepEquals, []uuid.UUID{uuid9})
	c.Assert(iple, Equals, 0)
	c.Assert(ds, Equals, int64(9000))
	c.Assert(ms, Equals, int64(16000))

	// Quota exceeded, the importing one should be ranked least priority
	le, iple, ds, ms = s.backend.CheckDiskQuota(12000)
	c.Assert(le, DeepEquals, []uuid.UUID{uuid5, uuid9})
	c.Assert(iple, Equals, 0)
	c.Assert(ds, Equals, int64(9000))
	c.Assert(ms, Equals, int64(16000))

	// Quota exceeded, the importing ones should not be visible
	le, iple, ds, ms = s.backend.CheckDiskQuota(5000)
	c.Assert(le, DeepEquals, []uuid.UUID{uuid1, uuid5, uuid9})
	c.Assert(iple, Equals, 1)
	c.Assert(ds, Equals, int64(9000))
	c.Assert(ms, Equals, int64(16000))
}
