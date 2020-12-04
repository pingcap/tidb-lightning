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

	rows0 := mock.NewMockRows(s.controller)
	rows1 := mock.NewMockRows(s.controller)
	rows2 := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().
		OpenEngine(ctx, engineUUID).
		Return(nil)
	s.mockBackend.EXPECT().
		MaxChunkSize().
		Return(654321)
	rows0.EXPECT().
		SplitIntoChunks(654321).
		Return([]kv.Rows{rows1, rows2})
	s.mockBackend.EXPECT().
		WriteRows(ctx, engineUUID, "`db`.`table`", []string{"c1", "c2"}, gomock.Any(), rows1).
		Return(nil)
	s.mockBackend.EXPECT().
		WriteRows(ctx, engineUUID, "`db`.`table`", []string{"c1", "c2"}, gomock.Any(), rows2).
		Return(nil)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, []string{"c1", "c2"}, rows0)
	c.Assert(err, IsNil)
}

func (s *backendSuite) TestWriteToEngineWithNothing(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	emptyRows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().MaxChunkSize().Return(654321)
	emptyRows.EXPECT().SplitIntoChunks(654321).Return([]kv.Rows{})

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
	s.mockBackend.EXPECT().MaxChunkSize().Return(123)
	rows.EXPECT().SplitIntoChunks(123).Return([]kv.Rows{rows})

	s.mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable write error"))

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
	s.mockBackend.EXPECT().MaxChunkSize().Return(123)
	rows.EXPECT().SplitIntoChunks(123).Return([]kv.Rows{rows})

	s.mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(errors.New("fake recoverable write batch error")).
		MinTimes(2)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, nil, rows)
	c.Assert(err, ErrorMatches, ".*fake recoverable write batch error")
}

func (s *backendSuite) TestWriteBatchSendRecovered(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	ctx := context.Background()
	rows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().MaxChunkSize().Return(123)
	rows.EXPECT().SplitIntoChunks(123).Return([]kv.Rows{rows})

	s.mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(errors.New("fake recoverable write batch error"))
	s.mockBackend.EXPECT().
		WriteRows(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rows).
		Return(nil)

	engine, err := s.backend.OpenEngine(ctx, "`db`.`table`", 1)
	c.Assert(err, IsNil)
	err = engine.WriteRows(ctx, nil, rows)
	c.Assert(err, IsNil)
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
	s.mockBackend.EXPECT().NewEncoder(nil, options).Return(encoder)

	c.Assert(s.mockBackend.NewEncoder(nil, options), Equals, encoder)
}
