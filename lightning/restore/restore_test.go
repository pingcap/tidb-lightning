package restore_test

// Ensure the number of open engines will never exceed table-concurrency.

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"github.com/satori/go.uuid"

	. "github.com/pingcap/check"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	tablesCount = 35
	importDelay = 500 * time.Millisecond
)

var _ = Suite(&testRestoreSuite{})

type testRestoreSuite struct{}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *testRestoreSuite) SetUpSuite(c *C)    {}
func (s *testRestoreSuite) TearDownSuite(c *C) {}

type mockKVEvent int

const (
	importEvent mockKVEvent = iota
	writeHeadEvent
	writeBatchEvent
)

// A mock tikv-importer service which:
//  - emits a custom error when the number of open engine exceeds the given table concurrency.
//  - throttle the import speed at 0.3s/engine to simulate accumulation of open engines in [TOOL-296].
//  - provides channels to let the client check if all events are properly delivered
//
// [TOOL-296]: https://internal.pingcap.net/jira/browse/TOOL-296
//
// Previously, closing an engine is placed on a serial "post-processing" queue
// together with other time-consuming actions like import and checksum,
// while opening an engine is done in a worker pool which will be recycled as soon as
// all data are transferred. The effect is that the number of open engines will increase unbounded
// as long as the post-processing serial queue is blocked.
// In this mock service, we simulate the blocking behavior by slowing down the import operation.
// This issue has been fixed by PR #59 which moves the close operation into the worker pool.
type mockKVService struct {
	engineLock              sync.Mutex
	engineList              map[string]int // string(uuid) -> writer count
	events                  chan<- mockKVEvent
	engineOverflowLimit     int
	engineOverflowErrorFunc func() error
}

func formatUuid(uuidBytes []byte) string {
	uuidValue, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		panic(err)
	}
	return uuidValue.String()
}

func (s *mockKVService) SwitchMode(context.Context, *pb.SwitchModeRequest) (*pb.SwitchModeResponse, error) {
	return &pb.SwitchModeResponse{}, nil
}
func (s *mockKVService) OpenEngine(_ context.Context, req *pb.OpenEngineRequest) (*pb.OpenEngineResponse, error) {
	s.engineLock.Lock()
	defer s.engineLock.Unlock()
	uuid := formatUuid(req.Uuid)
	s.engineList[uuid] += 0
	if len(s.engineList) > s.engineOverflowLimit {
		common.AppLogger.Errorf("[mock-importer] more than %d engines open: %v", s.engineOverflowLimit, s.engineList)
		return nil, s.engineOverflowErrorFunc()
	}
	common.AppLogger.Infof("[mock-importer] opened engine %s; %v", uuid, s.engineList)
	return &pb.OpenEngineResponse{}, nil
}
func (s *mockKVService) WriteEngine(wes pb.ImportKV_WriteEngineServer) error {
	var engine string
	for {
		req, err := wes.Recv()
		switch err {
		case nil:
			if head := req.GetHead(); head != nil {
				engine = formatUuid(head.Uuid)
				common.AppLogger.Infof("[mock-importer] start write to engine %s", engine)
				s.engineLock.Lock()
				s.engineList[engine] += 1
				s.engineLock.Unlock()
				s.events <- writeHeadEvent
			} else if req.GetBatch() != nil {
				s.events <- writeBatchEvent
			} else {
				panic("Unexpected event type?")
			}
		case io.EOF:
			common.AppLogger.Infof("[mock-importer] end write to engine %s", engine)
			s.engineLock.Lock()
			s.engineList[engine] -= 1
			s.engineLock.Unlock()
			return wes.SendAndClose(&pb.WriteEngineResponse{Error: nil})
		default:
			return err
		}
	}
}
func (s *mockKVService) CloseEngine(_ context.Context, req *pb.CloseEngineRequest) (*pb.CloseEngineResponse, error) {
	s.engineLock.Lock()
	defer s.engineLock.Unlock()
	uuid := formatUuid(req.Uuid)
	writerCount, exists := s.engineList[uuid]
	if !exists {
		return nil, fmt.Errorf("Engine %s not found", uuid)
	}
	if writerCount > 0 {
		return nil, fmt.Errorf("Engine %s still in use with %d writers left (EngineInUse)", uuid, writerCount)
	}
	delete(s.engineList, uuid)
	common.AppLogger.Infof("[mock-importer] removed engine %s; %v", uuid, s.engineList)
	return &pb.CloseEngineResponse{}, nil
}
func (s *mockKVService) ImportEngine(context.Context, *pb.ImportEngineRequest) (*pb.ImportEngineResponse, error) {
	s.engineLock.Lock()
	defer s.engineLock.Unlock()
	if len(s.engineList) > 0 {
		// only simulate the slow import when multiple engines are open.
		// this should speed up the test while still catching the original problem.
		time.Sleep(importDelay)
	}
	s.events <- importEvent
	return &pb.ImportEngineResponse{}, nil
}
func (s *mockKVService) CleanupEngine(context.Context, *pb.CleanupEngineRequest) (*pb.CleanupEngineResponse, error) {
	return &pb.CleanupEngineResponse{}, nil
}
func (s *mockKVService) CompactCluster(context.Context, *pb.CompactClusterRequest) (*pb.CompactClusterResponse, error) {
	return &pb.CompactClusterResponse{}, nil
}

// Runs the mock tikv-importer gRPC service. Returns the server and its listening address.
func runMockKVServer(c *C, limit int, errorFunc func() error) (*grpc.Server, string, <-chan mockKVEvent) {
	server := grpc.NewServer()
	events := make(chan mockKVEvent, tablesCount*4)
	pb.RegisterImportKVServer(server, &mockKVService{
		engineList:              make(map[string]int),
		events:                  events,
		engineOverflowLimit:     limit,
		engineOverflowErrorFunc: errorFunc,
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)
	go server.Serve(listener)
	return server, listener.Addr().String(), events
}

// Creates the database for the current test.
func createTestDB(c *C) (string, map[string]*mydump.MDDatabaseMeta) {
	dir, err := ioutil.TempDir("", "tidb_lightning_restore_test")
	c.Assert(err, IsNil)

	tables := make(map[string]*mydump.MDTableMeta)

	for i := 0; i < tablesCount; i++ {
		tableName := fmt.Sprintf("tbl%d", i)
		tableSchema := filepath.Join(dir, tableName+".schema")
		err = ioutil.WriteFile(tableSchema, []byte(fmt.Sprintf("CREATE TABLE %s(i TINYINT);\n", tableName)), 0644)
		c.Assert(err, IsNil)
		tableData := filepath.Join(dir, tableName+".sql")
		err = ioutil.WriteFile(tableData, []byte(fmt.Sprintf("INSERT INTO %s VALUES (1);\n", tableName)), 0644)
		c.Assert(err, IsNil)
		tables[tableName] = &mydump.MDTableMeta{
			DB:         "tsr",
			Name:       tableName,
			SchemaFile: tableSchema,
			DataFiles:  []string{tableData},
		}
	}

	db := map[string]*mydump.MDDatabaseMeta{
		"tsr": &mydump.MDDatabaseMeta{
			Tables: tables,
		},
	}
	return dir, db
}

// Creates the app configuration for the current test.
func createAppConfig(serverAddr string, concurrency int) *config.Config {
	cfg := config.NewConfig()
	cfg.TikvImporter.Addr = serverAddr
	cfg.App.TableConcurrency = concurrency
	cfg.App.CheckRequirements = false
	// TODO Get rid of the TiDB test dependency!
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.Port = 3306
	cfg.TiDB.User = "root"
	cfg.TiDB.StatusPort = 10080
	return cfg
}

func verifyWriteSuccess(c *C, events <-chan mockKVEvent) {
	timer := time.After(tablesCount * importDelay)

	var (
		eventsCount         = [3]int{0, 0, 0}
		eventsCountExpected = [3]int{tablesCount, tablesCount, tablesCount}
	)

	looping := true
	for looping && eventsCount != eventsCountExpected {
		select {
		case <-timer:
			looping = false
		case e := <-events:
			eventsCount[e]++
		}
	}
	c.Assert(eventsCount, Equals, eventsCountExpected)
}

func (s *testRestoreSuite) TestSimpleRestore(c *C) {
	ctx := context.Background()

	server, addr, events := runMockKVServer(c, 4, func() error {
		panic("table concurrency is violated!")
	})
	defer server.Stop()

	dir, db := createTestDB(c)
	defer os.RemoveAll(dir)

	cfg := createAppConfig(addr, 4)

	rc := restore.NewRestoreControlloer(ctx, db, cfg)
	defer rc.Close()

	rc.Run(ctx)

	verifyWriteSuccess(c, events)
}
