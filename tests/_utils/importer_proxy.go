package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
)

type logEntry struct {
	TS    time.Duration
	ID    int
	Phase string
	RPC   string
	Error error `json:",omitempty"`
	Args  []interface{}
}

func formatUUID(u []byte) string {
	if u == nil {
		return ""
	}

	buf := make([]byte, 36)

	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])

	return string(buf)
}

type proxyService struct {
	startTime time.Time
	client    kv.ImportKVClient
	lock      sync.Mutex
	logs      []logEntry
	id        int

	importDelayMs int64
}

func (s *proxyService) logStart(rpc string, args ...interface{}) int {
	s.lock.Lock()
	defer s.lock.Unlock()
	id := s.id
	s.id++
	s.logs = append(s.logs, logEntry{
		TS:    time.Since(s.startTime),
		ID:    id,
		Phase: "S",
		RPC:   rpc,
		Error: nil,
		Args:  args,
	})
	return id
}

func (s *proxyService) logEnd(rpc string, id int, err error, args ...interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.logs = append(s.logs, logEntry{
		TS:    time.Since(s.startTime),
		ID:    id,
		Phase: "E",
		RPC:   rpc,
		Error: err,
		Args:  args,
	})
}

func (s *proxyService) SwitchMode(ctx context.Context, req *kv.SwitchModeRequest) (*kv.SwitchModeResponse, error) {
	id := s.logStart("SwitchMode", req.PdAddr, req.Request.Mode.String())
	resp, err := s.client.SwitchMode(ctx, req)
	s.logEnd("SwitchMode", id, err)
	return resp, err
}

func (s *proxyService) OpenEngine(ctx context.Context, req *kv.OpenEngineRequest) (*kv.OpenEngineResponse, error) {
	id := s.logStart("OpenEngine", formatUUID(req.Uuid))
	resp, err := s.client.OpenEngine(ctx, req)
	s.logEnd("OpenEngine", id, err)
	return resp, err
}

func (s *proxyService) CloseEngine(ctx context.Context, req *kv.CloseEngineRequest) (*kv.CloseEngineResponse, error) {
	id := s.logStart("CloseEngine", formatUUID(req.Uuid))
	resp, err := s.client.CloseEngine(ctx, req)
	s.logEnd("CloseEngine", id, err, formatUUID(resp.GetError().GetEngineNotFound().GetUuid()))
	return resp, err
}

func (s *proxyService) ImportEngine(ctx context.Context, req *kv.ImportEngineRequest) (*kv.ImportEngineResponse, error) {
	id := s.logStart("ImportEngine", formatUUID(req.Uuid))
	time.Sleep(time.Duration(s.importDelayMs) * time.Millisecond)
	resp, err := s.client.ImportEngine(ctx, req)
	s.logEnd("ImportEngine", id, err)
	return resp, err
}

func (s *proxyService) CleanupEngine(ctx context.Context, req *kv.CleanupEngineRequest) (*kv.CleanupEngineResponse, error) {
	id := s.logStart("CleanupEngine", formatUUID(req.Uuid))
	resp, err := s.client.CleanupEngine(ctx, req)
	s.logEnd("CleanupEngine", id, err)
	return resp, err
}

func (s *proxyService) CompactCluster(ctx context.Context, req *kv.CompactClusterRequest) (*kv.CompactClusterResponse, error) {
	id := s.logStart("CompactCluster", req.PdAddr, req.Request.OutputLevel)
	resp, err := s.client.CompactCluster(ctx, req)
	s.logEnd("CompactCluster", id, err)
	return resp, err
}

func (s *proxyService) WriteEngine(receiver kv.ImportKV_WriteEngineServer) error {
	streamID := s.logStart("WriteEngine")
	ctx := context.Background()
	sender, streamErr := s.client.WriteEngine(ctx)
	defer s.logEnd("WriteEngine", streamID, streamErr)

	if streamErr != nil {
		return streamErr
	}

	for {
		req, err := receiver.Recv()

		switch err {
		case nil:
			msg := []interface{}{streamID}
			switch chunk := req.Chunk.(type) {
			case *kv.WriteEngineRequest_Head:
				msg = append(msg,
					"HEAD",
					formatUUID(chunk.Head.Uuid),
				)
			case *kv.WriteEngineRequest_Batch:
				kvSize := 0
				for _, mutation := range chunk.Batch.Mutations {
					kvSize += len(mutation.Key) + len(mutation.Value)
				}
				msg = append(msg,
					"BATCH",
					chunk.Batch.CommitTs,
					len(chunk.Batch.Mutations),
					kvSize,
				)
			default:
				msg = append(msg, "UNKNOWN")
			}

			id := s.logStart("WriteEngine/Send", msg...)
			streamErr = sender.Send(req)
			s.logEnd("WriteEngine/Send", id, streamErr, streamID)
			if streamErr != nil {
				return streamErr
			}

		case io.EOF:
			id := s.logStart("WriteEngine/CloseAndRecv", streamID)
			resp, err := sender.CloseAndRecv()
			s.logEnd("WriteEngine/CloseAndRecv", id, err, streamID, formatUUID(resp.GetError().GetEngineNotFound().GetUuid()))
			if err != nil {
				streamErr = err
				return err
			}
			streamErr = receiver.SendAndClose(resp)
			return streamErr

		default:
			// should really write `sender.finish(streamErr)` but that method is private
			sender.SendMsg(streamErr)
			sender.CloseSend()
			streamErr = err
			return err
		}
	}
}

func run() error {
	var controlServer http.Server
	var service proxyService

	var listenAddress, connectAddress string

	flag.StringVar(&listenAddress, "listen", "127.0.0.1:20170", "Address to listen to external connection")
	flag.StringVar(&connectAddress, "connect", "127.0.0.1:8808", "Address of the upstream importer server to connect to")
	flag.StringVar(&controlServer.Addr, "control", "127.0.0.1:40250", "Address to control this proxy")
	flag.Int64Var(&service.importDelayMs, "import-delay", 0, "Delay the ImportEngine operation by this amount (milliseconds)")
	flag.Parse()

	ctx := context.Background()
	shutdownCh := make(chan struct{})

	// Connect to upstream
	clientConn, err := grpc.DialContext(ctx, connectAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer clientConn.Close()

	// Start gRPC server
	server := grpc.NewServer()
	service.startTime = time.Now()
	service.client = kv.NewImportKVClient(clientConn)
	kv.RegisterImportKVServer(server, &service)
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	go server.Serve(listener)
	defer server.GracefulStop()

	// Start control server
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/shutdown", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		shutdownCh <- struct{}{}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/v1/dump", func(w http.ResponseWriter, req *http.Request) {
		service.lock.Lock()
		logs := service.logs
		service.lock.Unlock()
		// the above will create a snapshot of the logs array. since the log is append-only,
		// and the GC will keep reference valid concurrently, we are sure the array content here is
		// consistent even outside of the lock.

		since, err := strconv.ParseInt(req.URL.Query().Get("since"), 10, 64)
		if err != nil {
			since = 0
		}

		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		for _, log := range logs {
			if log.TS > time.Duration(since) {
				encoder.Encode(log)
			}
		}
	})
	controlServer.Handler = mux
	go controlServer.ListenAndServe()
	defer controlServer.Shutdown(ctx)

	// Wait for the end
	fmt.Fprintf(os.Stderr, "importer_proxy has started. Please `POST http://%s/api/v1/shutdown` to quit\n", controlServer.Addr)
	<-shutdownCh
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Failed with", err)
		os.Exit(1)
	}
}
