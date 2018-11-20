package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
)

type sinkService struct{}

func (s *sinkService) SwitchMode(context.Context, *kv.SwitchModeRequest) (*kv.SwitchModeResponse, error) {
	return &kv.SwitchModeResponse{}, nil
}

func (s *sinkService) OpenEngine(context.Context, *kv.OpenEngineRequest) (*kv.OpenEngineResponse, error) {
	return &kv.OpenEngineResponse{}, nil
}

func (s *sinkService) CloseEngine(context.Context, *kv.CloseEngineRequest) (*kv.CloseEngineResponse, error) {
	return &kv.CloseEngineResponse{}, nil
}

func (s *sinkService) ImportEngine(context.Context, *kv.ImportEngineRequest) (*kv.ImportEngineResponse, error) {
	return &kv.ImportEngineResponse{}, nil
}

func (s *sinkService) CleanupEngine(context.Context, *kv.CleanupEngineRequest) (*kv.CleanupEngineResponse, error) {
	return &kv.CleanupEngineResponse{}, nil
}

func (s *sinkService) CompactCluster(context.Context, *kv.CompactClusterRequest) (*kv.CompactClusterResponse, error) {
	return &kv.CompactClusterResponse{}, nil
}

var counter int64
var pairs int64

func (s *sinkService) WriteEngine(receiver kv.ImportKV_WriteEngineServer) error {
	for {
		req, err := receiver.Recv()
		switch err {
		case nil:
			if batch := req.GetBatch(); batch != nil {
				atomic.AddInt64(&counter, 1)
				atomic.AddInt64(&pairs, int64(len(batch.GetMutations())))
			}
		case io.EOF:
			return receiver.SendAndClose(&kv.WriteEngineResponse{})
		default:
			return err
		}
	}
}

func run() error {
	var service sinkService

	var listenAddress string

	flag.StringVar(&listenAddress, "listen", "127.0.0.1:20170", "Address to listen to external connection")
	flag.Parse()

	go func() {
		for range time.Tick(time.Second) {
			fmt.Println("[", time.Now().Format(time.RFC3339), "]", atomic.LoadInt64(&counter), ":: Received", atomic.LoadInt64(&pairs), "pairs")
		}
	}()

	// Start gRPC server
	server := grpc.NewServer()
	kv.RegisterImportKVServer(server, &service)
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	server.Serve(listener)
	server.GracefulStop()

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Failed with", err)
		os.Exit(1)
	}
}
