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

package kv

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
)

const (
	maxRetryTimes    int = 3 // tikv-importer has done retry internally. so we don't retry many times.
	retryBackoffTime     = time.Second * 3
)

/*

Usual workflow:

1. Create an `Importer` for the whole process.

2. For each table,

	i. Split into multiple "batches" consisting of data files with roughly equal total size.

	ii. For each batch,

		a. Create an `OpenedEngine` via `importer.OpenEngine()`

		b. For each chunk,

			i. Create a `WriteStream` via `engine.NewWriteStream()`
			ii. Deliver data into the stream via `stream.Put()`
			iii. Close the stream via `stream.Close()`

		c. When all chunks are written, obtain a `ClosedEngine` via `engine.CloseEngine()`

		d. Import data via `engine.Import()`

		e. Cleanup via `engine.Cleanup()`

3. Close the connection via `importer.Close()`

*/

// Importer represents a gRPC connection to tikv-importer. This type is
// goroutine safe: you can share this instance and execute any method anywhere.
type Importer struct {
	conn   *grpc.ClientConn
	cli    kv.ImportKVClient
	pdAddr string
}

// NewImporter creates a new connection to tikv-importer. A single connection
// per tidb-lightning instance is enough.
func NewImporter(ctx context.Context, importServerAddr string, pdAddr string) (*Importer, error) {
	conn, err := grpc.DialContext(ctx, importServerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Importer{
		conn:   conn,
		cli:    kv.NewImportKVClient(conn),
		pdAddr: pdAddr,
	}, nil
}

// Close the importer connection.
func (importer *Importer) Close() {
	importer.conn.Close()
}

// SwitchMode switches the TiKV cluster to another operation mode.
func (importer *Importer) SwitchMode(ctx context.Context, mode sst.SwitchMode) error {
	req := &kv.SwitchModeRequest{
		PdAddr: importer.pdAddr,
		Request: &sst.SwitchModeRequest{
			Mode: mode,
		},
	}

	task := log.With(zap.Stringer("mode", mode)).Begin(zap.DebugLevel, "switch mode")
	_, err := importer.cli.SwitchMode(ctx, req)
	task.End(zap.WarnLevel, err)
	return errors.Trace(err)
}

// Compact the target cluster for better performance.
func (importer *Importer) Compact(ctx context.Context, level int32) error {
	req := &kv.CompactClusterRequest{
		PdAddr: importer.pdAddr,
		Request: &sst.CompactRequest{
			// No need to set Range here.
			OutputLevel: level,
		},
	}

	task := log.With(zap.Int32("level", level)).Begin(zap.InfoLevel, "compact cluster")
	_, err := importer.cli.CompactCluster(ctx, req)
	task.End(zap.ErrorLevel, err)
	return errors.Trace(err)
}

// OpenedEngine is an opened importer engine file, allowing data to be written
// to it via WriteStream instances.
type OpenedEngine struct {
	importer *Importer
	logger   log.Logger
	uuid     uuid.UUID
	ts       uint64
}

// isIgnorableOpenCloseEngineError checks if the error from
// OpenEngine/CloseEngine can be safely ignored.
func isIgnorableOpenCloseEngineError(err error) bool {
	// We allow "FileExists" error. This happens when the engine has been opened
	// and closed before. This error typically arise when resuming from a
	// checkpoint with a partially-imported engine.
	//
	// If the error is legit in a no-checkpoints settings, the later WriteEngine
	// API will bail us out to keep us safe.
	return err == nil || strings.Contains(err.Error(), "FileExists")
}

func makeTag(tableName string, engineID int32) string {
	return fmt.Sprintf("%s:%d", tableName, engineID)
}

func makeLogger(tag string, engineUUID uuid.UUID) log.Logger {
	return log.With(
		zap.String("engineTag", tag),
		zap.Stringer("engineUUID", engineUUID),
	)
}

var engineNamespace = uuid.Must(uuid.FromString("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf"))

// OpenEngine opens an engine with the given table name and engine ID. This type
// is goroutine safe: you can share this instance and execute any method anywhere.
func (importer *Importer) OpenEngine(
	ctx context.Context,
	tableName string,
	engineID int32,
) (*OpenedEngine, error) {
	tag := makeTag(tableName, engineID)
	engineUUID := uuid.NewV5(engineNamespace, tag)
	req := &kv.OpenEngineRequest{
		Uuid: engineUUID.Bytes(),
	}
	_, err := importer.cli.OpenEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return nil, errors.Trace(err)
	}

	openCounter := metric.ImporterEngineCounter.WithLabelValues("open")
	openCounter.Inc()

	logger := makeLogger(tag, engineUUID)
	logger.Info("open engine")

	// gofail: var FailIfEngineCountExceeds int
	// {
	// 	closedCounter := metric.ImporterEngineCounter.WithLabelValues("closed")
	// 	openCount := metric.ReadCounter(openCounter)
	// 	closedCount := metric.ReadCounter(closedCounter)
	// 	if openCount - closedCount > float64(FailIfEngineCountExceeds) {
	// 		panic(fmt.Sprintf("forcing failure due to FailIfEngineCountExceeds: %v - %v >= %d", openCount, closedCount, FailIfEngineCountExceeds))
	// 	}
	// }
	// goto RETURN

	// gofail: RETURN:

	return &OpenedEngine{
		importer: importer,
		logger:   logger,
		ts:       uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
		uuid:     engineUUID,
	}, nil
}

// WriteStream is a single write stream into an opened engine. This type is
// **NOT** goroutine safe, all operations must be executed in the same
// goroutine.
type WriteStream struct {
	engine  *OpenedEngine
	wstream kv.ImportKV_WriteEngineClient
}

// NewWriteStream creates a new write engine associated with
func (engine *OpenedEngine) NewWriteStream(ctx context.Context) (*WriteStream, error) {
	wstream, err := engine.importer.cli.WriteEngine(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Bind uuid for this write request
	req := &kv.WriteEngineRequest{
		Chunk: &kv.WriteEngineRequest_Head{
			Head: &kv.WriteHead{
				Uuid: engine.uuid.Bytes(),
			},
		},
	}
	if err = wstream.Send(req); err != nil {
		if _, closeErr := wstream.CloseAndRecv(); closeErr != nil {
			// just log the close error, we need to propagate the send error instead
			engine.logger.Warn("close write stream failed", log.ShortError(closeErr))
		}
		return nil, errors.Trace(err)
	}

	return &WriteStream{
		engine:  engine,
		wstream: wstream,
	}, nil
}

// Put delivers some KV pairs to importer via this write stream.
func (stream *WriteStream) Put(kvs []kvec.KvPair) error {
	// Send kv paris as write request content
	mutations := make([]*kv.Mutation, len(kvs))
	for i, pair := range kvs {
		mutations[i] = &kv.Mutation{
			Op:    kv.Mutation_Put,
			Key:   pair.Key,
			Value: pair.Val,
		}
	}

	req := &kv.WriteEngineRequest{
		Chunk: &kv.WriteEngineRequest_Batch{
			Batch: &kv.WriteBatch{
				CommitTs:  stream.engine.ts,
				Mutations: mutations,
			},
		},
	}

	var sendErr error
	for i := 0; i < maxRetryTimes; i++ {
		sendErr = stream.wstream.Send(req)
		if !common.IsRetryableError(sendErr) {
			break
		}
		stream.engine.logger.Error("send write stream failed", log.ShortError(sendErr))
		time.Sleep(retryBackoffTime)
	}
	return errors.Trace(sendErr)
}

// Close the write stream.
func (stream *WriteStream) Close() error {
	if _, err := stream.wstream.CloseAndRecv(); err != nil {
		stream.engine.logger.Error("close write stream failed", log.ShortError(err))
		return errors.Trace(err)
	}
	return nil
}

// ClosedEngine is a closed importer engine file, allowing ingestion into TiKV.
// This type is goroutine safe: you can share this instance and execute any
// method anywhere.
type ClosedEngine struct {
	importer *Importer
	logger   log.Logger
	uuid     uuid.UUID
}

// Close the opened engine to prepare it for importing. This method will return
// error if any associated WriteStream is still not closed.
func (engine *OpenedEngine) Close(ctx context.Context) (*ClosedEngine, error) {
	closedEngine, err := engine.importer.rawUnsafeCloseEngine(ctx, engine.logger, engine.uuid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metric.ImporterEngineCounter.WithLabelValues("closed").Inc()
	return closedEngine, nil
}

// UnsafeCloseEngine closes the engine without first opening it. This method is
// "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (importer *Importer) UnsafeCloseEngine(ctx context.Context, tableName string, engineID int32) (*ClosedEngine, error) {
	tag := makeTag(tableName, engineID)
	engineUUID := uuid.NewV5(engineNamespace, tag)
	return importer.UnsafeCloseEngineWithUUID(ctx, tag, engineUUID)
}

// UnsafeCloseEngineWithUUID closes the engine using the UUID alone.
func (importer *Importer) UnsafeCloseEngineWithUUID(ctx context.Context, tag string, engineUUID uuid.UUID) (*ClosedEngine, error) {
	logger := makeLogger(tag, engineUUID)
	return importer.rawUnsafeCloseEngine(ctx, logger, engineUUID)
}

func (importer *Importer) rawUnsafeCloseEngine(ctx context.Context, logger log.Logger, engineUUID uuid.UUID) (*ClosedEngine, error) {
	req := &kv.CloseEngineRequest{
		Uuid: engineUUID.Bytes(),
	}
	task := logger.Begin(zap.InfoLevel, "engine close")
	_, err := importer.cli.CloseEngine(ctx, req)
	if isIgnorableOpenCloseEngineError(err) {
		err = nil
	}
	task.End(zap.ErrorLevel, err)

	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ClosedEngine{
		importer: importer,
		logger:   logger,
		uuid:     engineUUID,
	}, nil
}

// Import the data into the TiKV cluster via SST ingestion.
func (engine *ClosedEngine) Import(ctx context.Context) error {
	var err error

	for i := 0; i < maxRetryTimes; i++ {
		req := &kv.ImportEngineRequest{
			Uuid:   engine.uuid.Bytes(),
			PdAddr: engine.importer.pdAddr,
		}

		task := engine.logger.With(zap.Int("retryCnt", i)).Begin(zap.InfoLevel, "import")
		_, err = engine.importer.cli.ImportEngine(ctx, req)
		if !common.IsRetryableError(err) {
			task.End(zap.ErrorLevel, err)
			return errors.Trace(err)
		}
		task.Warn("import spuriously failed, going to retry again", log.ShortError(err))
		time.Sleep(retryBackoffTime)
	}

	return errors.Annotatef(err, "[%s] import reach max retry %d and still failed", engine.uuid, maxRetryTimes)
}

// Cleanup deletes the imported data from importer.
func (engine *ClosedEngine) Cleanup(ctx context.Context) error {
	req := &kv.CleanupEngineRequest{
		Uuid: engine.uuid.Bytes(),
	}
	task := engine.logger.Begin(zap.InfoLevel, "cleanup")
	_, err := engine.importer.cli.CleanupEngine(ctx, req)
	task.End(zap.WarnLevel, err)
	return errors.Trace(err)
}
