package kv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"google.golang.org/grpc"

	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	kvec "github.com/pingcap/tidb/util/kvencoder"
)

const (
	maxRetryTimes    int = 3 // tikv-importer has done retry internally. so we don't retry many times.
	retryBackoffTime     = time.Second * 3
)

/*

Usual workflow:

1. Create an `Importer` for the whole process.

2. For each table,

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
	timer := time.Now()

	_, err := importer.cli.SwitchMode(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "status: Unimplemented") {
			fmt.Fprintln(os.Stderr, "Error: The TiKV instance does not support mode switching. Please make sure the TiKV version is 2.0.4 or above.")
		}
		return errors.Trace(err)
	}

	common.AppLogger.Infof("switch to tikv %s mode takes %v", mode, time.Since(timer))
	return nil
}

// Compact the target cluster for better performance.
func (importer *Importer) Compact(ctx context.Context, level int32) error {
	common.AppLogger.Infof("compact level %d", level)

	req := &kv.CompactClusterRequest{
		PdAddr: importer.pdAddr,
		Request: &sst.CompactRequest{
			// No need to set Range here.
			OutputLevel: level,
		},
	}
	timer := time.Now()
	_, err := importer.cli.CompactCluster(ctx, req)
	common.AppLogger.Infof("compact level %d takes %v", level, time.Since(timer))

	return errors.Trace(err)
}

// OpenedEngine is an opened importer engine file, allowing data to be written
// to it via WriteStream instances.
type OpenedEngine struct {
	importer  *Importer
	tableName string
	uuid      uuid.UUID
	ts        uint64
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

// OpenEngine opens an engine with the given UUID. This type is goroutine safe:
// you can share this instance and execute any method anywhere.
func (importer *Importer) OpenEngine(
	ctx context.Context,
	tableName string,
	engineUUID uuid.UUID,
) (*OpenedEngine, error) {
	req := &kv.OpenEngineRequest{
		Uuid: engineUUID.Bytes(),
	}
	_, err := importer.cli.OpenEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return nil, errors.Trace(err)
	}

	openCounter := metric.EngineCounter.WithLabelValues("open")
	openCounter.Inc()
	common.AppLogger.Infof("[%s] open engine %s", tableName, engineUUID)

	// gofail: var FailIfEngineCountExceeds int
	// {
	// 	closedCounter := metric.EngineCounter.WithLabelValues("closed")
	// 	openCount := metric.ReadCounter(openCounter)
	// 	closedCount := metric.ReadCounter(closedCounter)
	// 	if openCount - closedCount > float64(FailIfEngineCountExceeds) {
	// 		panic(fmt.Sprintf("forcing failure due to FailIfEngineCountExceeds: %v - %v >= %d", openCount, closedCount, FailIfEngineCountExceeds))
	// 	}
	// }
	// goto RETURN

	// gofail: RETURN:

	return &OpenedEngine{
		importer:  importer,
		tableName: tableName,
		ts:        uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
		uuid:      engineUUID,
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
			common.AppLogger.Warnf("[%s] close write stream cause failed : %v", engine.tableName, closeErr)
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
		common.AppLogger.Errorf("[%s] write stream failed to send: %s", stream.engine.tableName, sendErr.Error())
		time.Sleep(retryBackoffTime)
	}
	return errors.Trace(sendErr)
}

// Close the write stream.
func (stream *WriteStream) Close() error {
	if _, err := stream.wstream.CloseAndRecv(); err != nil {
		if !common.IsContextCanceledError(err) {
			common.AppLogger.Errorf("[%s] close write stream cause failed : %v", stream.engine.tableName, err)
		}
		return errors.Trace(err)
	}
	return nil
}

// ClosedEngine is a closed importer engine file, allowing ingestion into TiKV.
// This type is goroutine safe: you can share this instance and execute any
// method anywhere.
type ClosedEngine struct {
	importer  *Importer
	tableName string
	uuid      uuid.UUID
}

// Close the opened engine to prepare it for importing. This method will return
// error if any associated WriteStream is still not closed.
func (engine *OpenedEngine) Close(ctx context.Context) (*ClosedEngine, error) {
	common.AppLogger.Infof("[%s] [%s] engine close", engine.tableName, engine.uuid)
	timer := time.Now()
	closedEngine, err := engine.importer.UnsafeCloseEngine(ctx, engine.tableName, engine.uuid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	common.AppLogger.Infof("[%s] [%s] engine close takes %v", engine.tableName, engine.uuid, time.Since(timer))
	metric.EngineCounter.WithLabelValues("closed").Inc()
	return closedEngine, nil
}

// UnsafeCloseEngine closes the engine without first opening it. This method is
// "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (importer *Importer) UnsafeCloseEngine(ctx context.Context, tableName string, engineUUID uuid.UUID) (*ClosedEngine, error) {
	req := &kv.CloseEngineRequest{
		Uuid: engineUUID.Bytes(),
	}
	_, err := importer.cli.CloseEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return nil, errors.Trace(err)
	}

	return &ClosedEngine{
		importer:  importer,
		tableName: tableName,
		uuid:      engineUUID,
	}, nil
}

// Import the data into the TiKV cluster via SST ingestion.
func (engine *ClosedEngine) Import(ctx context.Context) error {
	var err error

	for i := 0; i < maxRetryTimes; i++ {
		common.AppLogger.Infof("[%s] [%s] import", engine.tableName, engine.uuid)
		req := &kv.ImportEngineRequest{
			Uuid:   engine.uuid.Bytes(),
			PdAddr: engine.importer.pdAddr,
		}
		timer := time.Now()
		_, err = engine.importer.cli.ImportEngine(ctx, req)
		if !common.IsRetryableError(err) {
			common.AppLogger.Infof("[%s] [%s] import takes %v", engine.tableName, engine.uuid, time.Since(timer))
			return errors.Trace(err)
		}
		common.AppLogger.Warnf("[%s] [%s] import failed and retry %d time, err %v", engine.tableName, engine.uuid, i+1, err)
		time.Sleep(retryBackoffTime)
	}

	return errors.Annotatef(err, "[%s] [%s] import reach max retry %d and still failed", engine.tableName, engine.uuid, maxRetryTimes)
}

// Cleanup deletes the imported data from importer.
func (engine *ClosedEngine) Cleanup(ctx context.Context) error {
	common.AppLogger.Infof("[%s] [%s] cleanup ", engine.tableName, engine.uuid)
	req := &kv.CleanupEngineRequest{
		Uuid: engine.uuid.Bytes(),
	}
	timer := time.Now()
	_, err := engine.importer.cli.CleanupEngine(ctx, req)
	common.AppLogger.Infof("[%s] [%s] cleanup takes %v", engine.tableName, engine.uuid, time.Since(timer))
	return errors.Trace(err)
}
