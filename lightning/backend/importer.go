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

package backend

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	defaultRetryBackoffTime = time.Second * 3
)

// importer represents a gRPC connection to tikv-importer. This type is
// goroutine safe: you can share this instance and execute any method anywhere.
type importer struct {
	conn   *grpc.ClientConn
	cli    kv.ImportKVClient
	pdAddr string
}

// NewImporter creates a new connection to tikv-importer. A single connection
// per tidb-lightning instance is enough.
func NewImporter(ctx context.Context, importServerAddr string, pdAddr string) (Backend, error) {
	conn, err := grpc.DialContext(ctx, importServerAddr, grpc.WithInsecure())
	if err != nil {
		return MakeBackend(nil), errors.Trace(err)
	}

	return MakeBackend(&importer{
		conn:   conn,
		cli:    kv.NewImportKVClient(conn),
		pdAddr: pdAddr,
	}), nil
}

// NewMockImporter creates an *unconnected* importer based on a custom
// ImportKVClient. This is provided for testing only. Do not use this function
// outside of tests.
func NewMockImporter(cli kv.ImportKVClient, pdAddr string) Backend {
	return MakeBackend(&importer{
		conn:   nil,
		cli:    cli,
		pdAddr: pdAddr,
	})
}

// Close the importer connection.
func (importer *importer) Close() {
	if importer.conn != nil {
		if err := importer.conn.Close(); err != nil {
			log.L().Warn("close importer gRPC connection failed", zap.Error(err))
		}
	}
}

func (*importer) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (*importer) MaxChunkSize() int {
	// 31 MB. hardcoded by importer, so do we
	return 31 << 10
}

func (*importer) ShouldPostProcess() bool {
	return true
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

func (importer *importer) OpenEngine(ctx context.Context, engineUUID uuid.UUID, keyPrefix []byte) error {
	req := &kv.OpenEngineRequest{
		Uuid: engineUUID.Bytes(),
		KeyPrefix: keyPrefix,
	}

	_, err := importer.cli.OpenEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CloseEngineRequest{
		Uuid: engineUUID.Bytes(),
	}

	_, err := importer.cli.CloseEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.ImportEngineRequest{
		Uuid:   engineUUID.Bytes(),
		PdAddr: importer.pdAddr,
	}

	_, err := importer.cli.ImportEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CleanupEngineRequest{
		Uuid: engineUUID.Bytes(),
	}

	_, err := importer.cli.CleanupEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	columnNames []string,
	ts uint64,
	rows Rows,
) (finalErr error) {
	kvs := rows.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	wstream, err := importer.cli.WriteEngine(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	logger := log.With(zap.Stringer("engineUUID", engineUUID))

	defer func() {
		if _, closeErr := wstream.CloseAndRecv(); closeErr != nil {
			if finalErr == nil {
				finalErr = errors.Trace(closeErr)
			} else {
				// just log the close error, we need to propagate the earlier error instead
				logger.Warn("close write stream failed", log.ShortError(closeErr))
			}
		}
	}()

	// Bind uuid for this write request
	req := &kv.WriteEngineRequest{
		Chunk: &kv.WriteEngineRequest_Head{
			Head: &kv.WriteHead{
				Uuid: engineUUID.Bytes(),
			},
		},
	}
	if err := wstream.Send(req); err != nil {
		return errors.Trace(err)
	}

	// Send kv paris as write request content
	mutations := make([]*kv.Mutation, len(kvs))
	for i, pair := range kvs {
		mutations[i] = &kv.Mutation{
			Op:    kv.Mutation_Put,
			Key:   pair.Key,
			Value: pair.Val,
		}
	}

	req.Reset()
	req.Chunk = &kv.WriteEngineRequest_Batch{
		Batch: &kv.WriteBatch{
			CommitTs:  ts,
			Mutations: mutations,
		},
	}

	if err := wstream.Send(req); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (*importer) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (*importer) NewEncoder(tbl table.Table, sqlMode mysql.SQLMode) Encoder {
	return NewTableKVEncoder(tbl, sqlMode)
}
