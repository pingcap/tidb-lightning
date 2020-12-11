// Copyright 2020 PingCAP, Inc.
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
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"

	"github.com/pingcap/tidb-lightning/lightning/common"
)

type noopBackend struct {
	tls             *common.TLS
	regionSplitSize int64
}

func NewNoopBackend(tls *common.TLS, regionSplitSize int64) Backend {
	return MakeBackend(&noopBackend{
		tls:             tls,
		regionSplitSize: regionSplitSize,
	})
}

// Close the connection to the backend.
func (b *noopBackend) Close() {
}

// MakeEmptyRows creates an empty collection of encoded rows.
func (b *noopBackend) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

// RetryImportDelay returns the duration to sleep when retrying an import
func (b *noopBackend) RetryImportDelay() time.Duration {
	return 0
}

// MaxChunkSize returns the maximum size acceptable by the backend. The
// value will be used in `Rows.SplitIntoChunks`.
func (b *noopBackend) MaxChunkSize() int {
	// a batch size write to leveldb
	return int(b.regionSplitSize)
}

// ShouldPostProcess returns whether KV-specific post-processing should be
// performed for this backend. Post-processing includes checksum and analyze.
func (b *noopBackend) ShouldPostProcess() bool {
	return false
}

// NewEncoder creates an encoder of a TiDB table.
func (b *noopBackend) NewEncoder(tbl table.Table, options *SessionOptions) (Encoder, error) {
	return NewTableKVEncoder(tbl, options)
}

func (b *noopBackend) OpenEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (b *noopBackend) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	columnNames []string,
	commitTS uint64,
	rows Rows,
) error {
	return nil
}

func (b *noopBackend) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (b *noopBackend) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (b *noopBackend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

// CheckRequirements performs the check whether the backend satisfies the
// version requirements
func (b *noopBackend) CheckRequirements(ctx context.Context) error {
	return nil
}

// FetchRemoteTableModels obtains the models of all tables given the schema
// name. The returned table info does not need to be precise if the encoder,
// is not requiring them, but must at least fill in the following fields for
// TablesFromMeta to succeed:
//  - Name
//  - State (must be model.StatePublic)
//  - ID
//  - Columns
//     * Name
//     * State (must be model.StatePublic)
//     * Offset (must be 0, 1, 2, ...)
//  - PKIsHandle (true = do not generate _tidb_rowid)
func (b *noopBackend) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return fetchRemoteTableModelsFromTLS(ctx, b.tls, schemaName)
}
