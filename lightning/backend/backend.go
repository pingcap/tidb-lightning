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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/verification"
)

const (
	maxRetryTimes = 3 // tikv-importer has done retry internally. so we don't retry many times.
)

/*

Usual workflow:

1. Create a `Backend` for the whole process.

2. For each table,

	i. Split into multiple "batches" consisting of data files with roughly equal total size.

	ii. For each batch,

		a. Create an `OpenedEngine` via `backend.OpenEngine()`

		b. For each chunk, deliver data into the engine via `engine.WriteRows()`

		c. When all chunks are written, obtain a `ClosedEngine` via `engine.Close()`

		d. Import data via `engine.Import()`

		e. Cleanup via `engine.Cleanup()`

3. Close the connection via `backend.Close()`

*/

func makeTag(tableName string, engineID int32) string {
	return fmt.Sprintf("%s:%d", tableName, engineID)
}

func makeLogger(tag string, engineUUID uuid.UUID) log.Logger {
	return log.With(
		zap.String("engineTag", tag),
		zap.Stringer("engineUUID", engineUUID),
	)
}

func MakeUUID(tableName string, engineID int32) (string, uuid.UUID) {
	tag := makeTag(tableName, engineID)
	engineUUID := uuid.NewV5(engineNamespace, tag)
	return tag, engineUUID
}

var engineNamespace = uuid.Must(uuid.FromString("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf"))

// AbstractBackend is the abstract interface behind Backend.
// Implementations of this interface must be goroutine safe: you can share an
// instance and execute any method anywhere.
type AbstractBackend interface {
	// Close the connection to the backend.
	Close()

	// Flush ensure the written data is saved successfully, to make sure no data lose after restart
	Flush(engineId uuid.UUID) error

	// MakeEmptyRows creates an empty collection of encoded rows.
	MakeEmptyRows() Rows

	// RetryImportDelay returns the duration to sleep when retrying an import
	RetryImportDelay() time.Duration

	// MaxChunkSize returns the maximum size acceptable by the backend. The
	// value will be used in `Rows.SplitIntoChunks`.
	MaxChunkSize() int

	// ShouldPostProcess returns whether KV-specific post-processing should be
	// performed for this backend. Post-processing includes checksum, adjusting
	// auto-increment ID, and analyze.
	ShouldPostProcess() bool

	// NewEncoder creates an encoder of a TiDB table.
	NewEncoder(tbl table.Table, options *SessionOptions) Encoder

	OpenEngine(ctx context.Context, engineUUID uuid.UUID) error

	WriteRows(
		ctx context.Context,
		engineUUID uuid.UUID,
		tableName string,
		columnNames []string,
		commitTS uint64,
		rows Rows,
	) error

	CloseEngine(ctx context.Context, engineUUID uuid.UUID) error

	ImportEngine(ctx context.Context, engineUUID uuid.UUID) error

	CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error

	// CheckRequirements performs the check whether the backend satisfies the
	// version requirements
	CheckRequirements() error

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
	FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error)
}

func fetchRemoteTableModelsFromTLS(tls *common.TLS, schema string) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	err := tls.GetJSON("/schema/"+schema, &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read schema '%s' from remote", schema)
	}
	return tables, nil
}

// Backend is the delivery target for Lightning
type Backend struct {
	abstract AbstractBackend
}

type engine struct {
	backend AbstractBackend
	logger  log.Logger
	uuid    uuid.UUID
}

// OpenedEngine is an opened engine, allowing data to be written via WriteRows.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type OpenedEngine struct {
	engine
	tableName string
	ts        uint64
}

// // import_ the data written to the engine into the target.
// import_(ctx context.Context) error

// // cleanup deletes the imported data.
// cleanup(ctx context.Context) error

// ClosedEngine represents a closed engine, allowing ingestion into the target.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type ClosedEngine struct {
	engine
}

func MakeBackend(ab AbstractBackend) Backend {
	return Backend{abstract: ab}
}

func (be Backend) Close() {
	be.abstract.Close()
}

func (be Backend) MakeEmptyRows() Rows {
	return be.abstract.MakeEmptyRows()
}

func (be Backend) NewEncoder(tbl table.Table, options *SessionOptions) Encoder {
	return be.abstract.NewEncoder(tbl, options)
}

func (be Backend) ShouldPostProcess() bool {
	return be.abstract.ShouldPostProcess()
}

func (be Backend) CheckRequirements() error {
	return be.abstract.CheckRequirements()
}

func (be Backend) FetchRemoteTableModels(schemaName string) ([]*model.TableInfo, error) {
	return be.abstract.FetchRemoteTableModels(schemaName)
}

// OpenEngine opens an engine with the given table name and engine ID.
func (be Backend) OpenEngine(ctx context.Context, tableName string, engineID int32) (*OpenedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, engineID)
	logger := makeLogger(tag, engineUUID)

	if err := be.abstract.OpenEngine(ctx, engineUUID); err != nil {
		return nil, err
	}

	openCounter := metric.ImporterEngineCounter.WithLabelValues("open")
	openCounter.Inc()

	logger.Info("open engine")

	failpoint.Inject("FailIfEngineCountExceeds", func(val failpoint.Value) {
		closedCounter := metric.ImporterEngineCounter.WithLabelValues("closed")
		openCount := metric.ReadCounter(openCounter)
		closedCount := metric.ReadCounter(closedCounter)
		if injectValue := val.(int); openCount-closedCount > float64(injectValue) {
			panic(fmt.Sprintf("forcing failure due to FailIfEngineCountExceeds: %v - %v >= %d", openCount, closedCount, injectValue))
		}
	})

	return &OpenedEngine{
		engine: engine{
			backend: be.abstract,
			logger:  logger,
			uuid:    engineUUID,
		},
		tableName: tableName,
		ts:        uint64(time.Now().Unix()), // TODO ... set outside ? from pd ?
	}, nil
}

// Close the opened engine to prepare it for importing.
func (engine *OpenedEngine) Close(ctx context.Context) (*ClosedEngine, error) {
	closedEngine, err := engine.unsafeClose(ctx)
	if err == nil {
		metric.ImporterEngineCounter.WithLabelValues("closed").Inc()
	}
	return closedEngine, err
}

// Flush current written data
func (engine *OpenedEngine) Flush() error {
	return engine.backend.Flush(engine.uuid)
}

// WriteRows writes a collection of encoded rows into the engine.
func (engine *OpenedEngine) WriteRows(ctx context.Context, columnNames []string, rows Rows) error {
	var err error

outside:
	for _, r := range rows.SplitIntoChunks(engine.backend.MaxChunkSize()) {
		for i := 0; i < maxRetryTimes; i++ {
			err = engine.backend.WriteRows(ctx, engine.uuid, engine.tableName, columnNames, engine.ts, r)
			switch {
			case err == nil:
				continue outside
			case common.IsRetryableError(err):
				// retry next loop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] write rows reach max retry %d and still failed", engine.tableName, maxRetryTimes)
	}

	return nil
}

// UnsafeCloseEngine closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be Backend) UnsafeCloseEngine(ctx context.Context, tableName string, engineID int32) (*ClosedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, engineID)
	return be.UnsafeCloseEngineWithUUID(ctx, tag, engineUUID)
}

// UnsafeCloseEngineWithUUID closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be Backend) UnsafeCloseEngineWithUUID(ctx context.Context, tag string, engineUUID uuid.UUID) (*ClosedEngine, error) {
	return engine{
		backend: be.abstract,
		logger:  makeLogger(tag, engineUUID),
		uuid:    engineUUID,
	}.unsafeClose(ctx)
}

func (en engine) unsafeClose(ctx context.Context) (*ClosedEngine, error) {
	task := en.logger.Begin(zap.InfoLevel, "engine close")
	err := en.backend.CloseEngine(ctx, en.uuid)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		return nil, err
	}
	return &ClosedEngine{engine: en}, nil
}

// Import the data written to the engine into the target.
func (engine *ClosedEngine) Import(ctx context.Context) error {
	var err error

	for i := 0; i < maxRetryTimes; i++ {
		task := engine.logger.With(zap.Int("retryCnt", i)).Begin(zap.InfoLevel, "import")
		err = engine.backend.ImportEngine(ctx, engine.uuid)
		if !common.IsRetryableError(err) {
			task.End(zap.ErrorLevel, err)
			return err
		}
		task.Warn("import spuriously failed, going to retry again", log.ShortError(err))
		time.Sleep(engine.backend.RetryImportDelay())
	}

	return errors.Annotatef(err, "[%s] import reach max retry %d and still failed", engine.uuid, maxRetryTimes)
}

// Cleanup deletes the intermediate data from target.
func (engine *ClosedEngine) Cleanup(ctx context.Context) error {
	task := engine.logger.Begin(zap.InfoLevel, "cleanup")
	err := engine.backend.CleanupEngine(ctx, engine.uuid)
	task.End(zap.WarnLevel, err)
	return err
}

func (engine *ClosedEngine) Logger() log.Logger {
	return engine.logger
}

// Encoder encodes a row of SQL values into some opaque type which can be
// consumed by OpenEngine.WriteEncoded.
type Encoder interface {
	// Close the encoder.
	Close()

	// Encode encodes a row of SQL values into a backend-friendly format.
	Encode(
		logger log.Logger,
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, error)
}

// Row represents a single encoded row.
type Row interface {
	// ClassifyAndAppend separates the data-like and index-like parts of the
	// encoded row, and appends these parts into the existing buffers and
	// checksums.
	ClassifyAndAppend(
		data *Rows,
		dataChecksum *verification.KVChecksum,
		indices *Rows,
		indexChecksum *verification.KVChecksum,
	)
}

// Rows represents a collection of encoded rows.
type Rows interface {
	// SplitIntoChunks splits the rows into multiple consecutive parts, each
	// part having total byte size less than `splitSize`. The meaning of "byte
	// size" should be consistent with the value used in `Row.ClassifyAndAppend`.
	SplitIntoChunks(splitSize int) []Rows

	// Clear returns a new collection with empty content. It may share the
	// capacity with the current instance. The typical usage is `x = x.Clear()`.
	Clear() Rows
}
