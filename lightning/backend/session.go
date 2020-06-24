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
	"strconv"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-binlog"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"

	"github.com/pingcap/tidb-lightning/lightning/common"
)

// invalidIterator is a trimmed down Iterator type which is invalid.
type invalidIterator struct {
	kv.Iterator
}

// Valid implements the kv.Iterator interface
func (*invalidIterator) Valid() bool {
	return false
}

// Close implements the kv.Iterator interface
func (*invalidIterator) Close() {
}

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kv.Transaction
	kvPairs []common.KvPair
}

func (t *transaction) NewStagingBuffer() kv.MemBuffer {
	return t
}

func (t *transaction) Discard() {
	// do nothing
}

func (t *transaction) Flush() (int, error) {
	// do nothing
	return 0, nil
}

// Reset implements the kv.MemBuffer interface
func (t *transaction) Reset() {}

// Get implements the kv.Retriever interface
func (t *transaction) Get(ctx context.Context, key kv.Key) ([]byte, error) {
	return nil, kv.ErrNotExist
}

// Iter implements the kv.Retriever interface
func (t *transaction) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return &invalidIterator{}, nil
}

// Set implements the kv.Mutator interface
func (t *transaction) Set(k kv.Key, v []byte) error {
	t.kvPairs = append(t.kvPairs, common.KvPair{
		Key: k.Clone(),
		Val: append([]byte{}, v...),
	})
	return nil
}

// SetOption implements the kv.Transaction interface
func (t *transaction) SetOption(opt kv.Option, val interface{}) {}

// DelOption implements the kv.Transaction interface
func (t *transaction) DelOption(kv.Option) {}

// SetAssertion implements the kv.Transaction interface
func (t *transaction) SetAssertion(kv.Key, kv.AssertionType) {}

// session is a trimmed down Session type which only wraps our own trimmed-down
// transaction type and provides the session variables to the TiDB library
// optimized for Lightning.
type session struct {
	txn  transaction
	vars *variable.SessionVars
	// currently, we only set `CommonAddRecordCtx`
	values map[fmt.Stringer]interface{}
}

// SessionOptions is the initial configuration of the session.
type SessionOptions struct {
	SQLMode          mysql.SQLMode
	Timestamp        int64
	RowFormatVersion string
}

func newSession(options *SessionOptions) *session {
	sqlMode := options.SQLMode
	vars := variable.NewSessionVars()
	vars.SkipUTF8Check = true
	vars.StmtCtx.InInsertStmt = true
	vars.StmtCtx.BatchCheck = true
	vars.StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	vars.StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	vars.StmtCtx.TimeZone = vars.Location()
	vars.SetSystemVar("timestamp", strconv.FormatInt(options.Timestamp, 10))
	vars.SetSystemVar(variable.TiDBRowFormatVersion, options.RowFormatVersion)
	vars.TxnCtx = nil

	s := &session{
		txn:    transaction{},
		vars:   vars,
		values: make(map[fmt.Stringer]interface{}, 1),
	}

	return s
}

func (se *session) takeKvPairs() []common.KvPair {
	pairs := se.txn.kvPairs
	se.txn.kvPairs = make([]common.KvPair, 0, len(pairs))
	return pairs
}

// Txn implements the sessionctx.Context interface
func (se *session) Txn(active bool) (kv.Transaction, error) {
	return &se.txn, nil
}

// GetSessionVars implements the sessionctx.Context interface
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

// SetValue saves a value associated with this context for key.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.values[key] = value
}

// Value returns the value associated with this context for key.
func (se *session) Value(key fmt.Stringer) interface{} {
	return se.values[key]
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface
func (se *session) StmtAddDirtyTableOP(op int, physicalID int64, handle kv.Handle) {}

// NewTxn creates a new transaction for further execution.
// If old transaction is valid, it is committed first.
// It's used in BEGIN statement and DDL statements to commit old transaction.
func (se *session) NewTxn(context.Context) error {
	return nil
}

// GetClient gets a kv.Client.
func (se *session) GetClient() kv.Client {
	return nil
}

// ClearValue clears the value associated with this context for key.
func (se *session) ClearValue(key fmt.Stringer) {
	delete(se.values, key)
}

func (se *session) GetSessionManager() util.SessionManager {
	return nil
}

// RefreshTxnCtx commits old transaction without retry,
// and creates a new transaction.
// now just for load data and batch insert.
func (se *session) RefreshTxnCtx(context.Context) error {
	return nil
}

// InitTxnWithStartTS initializes a transaction with startTS.
// It should be called right before we builds an executor.
func (se *session) InitTxnWithStartTS(startTS uint64) error {
	return nil
}

// GetStore returns the store of session.
func (se *session) GetStore() kv.Storage {
	return nil
}

// PreparedPlanCache returns the cache of the physical plan
func (se *session) PreparedPlanCache() *kvcache.SimpleLRUCache {
	return nil
}

// StoreQueryFeedback stores the query feedback.
func (se *session) StoreQueryFeedback(feedback interface{}) {
}

// HasDirtyContent checks whether there's dirty update on the given table.
func (se *session) HasDirtyContent(tid int64) bool {
	return false
}

// StmtCommit flush all changes by the statement to the underlying transaction.
func (se *session) StmtCommit(tracker *memory.Tracker) error {
	return nil
}

// StmtRollback provides statement level rollback.
func (se *session) StmtRollback() {
}

// StmtGetMutation gets the binlog mutation for current statement.
func (se *session) StmtGetMutation(int64) *binlog.TableMutation {
	return nil
}

// DDLOwnerChecker returns owner.DDLOwnerChecker.
func (se *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	return nil
}

// AddTableLock adds table lock to the session lock map.
func (se *session) AddTableLock([]model.TableLockTpInfo) {}

// ReleaseTableLocks releases table locks in the session lock map.
func (se *session) ReleaseTableLocks(locks []model.TableLockTpInfo) {}

// ReleaseTableLockByTableID releases table locks in the session lock map by table ID.
func (se *session) ReleaseTableLockByTableIDs(tableIDs []int64) {}

// CheckTableLocked checks the table lock.
func (se *session) CheckTableLocked(tblID int64) (bool, model.TableLockType) {
	return false, model.TableLockNone
}

// GetAllTableLocks gets all table locks table id and db id hold by the session.
func (se *session) GetAllTableLocks() []model.TableLockTpInfo {
	return []model.TableLockTpInfo{}
}

// ReleaseAllTableLocks releases all table locks hold by the session.
func (se *session) ReleaseAllTableLocks() {}

// HasLockedTables uses to check whether this session locked any tables.
func (se *session) HasLockedTables() bool {
	return false
}

// PrepareTSFuture uses to prepare timestamp by future.
func (se *session) PrepareTSFuture(ctx context.Context) {}
