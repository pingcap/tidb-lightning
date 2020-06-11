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
	"strconv"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
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
	sessionctx.Context
	txn  transaction
	vars *variable.SessionVars
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
		txn:  transaction{},
		vars: vars,
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

// StmtAddDirtyTableOP implements the sessionctx.Context interface
func (se *session) StmtAddDirtyTableOP(op int, physicalID int64, handle kv.Handle) {}
