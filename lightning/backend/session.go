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
	"bytes"
	"context"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/kv/memdb"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"

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

type BufferStore struct {
	MemBuffer *memDbBuffer
}

// memDbBuffer implements the MemBuffer interface.
type memDbBuffer struct {
	sandbox         *memdb.Sandbox
	entrySizeLimit  int
	bufferLenLimit  uint64
	bufferSizeLimit uint64
}

type memDbIter struct {
	iter    memdb.Iterator
	start   []byte
	end     []byte
	reverse bool
}

// Next implements the Iterator Next.
func (i *memDbIter) Next() error {
	if i.reverse {
		i.iter.Prev()
	} else {
		i.iter.Next()
	}
	return nil
}

// Valid implements the Iterator Valid.
func (i *memDbIter) Valid() bool {
	if !i.reverse {
		return i.iter.Valid() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *memDbIter) Key() kv.Key {
	return i.iter.Key()
}

// Value implements the Iterator Value.
func (i *memDbIter) Value() []byte {
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *memDbIter) Close() {

}

// NewMemDbBuffer creates a new memDbBuffer.
func NewMemDbBuffer() *memDbBuffer {
	return &memDbBuffer{
		sandbox:         memdb.NewSandbox(),
		entrySizeLimit:  kv.TxnEntrySizeLimit,
		bufferSizeLimit: atomic.LoadUint64(&kv.TxnTotalSizeLimit),
	}
}

// Iter creates an Iterator.
func (m *memDbBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	i := &memDbIter{
		iter:    m.sandbox.NewIterator(),
		start:   k,
		end:     upperBound,
		reverse: false,
	}

	if k == nil {
		i.iter.SeekToFirst()
	} else {
		i.iter.Seek(k)
	}
	return i, nil
}

func (m *memDbBuffer) IterReverse(k kv.Key) (kv.Iterator, error) {
	return nil, nil
}

// Get returns the value associated with key.
func (m *memDbBuffer) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	return nil, nil
}

// Set associates key with value.
func (m *memDbBuffer) Set(k kv.Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(kv.ErrCannotSetNilValue)
	}
	if len(k)+len(v) > m.entrySizeLimit {
		return kv.ErrEntryTooLarge.GenWithStackByArgs(m.entrySizeLimit, len(k)+len(v))
	}

	m.sandbox.Put(k, v)
	if m.Size() > int(m.bufferSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(atomic.LoadUint64(&kv.TxnTotalSizeLimit))
	}
	return nil
}

// Delete removes the entry from buffer with provided key.
func (m *memDbBuffer) Delete(k kv.Key) error {
	return nil
}

// Size returns sum of keys and values length.
func (m *memDbBuffer) Size() int {
	return m.sandbox.Size()
}

// Len returns the number of entries in the DB.
func (m *memDbBuffer) Len() int {
	return m.sandbox.Len()
}

func (m *memDbBuffer) NewStagingBuffer() kv.MemBuffer {
	return &memDbBuffer{
		sandbox:         m.sandbox.Derive(),
		entrySizeLimit:  kv.TxnEntrySizeLimit,
		bufferSizeLimit: m.bufferSizeLimit - uint64(m.sandbox.Size()),
	}
}

func (m *memDbBuffer) Flush() (int, error) {
	// There is no need to check size limit,
	// because the size limit is maintain when derive this buffer.
	return m.sandbox.Flush(), nil
}

// Discard must do nothing because it will discard every time when AddRecord finished
// It's hard cord in TiDB after this PR(https://github.com/pingcap/tidb/pull/15931/files) merged.
func (m *memDbBuffer) Discard() {
	// Do nothing
}

// RealDiscard will truly discard all staging kvs, after lightning translate all kvs into kvpairs
// It will trigger manually.
func (m *memDbBuffer) RealDiscard() {
	m.sandbox.Discard()
}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf *memDbBuffer, f func(k kv.Key, v []byte) error) error {
	iter, err := memBuf.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for iter.Valid() {
		if err = f(iter.Key(), iter.Value()); err != nil {
			return errors.Trace(err)
		}
		err = iter.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kv.Transaction
	kvPairs     []common.KvPair
	bufferStore *memDbBuffer
}

func (t *transaction) NewStagingBuffer() kv.MemBuffer {
	if t.bufferStore == nil {
		t.bufferStore = NewMemDbBuffer()
	}
	return t.bufferStore
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
	bs := se.txn.bufferStore
	pairs := make([]common.KvPair, 0, bs.Size())
	err := WalkMemBuffer(bs, func(k kv.Key, v []byte) error {
		pairs = append(pairs, common.KvPair{
			Key: k.Clone(),
			Val: append([]byte{}, v...),
		})
		return nil
	})
	if err != nil {
		log.Fatal("failed to take kv pairs from buffer store:", zap.Error(err))
	}
	bs.RealDiscard()
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
