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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	kvec "github.com/pingcap/tidb/util/kvencoder"
	binlog "github.com/pingcap/tipb/go-binlog"
)

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kvPairs []kvec.KvPair
}

func (t *transaction) Get(k kv.Key) ([]byte, error) {
	panic("unexpected Get() call")
}

func (t *transaction) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	panic("unexpected Iter() call")
}

func (t *transaction) IterReverse(k kv.Key) (kv.Iterator, error) {
	panic("unexpected IterReverse() call")
}

func (t *transaction) Set(k kv.Key, v []byte) error {
	t.kvPairs = append(t.kvPairs, kvec.KvPair{
		Key: k.Clone(),
		Val: append([]byte{}, v...),
	})
	return nil
}

func (t *transaction) Delete(k kv.Key) error {
	panic("unexpected Delete() call")
}

func (t *transaction) Size() int {
	panic("unexpected Size() call")
}

func (t *transaction) Len() int {
	panic("unexpected Len() call")
}

func (t *transaction) Reset() {
	panic("unexpected Reset() call")
}

func (t *transaction) SetCap(cap int) {
	panic("unexpected SetCap() call")
}

func (t *transaction) Commit(context.Context) error {
	panic("unexpected Commit() call")
}

func (t *transaction) Rollback() error {
	panic("unexpected Rollback() call")
}

func (t *transaction) String() string {
	panic("unexpected String() call")
}

func (t *transaction) LockKeys(keys ...kv.Key) error {
	panic("unexpected LockKeys() call")
}

func (t *transaction) SetOption(opt kv.Option, val interface{}) {}

func (t *transaction) DelOption(kv.Option) {}

func (t *transaction) IsReadOnly() bool {
	panic("unexpected IsReadOnly() call")
}

func (t *transaction) StartTS() uint64 {
	panic("unexpected StartTS() call")
}

func (t *transaction) Valid() bool {
	panic("unexpected Valid() call")
}

func (t *transaction) GetMemBuffer() kv.MemBuffer {
	panic("unexpected GetMemBuffer() call")
}

func (t *transaction) SetVars(vars *kv.Variables) {
	panic("unexpected SetVars() call")
}

func (t *transaction) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	panic("unexpected BatchGet() call")
}

func (t *transaction) GetSnapshot() kv.Snapshot {
	panic("unexpected GetSnapshot() call")
}

//------------------------------------------------------------------------------

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type session struct {
	txn  transaction
	vars *variable.SessionVars
}

func newSession(sqlMode mysql.SQLMode) *session {
	vars := variable.NewSessionVars()
	vars.LightningMode = true
	vars.SkipUTF8Check = true
	vars.StmtCtx.InInsertStmt = true
	vars.StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode()
	vars.StmtCtx.TimeZone = vars.Location()
	return &session{
		txn:  transaction{},
		vars: vars,
	}
}

func (se *session) takeKvPairs() []kvec.KvPair {
	pairs := se.txn.kvPairs
	se.txn.kvPairs = make([]kvec.KvPair, 0, len(pairs))
	return pairs
}

func (se *session) NewTxn() error {
	panic("unexpected NewTxn() call")
}

func (se *session) Txn(active bool) (kv.Transaction, error) {
	return &se.txn, nil
}

func (se *session) GetClient() kv.Client {
	panic("unexpected GetClient() call")
}

func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	panic("unexpected SetValue() call")
}

func (se *session) Value(key fmt.Stringer) interface{} {
	panic("unexpected Value() call")
}

func (se *session) ClearValue(key fmt.Stringer) {
	panic("unexpected ClearValue() call")
}

func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

func (se *session) GetSessionManager() util.SessionManager {
	panic("unexpected GetSessionManager() call")
}

func (se *session) RefreshTxnCtx(context.Context) error {
	panic("unexpected RefreshTxnCtx() call")
}

func (se *session) InitTxnWithStartTS(startTS uint64) error {
	panic("unexpected InitTxnWithStartTS() call")
}

func (se *session) GetStore() kv.Storage {
	panic("unexpected GetStore() call")
}

func (se *session) PreparedPlanCache() *kvcache.SimpleLRUCache {
	panic("unexpected PreparedPlanCache() call")
}

func (se *session) StoreQueryFeedback(feedback interface{}) {
	panic("unexpected StoreQueryFeedback() call")
}

func (se *session) StmtCommit() error {
	panic("unexpected StmtCommit() call")
}

func (se *session) StmtRollback() {
	panic("unexpected StmtRollback() call")
}

func (se *session) StmtGetMutation(int64) *binlog.TableMutation {
	panic("unexpected StmtGetMutation() call")
}

func (se *session) StmtAddDirtyTableOP(op int, physicalID int64, handle int64, row []types.Datum) {
	panic("unexpected StmtAddDirtyTableOP() call")
}

func (se *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	panic("unexpected DDLOwnerChecker() call")
}
