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

package glue

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

type Glue interface {
	OwnsSQLExecutor() bool
	GetSQLExecutor() SQLExecutor
	GetDB() (*sql.DB, error)
	GetParser() *parser.Parser
	GetTables(context.Context, string) ([]*model.TableInfo, error)
	GetSession() (checkpoints.Session, error)
	OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.CheckpointsDB, error)
	// Record is used to report some information (key, value) to host TiDB, including progress, stage currently
	Record(string, uint64)
}

type SQLExecutor interface {
	// ExecuteWithLog and ObtainStringWithLog should support concurrently call and can't assure different calls goes to
	// same underlying connection
	ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error
	ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error)
	Close()
}

type dbConnector struct {
	dbConf   config.DBStore
	lazyConn func(dbConf config.DBStore) (*sql.DB, error)
	conn     *sql.DB
	err      error
}

func (connector *dbConnector) connect() error {
	if connector.conn == nil {
		connector.conn, connector.err = connector.lazyConn(connector.dbConf)
	}
	return connector.err
}

func (connector *dbConnector) close() error {
	if connector.conn != nil {
		return connector.conn.Close()
	}
	return nil
}

func (connector *dbConnector) getError() error {
	return connector.err
}

func (connector *dbConnector) getDB() *sql.DB {
	return connector.conn
}

// DBPool is multi-connection implementation of `SQLExecutor` interface
// It will pick one of any idle connections to handle DB events
// and put it back when events done
type DBPool struct {
	SQLExecutor
	singleton *dbConnector
	pool      sync.Pool
}

func (dbPool *DBPool) getConn() *dbConnector {
	conn := dbPool.pool.Get()
	if conn != nil {
		return conn.(*dbConnector)
	}
	return dbPool.singleton
}

func (dbPool *DBPool) putConn(conn *dbConnector) *DBPool {
	dbPool.pool.Put(conn)
	return dbPool
}

// NewDBPool is constructor of DBPool
func NewDBPool(size int, dbConf config.DBStore, lazyConn func(dbConf config.DBStore) (*sql.DB, error)) (*DBPool, error) {
	// TODO: `size` validation
	// NOTE: make sure we have one db connection at least
	singleton, err := lazyConn(dbConf)
	if err != nil {
		return nil, err
	}
	dbPool := &DBPool{}
	dbPool.singleton = &dbConnector{
		dbConf:   dbConf,
		lazyConn: lazyConn,
		conn:     singleton,
		err:      nil,
	}
	dbPool.putConn(dbPool.singleton)
	for i := 0; i < size-1; i++ {
		dbPool.putConn(&dbConnector{
			dbConf:   dbConf,
			lazyConn: lazyConn,
			conn:     nil,
			err:      nil,
		})
	}
	return dbPool, nil
}

// ExecuteWithLog implement `SQLExecutor` interface
func (dbPool *DBPool) ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error {
	conn := dbPool.getConn()
	defer func() {
		dbPool.putConn(conn)
	}()
	if err := conn.connect(); err != nil {
		return err
	}
	sql := common.SQLWithRetry{
		DB:     conn.getDB(),
		Logger: logger,
	}
	return sql.Exec(ctx, purpose, query)
}

// ObtainStringWithLog implement `SQLExecutor` interface
func (dbPool *DBPool) ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error) {
	conn := dbPool.getConn()
	defer func() {
		dbPool.putConn(conn)
	}()
	var s string
	var err error
	if err = conn.connect(); err == nil {
		err = common.SQLWithRetry{
			DB:     conn.getDB(),
			Logger: logger,
		}.QueryRow(ctx, purpose, query, &s)
	}
	return s, err
}

// Close implement `SQLExecutor` interface
func (dbPool *DBPool) Close() {
	for conn := dbPool.getConn(); conn != nil; conn = dbPool.getConn() {
		if conn.getError() == nil {
			// CONFUSED: why we ignore error occurs via db connection close issue?
			_ = conn.close()
		}
	}
}

type ExternalTiDBGlue struct {
	db     *sql.DB
	parser *parser.Parser
}

func NewExternalTiDBGlue(db *sql.DB, sqlMode mysql.SQLMode) *ExternalTiDBGlue {
	p := parser.New()
	p.SetSQLMode(sqlMode)

	return &ExternalTiDBGlue{db: db, parser: p}
}

func (e *ExternalTiDBGlue) GetSQLExecutor() SQLExecutor {
	return e
}

func (e *ExternalTiDBGlue) ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error {
	sql := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}
	return sql.Exec(ctx, purpose, query)
}

func (e *ExternalTiDBGlue) ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error) {
	var s string
	err := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}.QueryRow(ctx, purpose, query, &s)
	return s, err
}

func (e *ExternalTiDBGlue) GetDB() (*sql.DB, error) {
	return e.db, nil
}

func (e *ExternalTiDBGlue) GetParser() *parser.Parser {
	return e.parser
}

func (e ExternalTiDBGlue) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, errors.New("ExternalTiDBGlue doesn't have a valid GetTables function")
}

func (e ExternalTiDBGlue) GetSession() (checkpoints.Session, error) {
	return nil, errors.New("ExternalTiDBGlue doesn't have a valid GetSession function")
}

func (e *ExternalTiDBGlue) OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (checkpoints.CheckpointsDB, error) {
	return checkpoints.OpenCheckpointsDB(ctx, cfg)
}

func (e *ExternalTiDBGlue) OwnsSQLExecutor() bool {
	return true
}

func (e *ExternalTiDBGlue) Close() {
	e.db.Close()
}

func (e *ExternalTiDBGlue) Record(string, uint64) {
}

const (
	RecordEstimatedChunk = "EstimatedChunk"
	RecordFinishedChunk  = "FinishedChunk"
)
