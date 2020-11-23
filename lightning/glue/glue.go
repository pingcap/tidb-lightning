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
