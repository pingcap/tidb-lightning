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

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
)

// TODO: rename it to SQLExecutor, and add a glue to get SQLExecutor, Owns? createTableIfNotExistsStmt
type Glue interface {
	Execute(ctx context.Context, query string) error
	ExecuteWithLogArgs(ctx context.Context, query string, purpose string, fields ...zap.Field) error
	ObtainString(ctx context.Context, query string) (string, error)
	ObtainStringLogArgs(ctx context.Context, query string, purpose string, fields ...zap.Field) (string, error)

	GetParser() *parser.Parser
	GetTables(context.Context, string) ([]*model.TableInfo, error)
	OwnsSQLExecutor() bool
}

type externalTiDBGlue struct {
	db 	   *sql.DB
	parser *parser.Parser
}

func NewExternalTiDBGlue(db *sql.DB, sqlMode mysql.SQLMode) *externalTiDBGlue {
	p := parser.New()
	p.SetSQLMode(sqlMode)

	return &externalTiDBGlue{db: db, parser: p}
}

func (e externalTiDBGlue) Execute(ctx context.Context, sql string) error {
	panic("implement me")
}

func (e externalTiDBGlue) ExecuteWithLogArgs(ctx context.Context, query string, purpose string, fields ...zap.Field) error {
	sql := common.SQLWithRetry{
		DB:     e.db,
		Logger: log.With(fields...),
	}
	return sql.Exec(ctx, purpose, query)
}

func (e externalTiDBGlue) ObtainString(ctx context.Context, sql string) (string, error) {
	panic("implement me")
}

func (e externalTiDBGlue) ObtainStringLogArgs(ctx context.Context, query string, purpose string, fields ...zap.Field) (string, error) {
	var s string
	err := common.SQLWithRetry{
		DB:     e.db,
		Logger: log.With(fields...),
	}.QueryRow(ctx, purpose, query, &s)
	return s, err
}

func (e externalTiDBGlue) GetParser() *parser.Parser {
	return e.parser
}

func (e externalTiDBGlue) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

func (e externalTiDBGlue) OwnsSQLExecutor() bool {
	return true
}