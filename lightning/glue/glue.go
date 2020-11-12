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
	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
)

type Glue interface {
	OwnsSQLExecutor() bool
	GetSQLExecutor() SQLExecutor
	GetParser() *parser.Parser
	GetTables(context.Context, string) ([]*model.TableInfo, error)
	OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.CheckpointsDB, error)
	Record(string, uint64)
}

type SQLExecutor interface {
	ExecuteWithLog(ctx context.Context, query string, purpose string, logger *zap.Logger) error
	ObtainStringWithLog(ctx context.Context, query string, purpose string, logger *zap.Logger) (string, error)
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

func (e ExternalTiDBGlue) GetSQLExecutor() SQLExecutor {
	return e
}

func (e ExternalTiDBGlue) ExecuteWithLog(ctx context.Context, query string, purpose string, logger *zap.Logger) error {
	sql := common.SQLWithRetry{
		DB:     e.db,
		Logger: log.Logger{Logger: logger},
	}
	return sql.Exec(ctx, purpose, query)
}

func (e ExternalTiDBGlue) ObtainStringWithLog(ctx context.Context, query string, purpose string, logger *zap.Logger) (string, error) {
	var s string
	err := common.SQLWithRetry{
		DB:     e.db,
		Logger: log.Logger{Logger: logger},
	}.QueryRow(ctx, purpose, query, &s)
	return s, err
}

func (e ExternalTiDBGlue) GetParser() *parser.Parser {
	return e.parser
}

func (e ExternalTiDBGlue) GetDB() *sql.DB {
	return e.db
}

func (e ExternalTiDBGlue) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

func (e ExternalTiDBGlue) OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (checkpoints.CheckpointsDB, error) {
	return checkpoints.OpenCheckpointsDB(ctx, cfg)
}

func (e ExternalTiDBGlue) OwnsSQLExecutor() bool {
	return true
}

func (e ExternalTiDBGlue) Close() {
	e.db.Close()
}

func (e ExternalTiDBGlue) Record(string, uint64) {
}

type ImportStage uint64

const (
	StageWriting ImportStage = iota
	StagePostProcessing
)

func (s ImportStage) String() string {
	switch s {
	case StageWriting:
		return "Writing"
	case StagePostProcessing:
		return "PostProcessing"
	}
	return ""
}
