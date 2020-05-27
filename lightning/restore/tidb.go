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

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	. "github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"go.uber.org/zap"
)

type TiDBManager struct {
	db     *sql.DB
	parser *parser.Parser
}

func NewTiDBManager(dsn config.DBStore, tls *common.TLS) (*TiDBManager, error) {
	param := common.MySQLConnectParam{
		Host:             dsn.Host,
		Port:             dsn.Port,
		User:             dsn.User,
		Password:         dsn.Psw,
		SQLMode:          dsn.StrSQLMode,
		MaxAllowedPacket: dsn.MaxAllowedPacket,
		TLS:              dsn.TLS,
		Vars: map[string]string{
			"tidb_build_stats_concurrency":       strconv.Itoa(dsn.BuildStatsConcurrency),
			"tidb_distsql_scan_concurrency":      strconv.Itoa(dsn.DistSQLScanConcurrency),
			"tidb_index_serial_scan_concurrency": strconv.Itoa(dsn.IndexSerialScanConcurrency),
			"tidb_checksum_table_concurrency":    strconv.Itoa(dsn.ChecksumTableConcurrency),
		},
	}
	db, err := param.Connect()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewTiDBManagerWithDB(db, dsn.SQLMode), nil
}

// NewTiDBManagerWithDB creates a new TiDB manager with an existing database
// connection.
func NewTiDBManagerWithDB(db *sql.DB, sqlMode mysql.SQLMode) *TiDBManager {
	parser := parser.New()
	parser.SetSQLMode(sqlMode)

	return &TiDBManager{
		db:     db,
		parser: parser,
	}
}

func (timgr *TiDBManager) Close() {
	timgr.db.Close()
}

func (timgr *TiDBManager) InitSchema(ctx context.Context, database string, tablesSchema map[string]string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("db", database)),
	}

	var createDatabase strings.Builder
	createDatabase.WriteString("CREATE DATABASE IF NOT EXISTS ")
	common.WriteMySQLIdentifier(&createDatabase, database)
	err := sql.Exec(ctx, "create database", createDatabase.String())
	if err != nil {
		return errors.Trace(err)
	}
	var useDB strings.Builder
	useDB.WriteString("USE ")
	common.WriteMySQLIdentifier(&useDB, database)
	err = sql.Exec(ctx, "use database", useDB.String())
	if err != nil {
		return errors.Trace(err)
	}

	task := sql.Logger.Begin(zap.InfoLevel, "create tables")
	for tbl, sqlCreateTable := range tablesSchema {
		task.Debug("create table", zap.String("schema", sqlCreateTable))

		sqlCreateTable, err = timgr.createTableIfNotExistsStmt(sqlCreateTable, tbl)
		if err != nil {
			break
		}
		sql2 := common.SQLWithRetry{
			DB:           timgr.db,
			Logger:       sql.Logger.With(zap.String("table", common.UniqueTable(database, tbl))),
			HideQueryLog: true,
		}
		err = sql2.Exec(ctx, "create table", sqlCreateTable)
		if err != nil {
			break
		}
	}
	task.End(zap.ErrorLevel, err)

	return errors.Trace(err)
}

func (timgr *TiDBManager) createTableIfNotExistsStmt(createTable, tblName string) (string, error) {
	stmts, _, err := timgr.parser.Parse(createTable, "", "")
	if err != nil {
		return "", err
	}

	var res strings.Builder
	res.Grow(len(createTable))
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &res)

	for _, stmt := range stmts {
		if createTableNode, ok := stmt.(*ast.CreateTableStmt); ok {
			createTableNode.Table.Schema = model.NewCIStr("")
			createTableNode.Table.Name = model.NewCIStr(tblName)
			createTableNode.IfNotExists = true
		}
		if err := stmt.Restore(ctx); err != nil {
			return "", err
		}
		ctx.WritePlain(";")
	}

	return res.String(), nil
}

func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	return sql.Exec(ctx, "drop table", "DROP TABLE "+tableName)
}

func (timgr *TiDBManager) LoadSchemaInfo(
	ctx context.Context,
	schemas []*mydump.MDDatabaseMeta,
	getTables func(string) ([]*model.TableInfo, error),
) (map[string]*TidbDBInfo, error) {
	result := make(map[string]*TidbDBInfo, len(schemas))
	for _, schema := range schemas {
		tables, err := getTables(schema.Name)
		if err != nil {
			return nil, err
		}

		dbInfo := &TidbDBInfo{
			Name:   schema.Name,
			Tables: make(map[string]*TidbTableInfo),
		}

		for _, tbl := range tables {
			tableName := tbl.Name.String()
			if tbl.State != model.StatePublic {
				err := errors.Errorf("table [%s.%s] state is not public", schema.Name, tableName)
				metric.RecordTableCount(metric.TableStatePending, err)
				return nil, err
			}
			metric.RecordTableCount(metric.TableStatePending, err)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableInfo := &TidbTableInfo{
				ID:   tbl.ID,
				Name: tableName,
				Core: tbl,
			}
			dbInfo.Tables[tableName] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

func ObtainGCLifeTime(ctx context.Context, db *sql.DB) (string, error) {
	var gcLifeTime string
	err := common.SQLWithRetry{DB: db, Logger: log.L()}.QueryRow(ctx, "obtain GC lifetime",
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		&gcLifeTime,
	)
	return gcLifeTime, err
}

func UpdateGCLifeTime(ctx context.Context, db *sql.DB, gcLifeTime string) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.With(zap.String("gcLifeTime", gcLifeTime)),
	}
	return sql.Exec(ctx, "update GC lifetime",
		"UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		gcLifeTime,
	)
}

func ObtainRowFormatVersion(ctx context.Context, db *sql.DB) (rowFormatVersion string) {
	err := common.SQLWithRetry{DB: db, Logger: log.L()}.QueryRow(ctx, "obtain row format version",
		"SELECT @@tidb_row_format_version",
		&rowFormatVersion,
	)
	if err != nil {
		rowFormatVersion = "1"
	}
	return
}

func AlterAutoIncrement(ctx context.Context, db *sql.DB, tableName string, incr int64) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.With(zap.String("table", tableName), zap.Int64("auto_increment", incr)),
	}
	query := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%d", tableName, incr)
	task := sql.Logger.Begin(zap.InfoLevel, "alter table auto_increment")
	err := sql.Exec(ctx, "alter table auto_increment", query)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_increment failed, please perform the query manually (this is needed no matter the table has an auto-increment column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}

func AlterAutoRandom(ctx context.Context, db *sql.DB, tableName string, randomBase int64) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.With(zap.String("table", tableName), zap.Int64("auto_random", randomBase)),
	}
	query := fmt.Sprintf("ALTER TABLE %s AUTO_RANDOM_BASE=%d", tableName, randomBase)
	task := sql.Logger.Begin(zap.InfoLevel, "alter table auto_random")
	err := sql.Exec(ctx, "alter table auto_random_base", query)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_random_base failed, please perform the query manually (this is needed no matter the table has an auto-random column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}
