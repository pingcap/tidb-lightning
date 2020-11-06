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

	tmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-lightning/lightning/glue"

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

// getSQLErrCode returns error code if err is a mysql error
func getSQLErrCode(err error) (terror.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*tmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return terror.ErrCode(mysqlErr.Number), true
}

func isUnknownSystemVariableErr(err error) bool {
	code, ok := getSQLErrCode(err)
	if !ok {
		return strings.Contains(err.Error(), "Unknown system variable")
	}
	return code == mysql.ErrUnknownSystemVariable
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

			// after https://github.com/pingcap/tidb/pull/17102 merge,
			// we need set session to true for insert auto_random value in TiDB Backend
			"allow_auto_random_explicit_insert": "1",
		},
	}
	db, err := param.Connect()
	if err != nil {
		if isUnknownSystemVariableErr(err) {
			// not support allow_auto_random_explicit_insert, retry connect
			delete(param.Vars, "allow_auto_random_explicit_insert")
			db, err = param.Connect()
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			return nil, errors.Trace(err)
		}
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
	getTables func(context.Context, string) ([]*model.TableInfo, error),
) (map[string]*TidbDBInfo, error) {
	result := make(map[string]*TidbDBInfo, len(schemas))
	for _, schema := range schemas {
		tables, err := getTables(ctx, schema.Name)
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
				DB:   schema.Name,
				Name: tableName,
				Core: tbl,
			}
			dbInfo.Tables[tableName] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

func ObtainGCLifeTime(ctx context.Context, g glue.Glue) (string, error) {
	return g.ObtainStringLogArgs(
		ctx,
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		"obtain GC lifetime",
	)
}

func UpdateGCLifeTime(ctx context.Context, g glue.Glue, gcLifeTime string) error {
	fields := []zap.Field{zap.String("gcLifeTime", gcLifeTime)}
	query := fmt.Sprintf("UPDATE mysql.tidb SET VARIABLE_VALUE = %s WHERE VARIABLE_NAME = 'tikv_gc_life_time'", gcLifeTime)

	return g.ExecuteWithLogArgs(ctx, query,"update GC lifetime", fields...)
}

func ObtainRowFormatVersion(ctx context.Context, g glue.Glue) string {
	rowFormatVersion, err := g.ObtainStringLogArgs(
		ctx,
		"SELECT @@tidb_row_format_version",
		"obtain row format version",
	)
	if err != nil {
		rowFormatVersion = "1"
	}
	return rowFormatVersion
}

func ObtainNewCollationEnabled(ctx context.Context, g glue.Glue) bool {
	newCollationEnabled := false
	newCollationVal, err := g.ObtainStringLogArgs(
		ctx,
		"SELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'",
		"obtain new collation enabled",
	)
	if err == nil && newCollationVal == "True" {
		newCollationEnabled = true
	}

	return newCollationEnabled
}

// AlterAutoIncrement rebase the table auto increment id
//
// NOTE: since tidb can make sure the auto id is always be rebase even if the `incr` value is smaller
// the the auto incremanet base in tidb side, we needn't fetch currently auto increment value here.
// See: https://github.com/pingcap/tidb/blob/64698ef9a3358bfd0fdc323996bb7928a56cadca/ddl/ddl_api.go#L2528-L2533
func AlterAutoIncrement(ctx context.Context, g glue.Glue, tableName string, incr int64) error {
	fields := []zap.Field{zap.String("table", tableName), zap.Int64("auto_increment", incr)}
	logger := log.With(fields...)
	query := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%d", tableName, incr)
	task := logger.Begin(zap.InfoLevel, "alter table auto_increment")
	err := g.ExecuteWithLogArgs(ctx, query, "alter table auto_increment", fields...)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_increment failed, please perform the query manually (this is needed no matter the table has an auto-increment column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}

func AlterAutoRandom(ctx context.Context, g glue.Glue, tableName string, randomBase int64) error {
	fields := []zap.Field{zap.String("table", tableName), zap.Int64("auto_random", randomBase)}
	logger := log.With(fields...)
	query := fmt.Sprintf("ALTER TABLE %s AUTO_RANDOM_BASE=%d", tableName, randomBase)
	task := logger.Begin(zap.InfoLevel, "alter table auto_random")
	err := g.ExecuteWithLogArgs(ctx, query, "alter table auto_random_base", fields...)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_random_base failed, please perform the query manually (this is needed no matter the table has an auto-random column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}
