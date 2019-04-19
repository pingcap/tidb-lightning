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
	"net/http"
	"net/url"
	"regexp"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"go.uber.org/zap"
)

type TiDBManager struct {
	db      *sql.DB
	client  *http.Client
	baseURL *url.URL
}

type TidbDBInfo struct {
	Name   string
	Tables map[string]*TidbTableInfo
}

type TidbTableInfo struct {
	ID              int64
	Name            string
	Columns         int
	Indices         int
	CreateTableStmt string
	core            *model.TableInfo
}

func NewTiDBManager(dsn config.DBStore) (*TiDBManager, error) {
	db, err := common.ConnectDB(dsn.Host, dsn.Port, dsn.User, dsn.Psw)
	if err != nil {
		return nil, errors.Trace(err)
	}

	u, err := url.Parse(fmt.Sprintf("http://%s:%d", dsn.Host, dsn.StatusPort))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &TiDBManager{
		db:      db,
		client:  &http.Client{},
		baseURL: u,
	}, nil
}

func (timgr *TiDBManager) Close() {
	timgr.db.Close()
}

func (timgr *TiDBManager) InitSchema(ctx context.Context, database string, tablesSchema map[string]string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("db", database)),
	}

	createDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	err := sql.Exec(ctx, "create database", createDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	useDB := fmt.Sprintf("USE `%s`", database)
	err = sql.Exec(ctx, "use database", useDB)
	if err != nil {
		return errors.Trace(err)
	}

	task := sql.Logger.Begin(zap.InfoLevel, "create tables")
	for tbl, sqlCreateTable := range tablesSchema {
		task.Debug("create table", zap.String("schema", sqlCreateTable))

		safeCreateTable := createTableIfNotExistsStmt(sqlCreateTable)
		sql2 := common.SQLWithRetry{
			DB:           timgr.db,
			Logger:       sql.Logger.With(zap.String("table", common.UniqueTable(database, tbl))),
			HideQueryLog: true,
		}
		err = sql2.Exec(ctx, "create table", safeCreateTable)
		if err != nil {
			break
		}
	}
	task.End(zap.ErrorLevel, err)

	return errors.Trace(err)
}

var createTableRegexp = regexp.MustCompile(`(?i)CREATE TABLE( IF NOT EXISTS)?`)

func createTableIfNotExistsStmt(createTable string) string {
	indices := createTableRegexp.FindStringSubmatchIndex(createTable)
	// if the " IF NOT EXISTS" group is missing, that submatch will be empty.
	if len(indices) == 4 && indices[2] == indices[3] {
		before := createTable[:indices[1]]
		after := createTable[indices[1]:]
		createTable = before + " IF NOT EXISTS " + after
	}
	return createTable
}

func (timgr *TiDBManager) getTables(schema string) ([]*model.TableInfo, error) {
	baseURL := *timgr.baseURL
	baseURL.Path = fmt.Sprintf("schema/%s", schema)

	var tables []*model.TableInfo
	err := common.GetJSON(timgr.client, baseURL.String(), &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "get tables for schema %s", schema)
	}
	return tables, nil
}

func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	return sql.Exec(ctx, "drop table", "DROP TABLE "+tableName)
}

func (timgr *TiDBManager) LoadSchemaInfo(ctx context.Context, schemas []*mydump.MDDatabaseMeta) (map[string]*TidbDBInfo, error) {
	result := make(map[string]*TidbDBInfo, len(schemas))
	for _, schema := range schemas {
		tables, err := timgr.getTables(schema.Name)
		if err != nil {
			return nil, errors.Trace(err)
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
			createTableStmt, err := timgr.getCreateTableStmt(ctx, schema.Name, tableName)
			metric.RecordTableCount(metric.TableStatePending, err)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableInfo := &TidbTableInfo{
				ID:              tbl.ID,
				Name:            tableName,
				Columns:         len(tbl.Columns),
				Indices:         len(tbl.Indices),
				CreateTableStmt: createTableStmt,
				core:            tbl,
			}
			dbInfo.Tables[tableName] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

func (timgr *TiDBManager) getCreateTableStmt(ctx context.Context, schema, table string) (string, error) {
	tableName := common.UniqueTable(schema, table)
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	var tbl, createTable string
	err := sql.QueryRow(ctx, "show create table",
		"SHOW CREATE TABLE "+tableName,
		&tbl, &createTable,
	)
	return createTable, err
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
		task.Error("alter table auto_increment failed, please perform the query manually", zap.String("query", query))
	}
	return errors.Annotatef(err, "%s", query)
}
