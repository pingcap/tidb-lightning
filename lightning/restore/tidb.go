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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
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
	createDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	err := common.ExecWithRetry(ctx, timgr.db, createDatabase, createDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	useDB := fmt.Sprintf("USE `%s`", database)
	err = common.ExecWithRetry(ctx, timgr.db, useDB, useDB)
	if err != nil {
		return errors.Trace(err)
	}

	for _, sqlCreateTable := range tablesSchema {
		timer := time.Now()
		if err = safeCreateTable(ctx, timgr.db, sqlCreateTable); err != nil {
			return errors.Trace(err)
		}
		common.AppLogger.Infof("%s takes %v", sqlCreateTable, time.Since(timer))
	}

	return nil
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

func safeCreateTable(ctx context.Context, db *sql.DB, createTable string) error {
	createTable = createTableIfNotExistsStmt(createTable)
	err := common.ExecWithRetry(ctx, db, createTable, createTable)
	return errors.Trace(err)
}

func (timgr *TiDBManager) getTables(schema string) ([]*model.TableInfo, error) {
	baseURL := *timgr.baseURL
	baseURL.Path = fmt.Sprintf("schema/%s", schema)

	var tables []*model.TableInfo
	err := common.GetJSON(timgr.client, baseURL.String(), &tables)
	if err != nil {
		return nil, errors.Annotatef(errors.Trace(err), "get tables for schema %s", schema)
	}
	return tables, nil
}

func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	query := "DROP TABLE " + tableName
	return errors.Trace(common.ExecWithRetry(ctx, timgr.db, query, query))
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
	query := fmt.Sprintf("SHOW CREATE TABLE %s", common.UniqueTable(schema, table))
	var tbl, createTable string
	err := common.QueryRowWithRetry(ctx, timgr.db, query, &tbl, &createTable)
	return createTable, errors.Annotatef(err, "%s", query)
}

func ObtainGCLifeTime(ctx context.Context, db *sql.DB) (gcLifeTime string, err error) {
	query := "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'"
	err = common.QueryRowWithRetry(ctx, db, query, &gcLifeTime)
	return gcLifeTime, errors.Annotatef(err, "%s", query)
}

func UpdateGCLifeTime(ctx context.Context, db *sql.DB, gcLifeTime string) error {
	query := "UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'"
	err := common.ExecWithRetry(ctx, db, query, query, gcLifeTime)
	return errors.Annotatef(err, "%s -- ? = %s", query, gcLifeTime)
}

func AlterAutoIncrement(ctx context.Context, db *sql.DB, schema string, table string, incr int64) error {
	tableName := common.UniqueTable(schema, table)
	query := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%d", tableName, incr)
	common.AppLogger.Infof("[%s.%s] %s", schema, table, query)
	err := common.ExecWithRetry(ctx, db, query, query)
	if err != nil {
		common.AppLogger.Errorf("query failed %v, you should do it manually, err %v", query, err)
	}
	return errors.Annotatef(err, "%s", query)
}
