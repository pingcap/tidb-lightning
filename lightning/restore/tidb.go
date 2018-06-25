package restore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb/model"
	"golang.org/x/net/context"
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
	err := common.ExecWithRetry(ctx, timgr.db, []string{createDatabase})
	if err != nil {
		return errors.Trace(err)
	}
	useDB := fmt.Sprintf("USE `%s`", database)
	err = common.ExecWithRetry(ctx, timgr.db, []string{useDB})
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

func createTableIfNotExistsStmt(createTable string) string {
	upCreateTable := strings.ToUpper(createTable)
	if strings.Index(upCreateTable, "CREATE TABLE IF NOT EXISTS") < 0 {
		substrs := strings.SplitN(upCreateTable, "CREATE TABLE", 2)
		if len(substrs) == 2 {
			prefix := substrs[0] // ps : annotation might be
			schema := substrs[1] // ps : schema definition in detail
			createTable = prefix + " CREATE TABLE IF NOT EXISTS " + createTable[len(createTable)-len(schema):]
		}
	}

	return createTable
}

func safeCreateTable(ctx context.Context, db *sql.DB, createTable string) error {
	createTable = createTableIfNotExistsStmt(createTable)
	err := common.ExecWithRetry(ctx, db, []string{createTable})
	return errors.Trace(err)
}

func (timgr *TiDBManager) GetSchemas() ([]*model.DBInfo, error) {
	schemas, err := timgr.getSchemas()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// schema.Tables is empty, we need to set them manually.
	for _, schema := range schemas {
		tables, err := timgr.getTables(schema.Name.String())
		if err != nil {
			return nil, errors.Trace(err)
		}
		schema.Tables = tables
	}
	return schemas, nil
}

func (timgr *TiDBManager) getSchemas() ([]*model.DBInfo, error) {
	baseURL := *timgr.baseURL
	baseURL.Path = "schema"
	resp, err := timgr.client.Get(baseURL.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return nil, errors.Errorf("get %s http status code != 200, message %s", baseURL.String(), string(body))
	}

	var schemas []*model.DBInfo
	err = json.NewDecoder(resp.Body).Decode(&schemas)
	return schemas, errors.Trace(err)
}

func (timgr *TiDBManager) getTables(schema string) ([]*model.TableInfo, error) {
	baseURL := *timgr.baseURL
	baseURL.Path = fmt.Sprintf("schema/%s", schema)
	resp, err := timgr.client.Get(baseURL.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return nil, errors.Errorf("get %s http status code !=200, message %s", baseURL.String(), string(body))
	}

	var tables []*model.TableInfo
	err = json.NewDecoder(resp.Body).Decode(&tables)
	return tables, errors.Annotatef(err, "get tables for schema %s", schema)
}

func (timgr *TiDBManager) LoadSchemaInfo(ctx context.Context, schema string) (*TidbDBInfo, error) {
	tables, err := timgr.getTables(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbInfo := &TidbDBInfo{
		Name:   schema,
		Tables: make(map[string]*TidbTableInfo),
	}

	for _, tbl := range tables {
		tableName := tbl.Name.String()
		if tbl.State != model.StatePublic {
			return nil, errors.Errorf("table [%s.%s] state is not public", schema, tableName)
		}
		createTableStmt, err := timgr.getCreateTableStmt(ctx, schema, tableName)
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

	return dbInfo, nil
}

func (timgr *TiDBManager) getCreateTableStmt(ctx context.Context, schema, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
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
	query := fmt.Sprintf(
		"UPDATE mysql.tidb SET VARIABLE_VALUE = '%s' WHERE VARIABLE_NAME = 'tikv_gc_life_time'", gcLifeTime)
	err := common.ExecWithRetry(ctx, db, []string{query})
	return errors.Annotatef(err, "%s", query)
}

func AlterAutoIncrement(ctx context.Context, db *sql.DB, schema string, table string, incr int64) error {
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d", schema, table, incr)
	common.AppLogger.Infof("[%s.%s] %s", schema, table, query)
	err := common.ExecWithRetry(ctx, db, []string{query})
	if err != nil {
		log.Errorf("query failed %v, you should do it manually", query, err)
	}
	return errors.Annotatef(err, "%s", query)
}
