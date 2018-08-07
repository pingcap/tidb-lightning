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
	tidb "github.com/pingcap/tidb-lightning/lightning/model"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/model"
	"golang.org/x/net/context"
)

type TiDBManager struct {
	db      *sql.DB
	client  *http.Client
	baseURL *url.URL
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

func (timgr *TiDBManager) InitSchema(ctx context.Context, router *router.Table, database string, tableSchemas map[string]string) error {
	originalDatabase := database
	database, _ = fetchMatchedLiteral(router, database, "")
	createDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	common.AppLogger.Infof("create database statement: %s", createDatabase)
	err := common.ExecWithRetry(ctx, timgr.db, []string{createDatabase})
	if err != nil {
		return errors.Trace(err)
	}
	useDB := fmt.Sprintf("USE `%s`", database)
	err = common.ExecWithRetry(ctx, timgr.db, []string{useDB})
	if err != nil {
		return errors.Trace(err)
	}

	for table, sqlCreateTable := range tableSchemas {
		if err = safeCreateTable(ctx, router, timgr.db, originalDatabase, table, sqlCreateTable); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func safeCreateTable(ctx context.Context, router *router.Table, db *sql.DB, schema string, table string, createTable string) error {
	createTable = createTableIfNotExistsStmt(router, schema, table, createTable)

	timer := time.Now()
	err := common.ExecWithRetry(ctx, db, []string{createTable})
	common.AppLogger.Infof("%s takes %v", createTable, time.Since(timer))
	return errors.Trace(err)
}

func createTableIfNotExistsStmt(router *router.Table, schema string, table string, createTable string) string {
	createTable = strings.ToUpper(createTable)
	if strings.Index(createTable, "CREATE TABLE IF NOT EXISTS") < 0 {
		substrs := strings.SplitN(createTable, "CREATE TABLE", 2)
		// TODO: rename table
		if len(substrs) == 2 {
			prefix := substrs[0] // ps : annotation might be
			schema := substrs[1] // ps : schema definition in detail
			createTable = prefix + " CREATE TABLE IF NOT EXISTS " + createTable[len(createTable)-len(schema):]
		}
	}

	_, targetTable := fetchMatchedLiteral(router, schema, table)
	createTable = renameShardingTable(createTable, table, targetTable)

	return createTable
}

func (timgr *TiDBManager) GetSchemas() ([]*model.DBInfo, error) {
	schemas, err := timgr.getSchemas()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// schema.Tables is empty, we need to set them manually.
	for _, schema := range schemas {
		tables, err := timgr.getTables(schema.Name.L)
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

func (timgr *TiDBManager) LoadSchemaInfo(ctx context.Context, router *router.Table, schemas map[string]*mydump.MDDatabaseMeta) (err error) {
	tableInfoCache := make(map[string][]*model.TableInfo)
	for srcSchema, dbMeta := range schemas {

		targetSchema, _ := fetchMatchedLiteral(router, srcSchema, "")

		tables, ok := tableInfoCache[targetSchema]
		if !ok {
			tables, err = timgr.getTables(targetSchema)
			if err != nil {
				return errors.Trace(err)
			}
			tableInfoCache[targetSchema] = tables
		}

		dbInfo := &tidb.DBInfo{
			Name:   targetSchema,
			Tables: make(map[string]*tidb.TableInfo),
		}

		for _, tbl := range tables {
			tableName := tbl.Name.L
			if tbl.State != model.StatePublic {
				return errors.Errorf("table [%s.%s] state is not public", targetSchema, tableName)
			}

			createTableStmt, err := timgr.getCreateTableStmt(ctx, targetSchema, tableName)
			if err != nil {
				return errors.Trace(err)
			}

			tableInfo := &tidb.TableInfo{
				ID:              tbl.ID,
				Name:            tableName,
				Columns:         len(tbl.Columns),
				Indices:         len(tbl.Indices),
				CreateTableStmt: createTableStmt,
				Core:            tbl,
			}

			dbInfo.Tables[tableName] = tableInfo
		}

		// set TableInfo
		common.AppLogger.Infof("meta tables %+v", dbMeta.Tables)
		for srcTable, tableMeta := range dbMeta.Tables {
			_, targetTable := fetchMatchedLiteral(router, srcSchema, srcTable)
			common.AppLogger.Infof("srcSchema %s, srcTable %s, targetTable %s", srcSchema, srcTable, targetTable)
			common.AppLogger.Infof("dbinfo.tables %+v", dbInfo.Tables)
			for _, tableInfo := range dbInfo.Tables {
				common.AppLogger.Infof("set table info %+v", tableInfo)
				if tableInfo.Name == targetTable {
					tableMeta.SetTableInfo(tableInfo)
				}
			}
		}

		// set DBInfo
		common.AppLogger.Infof("set dbinfo info %+v", dbInfo)
		dbMeta.SetDBInfo(dbInfo)
	}
	return nil
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
		common.AppLogger.Errorf("query failed %v, you should do it manually, err %v", query, err)
	}
	return errors.Annotatef(err, "%s", query)
}
