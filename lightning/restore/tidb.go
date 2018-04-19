package restore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
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
	ID      int64
	Name    string
	Columns int
	Indices int

	core *model.TableInfo
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

func (timgr *TiDBManager) InitSchema(database string, tablesSchema map[string]string) error {
	_, err := timgr.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = timgr.db.Exec(fmt.Sprintf("USE %s", database))
	if err != nil {
		return errors.Trace(err)
	}

	for _, sqlCreateTable := range tablesSchema {
		if err = safeCreateTable(timgr.db, sqlCreateTable); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func toCreateTableIfNotExists(createTable string) string {
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

func safeCreateTable(db *sql.DB, createTable string) error {
	createTable = toCreateTableIfNotExists(createTable)
	_, err := db.Exec(createTable)
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
	timgr.baseURL.Path = "schema"
	resp, err := timgr.client.Get(timgr.baseURL.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	var schemas []*model.DBInfo
	err = json.NewDecoder(resp.Body).Decode(&schemas)
	return schemas, errors.Trace(err)
}

func (timgr *TiDBManager) getTables(schema string) ([]*model.TableInfo, error) {
	timgr.baseURL.Path = fmt.Sprintf("schema/%s", schema)
	resp, err := timgr.client.Get(timgr.baseURL.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	var tables []*model.TableInfo
	err = json.NewDecoder(resp.Body).Decode(&tables)
	return tables, errors.Trace(err)
}

func (timgr *TiDBManager) LoadSchemaInfo(schema string) (*TidbDBInfo, error) {
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
		tableInfo := &TidbTableInfo{
			ID:      tbl.ID,
			Name:    tableName,
			Columns: len(tbl.Columns),
			Indices: len(tbl.Indices),
			core:    tbl,
		}
		dbInfo.Tables[tableName] = tableInfo
	}

	return dbInfo, nil
}

func (timgr *TiDBManager) SyncSchema(schema string) (*TidbDBInfo, error) {
	return timgr.LoadSchemaInfo(schema)
}

func (tbl *TidbTableInfo) WithExplicitPrimaryKey() bool {
	// TODO : need to check `tableInfo.PKIsHandle` ??
	for _, col := range tbl.core.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			return true
		}
	}
	return false
}

func (tbl *TidbTableInfo) WithIntegerPrimaryKey() bool {
	return tbl.core.PKIsHandle
}

func (tbl *TidbTableInfo) WithAutoIncrPrimaryKey() bool {
	for _, col := range tbl.core.Columns {
		if mysql.HasPriKeyFlag(col.Flag) && mysql.HasAutoIncrementFlag(col.Flag) {
			return true
		}
	}
	return false
}

func ObtainGCLifeTime(db *sql.DB) (gcLifeTime string, err error) {
	r := db.QueryRow(
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'")
	err = r.Scan(&gcLifeTime)
	return gcLifeTime, errors.Annotatef(err, "query tikv_gc_life_time")
}

func UpdateGCLifeTime(db *sql.DB, gcLifeTime string) error {
	_, err := db.Exec(fmt.Sprintf(
		"UPDATE mysql.tidb SET VARIABLE_VALUE = '%s' WHERE VARIABLE_NAME = 'tikv_gc_life_time'", gcLifeTime))
	return errors.Annotatef(err, "update tikv_gc_life_time=%s", gcLifeTime)
}

func AlterAutoIncrement(db *sql.DB, schema string, table string, incr int64) error {
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d", schema, table, incr)
	_, err := db.Exec(query)
	return errors.Annotatef(err, "alter table %s.%s auto_increment=%d", schema, table, incr)
}
