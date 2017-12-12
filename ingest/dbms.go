package ingest

import (
	"database/sql"

	_ "github.com/juju/errors"
	"github.com/ngaut/log"
)

/*
	Mission
	- ID
	- DataSrouce
	- Cluster
		- PD addr
*/

/*
	Kv Deliver Session
	- Server
	- UUID
	- Timestamp
*/

const (
	progressDB string = "progress"

	progressTableSchema string = `CREATE TABLE IF NOT EXISTS table_file_progress (
		id INTEGER PRIMARY KEY AUTO_INCREMENT,
	    db VARCHAR(128) NOT NULL DEFAULT '',
	    tablename VARCHAR(128) NOT NULL DEFAULT '',
	    file VARCHAR(512) NOT NULL DEFAULT '',
	    stage VARCHAR(64) NOT NULL DEFAULT '',
	    start_offset BIGINT NOT NULL DEFAULT 0,
	    end_offset BIGINT NOT NULL DEFAULT 0,
	    max_row_id BIGINT NOT NULL DEFAULT -1,

	    KEY idx_dtf (db, tablename, file),
	    KEY idx_dtfs (db, tablename, file, stage)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;`
)

type ProgressDBMS struct {
	db *sql.DB
}

func NewProgressDBMS(db *sql.DB, database string) *ProgressDBMS {
	dbms := &ProgressDBMS{db: db}
	return dbms.init(database)
}

func (dbms *ProgressDBMS) init(database string) *ProgressDBMS {
	dbms.db.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	dbms.db.Exec("USE " + database)
	dbms.db.Exec(progressTableSchema)
	return dbms
}

func (dbms *ProgressDBMS) Setup(tpgs map[string]*TableProgress) {
	for _, tpg := range tpgs {
		for _, fpg := range tpg.FilesPrgoress {
			dbms.insertFileProgress(fpg)
			fpg.dbms = dbms
		}
		tpg.dbms = dbms
	}
}

func (dbms *ProgressDBMS) LoadAllProgress(database string, tables []string) map[string]*TableProgress {
	tablesProgress := make(map[string]*TableProgress)
	for _, table := range tables {
		tpg := NewTableProgress(database, table, map[string]int64{})
		if ok := tpg.Recover(dbms.db); !ok {
			// TODO ...
			log.Warnf("[%s] table gets not any preogress record !", table)
			continue
		}
		tablesProgress[table] = tpg
	}

	return tablesProgress
}

func (dbms *ProgressDBMS) loadTableProgress(database string, table string, tpg *TableProgress) bool {
	// query all file regions belongs to db/table
	db := dbms.db
	rows, err := db.Query(
		"SELECT * FROM `table_file_progress` WHERE `db` = ? AND `tablename` = ?", database, table)
	if err != nil {
		log.Debugf("err = %v", err)
		return false
	}
	defer rows.Close()

	var regionNum int = 0
	for rows.Next() {
		regionNum++

		var id int
		var _db, _table, name, stage string
		var soff, eoff, maxRowID int64

		_ = rows.Scan(&id, &_db, &_table, &name, &stage, &soff, &eoff, &maxRowID)

		// region of file
		tableRegion := NewTableRegion(database, table, name)
		tableRegion.StartOffset = soff
		tableRegion.EndOffset = eoff
		tableRegion.MaxRowID = maxRowID

		// whole file
		fileProgress, ok := tpg.FilesPrgoress[name]
		if !ok {
			fileProgress = NewTableFileProgress(database, table, name, 0)
			tpg.FilesPrgoress[name] = fileProgress
		}
		fileProgress.addRegion(tableRegion, stage)
	}

	return regionNum > 0
}

func (dbms *ProgressDBMS) insertFileProgress(fpg *TableFileProgress) {
	sql := "INSERT INTO `table_file_progress` " +
		"(`db`,`tablename`,`file`,`stage`,`start_offset`,`end_offset`, `max_row_id`) " +
		"VALUES (?,?,?,?,?,?,?)"

	tx, _ := dbms.db.Begin()
	for stage, r := range fpg.regions {
		_, _ = tx.Exec(sql, fpg.DB, fpg.Table, fpg.Name, stage, r.StartOffset, r.EndOffset, r.MaxRowID)
	}
	_ = tx.Commit()
}

func (dbms *ProgressDBMS) updateFileProgress(fpg *TableFileProgress) {
	sql := "UPDATE `table_file_progress` " +
		"SET `start_offset` = ?, `end_offset` = ?, `max_row_id` = ? " +
		"WHERE `db` = ? AND `tablename` = ? AND `file` = ? AND `stage` = ?"

	tx, _ := dbms.db.Begin()
	for stage, region := range fpg.regions {
		// log.Debugf("[%s - %s - %s] off = %d , row = %d",
		// 	fpg.DB, fpg.Table, fpg.Name, region.EndOffset, region.MaxRowID)

		_, _ = tx.Exec(sql, region.StartOffset, region.EndOffset, region.MaxRowID,
			fpg.DB, fpg.Table, fpg.Name, stage)
	}
	tx.Commit()
}

func (dbms *ProgressDBMS) getTableFiles(database string, table string) []string {
	rows, _ := dbms.db.Query(
		"SELECT DISTINCT file FROM `table_file_progress` WHERE `db` = ? AND `tablename` = ?", database, table)
	defer rows.Close()

	files := make([]string, 0, 1)
	for file := ""; rows.Next(); {
		rows.Scan(&file)
		files = append(files, file)
	}
	return files
}

///////////////////////////

func (tpg *TableProgress) Recover(db *sql.DB) bool {
	// TODO : ugly code
	tpg.dbms = &ProgressDBMS{db: db}
	ok := tpg.dbms.loadTableProgress(tpg.DB, tpg.Table, tpg)
	for _, fpg := range tpg.FilesPrgoress {
		fpg.dbms = tpg.dbms
	}
	return ok
}
