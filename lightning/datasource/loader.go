package datasource

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
)

var (
	// errors
	errDirNotExists = errors.New("mydumper dir not exists")
	errMissingFile  = errors.New("missing file")
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile string
	Tables     map[string]*MDTableMeta
}

func (m *MDDatabaseMeta) String() string {
	v, err := json.Marshal(m)
	if err != nil {
		common.AppLogger.Error("json marshal MDDatabaseMeta error %s", errors.ErrorStack(err))
	}
	return string(v)
}

func (m *MDDatabaseMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile)
	if err != nil {
		common.AppLogger.Errorf("failed to extract database schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

type MDTableMeta struct {
	DB         string
	Name       string
	SchemaFile string
	DataFiles  []string
}

func (m *MDTableMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile)
	if err != nil {
		common.AppLogger.Errorf("failed to extract table schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type DataSource struct {
	sourceType string
	noSchema   bool
	dir        string
	dbs        map[string]*MDDatabaseMeta
}

func New(cfg *config.Config) (*DataSource, error) {
	ds := &DataSource{
		sourceType: cfg.DataSource.SourceType,
		dir:        cfg.DataSource.SourceDir,
		dbs:        make(map[string]*MDDatabaseMeta),
		noSchema:   cfg.DataSource.NoSchema,
	}

	if err := ds.setup(ds.dir); err != nil {
		// common.AppLogger.Errorf("init mydumper loader failed : %s\n", err.Error())
		return nil, errors.Trace(err)
	}

	return ds, nil
}

func (ds *DataSource) setup(dir string) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/
	if !common.IsDirExists(dir) {
		return errors.Annotatef(errDirNotExists, "dir %s", dir)
	}

	files := common.ListFiles(dir)

	common.AppLogger.Debugf("Files detected : %+v", files)

	if !ds.noSchema {
		// DB : [table , table ...]
		if err := ds.setupDBs(files); err != nil {
			return errors.Trace(err)
		}

		// Table : create table ~
		if err := ds.setupTables(files); err != nil {
			return errors.Trace(err)
		}
	}

	// Sql file for restore data
	return ds.setupTablesData(files)
}

func (ds *DataSource) setupDBs(files map[string]string) error {
	for fpath, fname := range files {
		if !strings.HasSuffix(fname, "-schema-create.sql") {
			continue
		}

		idx := strings.Index(fname, "-schema-create.sql")
		dbname := fname[:idx]
		ds.dbs[dbname] = &MDDatabaseMeta{
			Name:       dbname,
			SchemaFile: fpath,
			Tables:     make(map[string]*MDTableMeta),
		}
	}

	if len(ds.dbs) == 0 {
		return errors.Annotatef(errMissingFile, "missing {schema}-schema-create.sql")
	}

	return nil
}

func (ds *DataSource) setupTables(files map[string]string) error {
	for fpath, fname := range files {
		// filepath.Base(path)
		if !strings.HasSuffix(fname, "-schema.sql") {
			continue
		}

		idx := strings.Index(fname, "-schema.sql")
		name := fname[:idx]
		fields := strings.Split(name, ".")
		if len(fields) != 2 {
			common.AppLogger.Warnf("invalid table schema file - %s", fpath)
			continue
		}

		db, table := fields[0], fields[1]
		dbMeta, ok := ds.dbs[db]
		if !ok {
			return errors.Errorf("invalid table schema file, cannot find db - %s", fpath)
		} else if _, exists := dbMeta.Tables[table]; exists {
			return errors.Errorf("invalid table schema file, duplicated item - %s", fpath)
		} else {
			dbMeta.Tables[table] = &MDTableMeta{
				DB:         db,
				Name:       table,
				SchemaFile: fpath,
				DataFiles:  make([]string, 0, 16),
			}
		}
	}

	return nil
}

func (ds *DataSource) setupTablesData(files map[string]string) error {
	var suffix string
	switch ds.sourceType {
	case base.TypeCSV:
		suffix = ".csv"
	case base.TypeMydumper:
		suffix = ".sql"
	}

	for fpath, fname := range files {
		if !strings.HasSuffix(fname, suffix) ||
			strings.Index(fname, "-schema.sql") >= 0 ||
			strings.Index(fname, "-schema-create.sql") >= 0 {
			continue
		}

		// ignore functionality :
		// 		- view
		//		- triggers
		if strings.Index(fname, "-schema-view.sql") >= 0 ||
			strings.Index(fname, "-schema-triggers.sql") >= 0 ||
			strings.Index(fname, "-schema-post.sql") >= 0 {
			common.AppLogger.Warnf("[loader] ignore unsupport view/trigger: %s", fpath)
			continue
		}

		idx := strings.Index(fname, suffix)
		name := fname[:idx]
		fields := strings.Split(name, ".")
		if len(fields) < 2 {
			common.AppLogger.Warnf("invalid db table sql file - %s", fpath)
			continue
		}

		db, table := fields[0], fields[1]
		dbMeta, ok := ds.dbs[db]
		if !ok {
			if !ds.noSchema {
				return errors.Errorf("invalid data sql file, miss host db - %s", fpath)
			}
			dbMeta = &MDDatabaseMeta{
				Name:   db,
				Tables: make(map[string]*MDTableMeta),
			}
			ds.dbs[db] = dbMeta
		}
		tableMeta, ok := dbMeta.Tables[table]
		if !ok {
			if !ds.noSchema {
				return errors.Errorf("invalid data sql file, miss host table - %s", fpath)
			}
			tableMeta = &MDTableMeta{
				DB:        db,
				Name:      table,
				DataFiles: make([]string, 0, 16),
			}
			dbMeta.Tables[table] = tableMeta
		}
		tableMeta.DataFiles = append(tableMeta.DataFiles, fpath)
	}

	// sort all tables' data files by file-name
	for _, dbMeta := range ds.dbs {
		for _, tblMeta := range dbMeta.Tables {
			sort.Strings(tblMeta.DataFiles)
		}
	}

	return nil
}

func (ds *DataSource) GetDatabase() *MDDatabaseMeta {
	for db := range ds.dbs {
		return ds.dbs[db]
	}
	return nil
}

func ExportStatement(sqlFile string) ([]byte, error) {
	fd, err := os.Open(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	f, err := os.Stat(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	data := make([]byte, 0, f.Size()+1)
	buffer := make([]byte, 0, f.Size()+1)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}

		buffer = append(buffer, []byte(line)...)
		if buffer[len(buffer)-1] == ';' {
			statement := string(buffer)
			if !(strings.HasPrefix(statement, "/*") && strings.HasSuffix(statement, "*/;")) {
				data = append(data, buffer...)
			}
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, '\n')
		}
	}

	return data, nil
}
