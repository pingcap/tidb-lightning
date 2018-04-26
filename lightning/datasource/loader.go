package datasource

import (
	"sort"
	"strings"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

const (
	TypeCSV      = "csv"
	TypeMydumper = "mydumper"
)

var (
	// errors
	errMDEmpty   = errors.New("empty mydumper dir")
	errMDInvalid = errors.New("invalid mydumper dir, none metadata exists")
	errMDMiss    = errors.New("invalid mydumper files")
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile string
	Tables     map[string]*MDTableMeta
}

func (m *MDDatabaseMeta) GetSchema() string {
	schema, err := exportStatement(m.SchemaFile)
	if err != nil {
		log.Errorf("failed to extract database schema (%s) : %s", m.SchemaFile, err.Error())
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
	schema, err := exportStatement(m.SchemaFile)
	if err != nil {
		log.Errorf("failed to extract table schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type DataSource struct {
	sourceType string
	dir        string
	dbs        map[string]*MDDatabaseMeta
}

func New(cfg *config.Config) (*DataSource, error) {
	ds := &DataSource{
		sourceType: cfg.DataSource.SourceType,
		dir:        cfg.DataSource.SourceDir,
		dbs:        make(map[string]*MDDatabaseMeta),
	}

	if err := ds.setup(ds.dir); err != nil {
		// log.Errorf("init mydumper loader failed : %s\n", err.Error())
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
		return errMDEmpty
	}

	files := common.ListFiles(dir)

	// ps : skip checking it as no denpendcy on it so far
	// metaFile := filepath.Join(dir, "metadata")
	// if _, exists := files[metaFile]; !exists {
	// 	return errMDInvalid
	// }

	log.Debugf("Files detected : %+v", files)

	// DB : [table , table ...]
	if err := ds.setupDBs(files); err != nil {
		return errors.Trace(err)
	}

	// Table : create table ~
	if err := ds.setupTables(files); err != nil {
		return errors.Trace(err)
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
		return errMDMiss
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
			log.Warnf("invalid table schema file - %s", fpath)
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
	case TypeCSV:
		suffix = ".csv"
	case TypeMydumper:
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
			log.Warnf("[loader] ignore unsupport view/trigger: %s", fpath)
			continue
		}

		idx := strings.Index(fname, suffix)
		name := fname[:idx]
		fields := strings.Split(name, ".")
		if len(fields) < 2 {
			log.Warnf("invalid db table sql file - %s", fpath)
			continue
		}

		db, table := fields[0], fields[1]
		if dbMeta, ok := ds.dbs[db]; !ok {
			return errors.Errorf("invalid data sql file, miss host db - %s", fpath)
		} else if tableMeta, ok := dbMeta.Tables[table]; !ok {
			return errors.Errorf("invalid data sql file, miss host table - %s", fpath)
		} else {
			tableMeta.DataFiles = append(tableMeta.DataFiles, fpath)
		}
	}

	// sort all tables' data files by file-name
	for _, dbMeta := range ds.dbs {
		for _, tblMeta := range dbMeta.Tables {
			sort.Strings(tblMeta.DataFiles)
		}
	}

	return nil
}

func (l *MDLoader) GetDatabase() *MDDatabaseMeta {
	for db := range l.dbs {
		return l.dbs[db]
	}
	return nil
}
