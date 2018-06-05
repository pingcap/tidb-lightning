package mydump

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
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
		log.Error("json marshal MDDatabaseMeta error %s", errors.ErrorStack(err))
	}
	return string(v)
}

func (m *MDDatabaseMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile)
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
	schema, err := ExportStatement(m.SchemaFile)
	if err != nil {
		log.Errorf("failed to extract table schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	dir      string
	noSchema bool
	dbs      map[string]*MDDatabaseMeta
}

func NewMyDumpLoader(cfg *config.Config) (*MDLoader, error) {
	mdl := &MDLoader{
		dir:      cfg.Mydumper.SourceDir,
		noSchema: cfg.Mydumper.NoSchema,
		dbs:      make(map[string]*MDDatabaseMeta),
	}

	if err := mdl.setup(mdl.dir); err != nil {
		// log.Errorf("init mydumper loader failed : %s\n", err.Error())
		return nil, errors.Trace(err)
	}

	return mdl, nil
}

func (l *MDLoader) setup(dir string) error {
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

	log.Debugf("Files detected : %+v", files)

	if !l.noSchema {
		// DB : [table , table ...]
		if err := l.setupDBs(files); err != nil {
			return errors.Trace(err)
		}

		// Table : create table ~
		if err := l.setupTables(files); err != nil {
			return errors.Trace(err)
		}
	}

	// Sql file for restore data
	return l.setupTablesData(files)
}

func (l *MDLoader) setupDBs(files map[string]string) error {
	for fpath, fname := range files {
		if !strings.HasSuffix(fname, "-schema-create.sql") {
			continue
		}

		idx := strings.Index(fname, "-schema-create.sql")
		dbname := fname[:idx]
		l.dbs[dbname] = &MDDatabaseMeta{
			Name:       dbname,
			SchemaFile: fpath,
			Tables:     make(map[string]*MDTableMeta),
		}
	}

	if len(l.dbs) == 0 {
		return errors.Annotatef(errMissingFile, "missing {schema}-schema-create.sql")
	}

	return nil
}

func (l *MDLoader) setupTables(files map[string]string) error {
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
		dbMeta, ok := l.dbs[db]
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

func (l *MDLoader) setupTablesData(files map[string]string) error {
	for fpath, fname := range files {
		if !strings.HasSuffix(fname, ".sql") ||
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

		idx := strings.Index(fname, ".sql")
		name := fname[:idx]
		fields := strings.Split(name, ".")
		if len(fields) < 2 {
			log.Warnf("invalid db table sql file - %s", fpath)
			continue
		}

		db, table := fields[0], fields[1]
		dbMeta, ok := l.dbs[db]
		if !ok {
			if !l.noSchema {
				return errors.Errorf("invalid data sql file, miss host db - %s", fpath)
			}
			dbMeta = &MDDatabaseMeta{
				Name:   db,
				Tables: make(map[string]*MDTableMeta),
			}
			l.dbs[db] = dbMeta
		}
		tableMeta, ok := dbMeta.Tables[table]
		if !ok {
			if !l.noSchema {
				return errors.Errorf("invalid data sql file, miss host table - %s", fpath)
			}
			tableMeta = &MDTableMeta{
				DB:        db,
				Name:      table,
				DataFiles: make([]string, 0, 16),
			}
			dbMeta.Tables[table] = tableMeta
		}
		// tableMeta.Rows += l.countTableFileRows(fpath)
		tableMeta.DataFiles = append(tableMeta.DataFiles, fpath)
	}

	// sort all tables' data files by file-name
	for _, dbMeta := range l.dbs {
		for _, tblMeta := range dbMeta.Tables {
			sort.Strings(tblMeta.DataFiles)
		}
	}

	return nil
}

func (l *MDLoader) GetDatabase() *MDDatabaseMeta {
	// TODO: support multiple databases.
	for db := range l.dbs {
		return l.dbs[db]
	}
	return nil
}
