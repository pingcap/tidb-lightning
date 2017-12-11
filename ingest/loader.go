package ingest

import (
	"path/filepath"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	_ "golang.org/x/net/context"
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
	schema, err := ExportStatment(m.SchemaFile)
	if err != nil {
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
	schema, err := ExportStatment(m.SchemaFile)
	if err != nil {
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	dir string
	dbs map[string]*MDDatabaseMeta
}

func NewMyDumpLoader(cfg *Config) *MDLoader {
	mdl := &MDLoader{
		dir: cfg.SourceDir,
		dbs: make(map[string]*MDDatabaseMeta),
	}

	if err := mdl.setup(mdl.dir); err != nil {
		log.Errorf("init mydumper loader failed : %s\n", err.Error())
		return nil
	}

	return mdl
}

func (l *MDLoader) setup(dir string) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/

	if !IsDirExists(dir) {
		return errMDEmpty
	}

	files := ListFiles(dir)
	metaFile := filepath.Join(dir, "metadata")
	if _, exists := files[metaFile]; !exists {
		return errMDInvalid
	}

	log.Debugf("collected files:%+v", files)

	// DB : [table , table ...]
	if err := l.setupDBs(files); err != nil {
		return err
	}

	// Table : create table ~
	if err := l.setupTables(files); err != nil {
		return err
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
		return errMDMiss
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
		if !(len(fields) == 2 || len(fields) == 3) {
			log.Warnf("invalid db table sql file - %s", fpath)
			continue
		}

		db, table := fields[0], fields[1]
		if dbMeta, ok := l.dbs[db]; !ok {
			return errors.Errorf("invalid data sql file, miss host db - %s", fpath)
		} else if tableMeta, ok := dbMeta.Tables[table]; !ok {
			return errors.Errorf("invalid data sql file, miss host table - %s", fpath)
		} else {
			tableMeta.DataFiles = append(tableMeta.DataFiles, fpath)
		}
	}

	return nil
}

func (l *MDLoader) GetTree() *MDDatabaseMeta {
	for db := range l.dbs {
		return l.dbs[db]
	}
	return nil
}

////////////////////////// Test //////////////////////////

func (l *MDLoader) ShowSchema() {
	for _, dbMeta := range l.dbs {
		log.Debugf("[DB : %s]", dbMeta.Name)

		data, _ := ExportStatment(dbMeta.SchemaFile)
		log.Warnf("-----------  %s", string(data))

		for _, tblMeta := range dbMeta.Tables {
			log.Debugf("table => %s (parts = %d)", tblMeta.Name, len(tblMeta.DataFiles))

			data, _ := ExportStatment(tblMeta.SchemaFile)
			log.Warnf("-----------  %s", string(data))
		}
	}
}
