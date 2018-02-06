package mydump

import (
	"bytes"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"

	"github.com/pingcap/tidb-lightning/ingest/common"
	"github.com/pingcap/tidb-lightning/ingest/config"
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
	dir string
	dbs map[string]*MDDatabaseMeta
}

func NewMyDumpLoader(cfg *config.Config) (*MDLoader, error) {
	mdl := &MDLoader{
		dir: cfg.SourceDir,
		dbs: make(map[string]*MDDatabaseMeta),
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
		return errMDEmpty
	}

	files := common.ListFiles(dir)
	metaFile := filepath.Join(dir, "metadata")
	if _, exists := files[metaFile]; !exists {
		return errMDInvalid
	}

	log.Debugf("Files detected : %+v", files)

	// DB : [table , table ...]
	if err := l.setupDBs(files); err != nil {
		return errors.Trace(err)
	}

	// Table : create table ~
	if err := l.setupTables(files); err != nil {
		return errors.Trace(err)
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
			// tableMeta.Rows += l.countTableFileRows(fpath)
			tableMeta.DataFiles = append(tableMeta.DataFiles, fpath)
		}
	}

	// sort all tables' data files by file-name
	for _, dbMeta := range l.dbs {
		for _, tblMeta := range dbMeta.Tables {
			sort.Strings(tblMeta.DataFiles)
		}
	}

	return nil
}

func (l *MDLoader) countTableFileRows(file string) int {
	reader, err := NewMDDataReader(file, 0)
	if err != nil {
		log.Errorf("read mydump file failed (%s) : %s", file, err.Error())
		return -1
	}
	defer reader.Close()

	var rows int
	for {
		statements, err := reader.Read(defReadBlockSize)
		if err == io.EOF {
			break
		}

		for _, stmt := range statements {
			rows += countValues(stmt)
		}
	}
	return rows
}

func (l *MDLoader) GetDatabase() *MDDatabaseMeta {
	for db := range l.dbs {
		return l.dbs[db]
	}
	return nil
}

func countValues(sqlText []byte) int {
	var textLen int = len(sqlText)
	var slice []byte
	var rows int = 0

	for i, c1 := range sqlText {
		if c1 == ')' && i < textLen-1 {
			slice = bytes.TrimSpace(sqlText[i+1:])
			if len(slice) > 0 && (slice[0] == ',' || slice[0] == ';') {
				rows += 1
			}
		}
	}
	return rows
}
