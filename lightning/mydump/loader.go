package mydump

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pkg/errors"
)

var (
	// errors
	errDirNotExists = errors.New("mydumper dir not exists")
	errMissingFile  = errors.New("missing file")
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile string
	Tables     []*MDTableMeta
	charSet    string
}

func (m *MDDatabaseMeta) String() string {
	v, err := json.Marshal(m)
	if err != nil {
		common.AppLogger.Errorf("json marshal MDDatabaseMeta error %s", errors.ErrorStack(err))
	}
	return string(v)
}

func (m *MDDatabaseMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile, m.charSet)
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
	charSet    string
}

func (m *MDTableMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile, m.charSet)
	if err != nil {
		common.AppLogger.Errorf("failed to extract table schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	dir           string
	noSchema      bool
	dbSchemas     []fileInfo
	tableSchemas  []fileInfo
	tableDatas    []fileInfo
	dbs           []*MDDatabaseMeta
	filter        *filter.Filter
	dbIndexMap    map[string]int
	tableIndexMap map[filter.Table]int
	charSet       string
}

func NewMyDumpLoader(cfg *config.Config) (*MDLoader, error) {
	mdl := &MDLoader{
		dir:           cfg.Mydumper.SourceDir,
		noSchema:      cfg.Mydumper.NoSchema,
		filter:        filter.New(false, cfg.BWList),
		dbIndexMap:    make(map[string]int),
		tableIndexMap: make(map[filter.Table]int),
		charSet:       cfg.Mydumper.CharacterSet,
	}

	if err := mdl.setup(mdl.dir); err != nil {
		// common.AppLogger.Errorf("init mydumper loader failed : %s\n", err.Error())
		return nil, errors.Trace(err)
	}

	return mdl, nil
}

type fileType int

const (
	fileTypeDatabaseSchema fileType = iota
	fileTypeTableSchema
	fileTypeTableDataSQL
)

func (ftype fileType) String() string {
	switch ftype {
	case fileTypeDatabaseSchema:
		return "database schema"
	case fileTypeTableSchema:
		return "table schema"
	case fileTypeTableDataSQL:
		return "table data SQL"
	default:
		return "(unknown)"
	}
}

type fileInfo struct {
	tableName filter.Table
	path      string
}

var tableNameRegexp = regexp.MustCompile(`^([^.]+)\.(.*?)(?:\.[0-9]+)?$`)

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

	if err := l.listFiles(dir); err != nil {
		common.AppLogger.Errorf("list file failed : %s", err.Error())
		return errors.Trace(err)
	}

	if !l.noSchema {
		// setup database schema
		if len(l.dbSchemas) == 0 {
			return errors.Annotatef(errMissingFile, "missing {schema}-schema-create.sql")
		}
		for _, fileInfo := range l.dbSchemas {
			if _, dbExists := l.insertDB(fileInfo.tableName.Schema, fileInfo.path); dbExists {
				return errors.Errorf("invalid database schema file, duplicated item - %s", fileInfo.path)
			}
		}

		// setup table schema
		for _, fileInfo := range l.tableSchemas {
			_, dbExists, tableExists := l.insertTable(fileInfo.tableName, fileInfo.path)
			if !dbExists {
				return errors.Errorf("invalid table schema file, cannot find db - %s", fileInfo.path)
			} else if tableExists {
				return errors.Errorf("invalid table schema file, duplicated item - %s", fileInfo.path)
			}
		}
	}

	// Sql file for restore data
	for _, fileInfo := range l.tableDatas {
		tableMeta, dbExists, tableExists := l.insertTable(fileInfo.tableName, "")
		if !l.noSchema {
			if !dbExists {
				return errors.Errorf("invalid data file, miss host db - %s", fileInfo.path)
			} else if !tableExists {
				return errors.Errorf("invalid data file, miss host table - %s", fileInfo.path)
			}
		}
		tableMeta.DataFiles = append(tableMeta.DataFiles, fileInfo.path)
	}
	// no need to do further sorting, since `filepath.Walk` yields the paths in
	// a deterministic (lexicographical) order, meaning the chunk order will be
	// the same everytime (as long as the source is immutable).

	return nil
}

func (l *MDLoader) listFiles(dir string) error {
	err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		fname := strings.TrimSpace(f.Name())
		info := fileInfo{path: path}

		var (
			ftype         fileType
			qualifiedName string
		)
		switch {
		case strings.HasSuffix(fname, "-schema-create.sql"):
			ftype = fileTypeDatabaseSchema
			qualifiedName = fname[:len(fname)-18] + "."

		case strings.HasSuffix(fname, "-schema.sql"):
			ftype = fileTypeTableSchema
			qualifiedName = fname[:len(fname)-11]

			// ignore functionality :
			// 		- view
			//		- triggers
		case strings.HasSuffix(fname, "-schema-view.sql"),
			strings.HasSuffix(fname, "-schema-trigger.sql"),
			strings.HasSuffix(fname, "-schema-post.sql"):
			common.AppLogger.Warn("[loader] ignore unsupport view/trigger:", path)
			return nil
		case strings.HasSuffix(fname, ".sql"):
			ftype = fileTypeTableDataSQL
			qualifiedName = fname[:len(fname)-4]
		default:
			return nil
		}

		matchRes := tableNameRegexp.FindStringSubmatch(qualifiedName)
		if len(matchRes) != 3 {
			common.AppLogger.Debugf("[loader] ignore almost %s file: %s", ftype, path)
			return nil
		}
		info.tableName.Schema = matchRes[1]
		info.tableName.Name = matchRes[2]

		if l.shouldSkip(&info.tableName) {
			common.AppLogger.Infof("[filter] ignoring table file %s", path)
			return nil
		}

		switch ftype {
		case fileTypeDatabaseSchema:
			l.dbSchemas = append(l.dbSchemas, info)
		case fileTypeTableSchema:
			l.tableSchemas = append(l.tableSchemas, info)
		case fileTypeTableDataSQL:
			l.tableDatas = append(l.tableDatas, info)
		}
		return nil
	})

	return errors.Trace(err)
}

func (l *MDLoader) shouldSkip(table *filter.Table) bool {
	return len(l.filter.ApplyOn([]*filter.Table{table})) == 0
}

func (l *MDLoader) insertDB(dbName string, path string) (*MDDatabaseMeta, bool) {
	dbIndex, ok := l.dbIndexMap[dbName]
	if ok {
		return l.dbs[dbIndex], true
	} else {
		l.dbIndexMap[dbName] = len(l.dbs)
		ptr := &MDDatabaseMeta{
			Name:       dbName,
			SchemaFile: path,
			charSet:    l.charSet,
		}
		l.dbs = append(l.dbs, ptr)
		return ptr, false
	}
}

func (l *MDLoader) insertTable(tableName filter.Table, path string) (*MDTableMeta, bool, bool) {
	dbMeta, dbExists := l.insertDB(tableName.Schema, "")
	tableIndex, ok := l.tableIndexMap[tableName]
	if ok {
		return dbMeta.Tables[tableIndex], dbExists, true
	} else {
		l.tableIndexMap[tableName] = len(dbMeta.Tables)
		ptr := &MDTableMeta{
			DB:         tableName.Schema,
			Name:       tableName.Name,
			SchemaFile: path,
			DataFiles:  make([]string, 0, 16),
			charSet:    l.charSet,
		}
		dbMeta.Tables = append(dbMeta.Tables, ptr)
		return ptr, dbExists, false
	}
}

func (l *MDLoader) GetDatabases() []*MDDatabaseMeta {
	return l.dbs
}
