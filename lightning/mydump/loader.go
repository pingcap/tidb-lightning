// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"context"
	"regexp"
	"sort"
	"strings"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile string
	Tables     []*MDTableMeta
	charSet    string
}

type MDTableMeta struct {
	DB         string
	Name       string
	SchemaFile fileInfo
	DataFiles  []fileInfo
	charSet    string
	TotalSize  int64
}

func (m *MDTableMeta) GetSchema(store storage.ExternalStorage) string {
	schema, err := ExportStatement(store, m.SchemaFile, m.charSet)
	if err != nil {
		log.L().Error("failed to extract table schema",
			zap.String("Path", m.SchemaFile.Path),
			log.ShortError(err),
		)
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	store    storage.ExternalStorage
	noSchema bool
	dbs      []*MDDatabaseMeta
	filter   filter.Filter
	router   *router.Table
	charSet  string
}

type mdLoaderSetup struct {
	loader        *MDLoader
	dbSchemas     []tableFileInfo
	tableSchemas  []tableFileInfo
	tableDatas    []tableFileInfo
	dbIndexMap    map[string]int
	tableIndexMap map[filter.Table]int
}

func NewMyDumpLoader(ctx context.Context, cfg *config.Config) (*MDLoader, error) {
	u, err := storage.ParseBackend(cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return nil, err
	}
	s, err := storage.Create(ctx, u, true)
	if err != nil {
		return nil, err
	}

	return NewMyDumpLoaderWithStore(ctx, cfg, s)
}

func NewMyDumpLoaderWithStore(ctx context.Context, cfg *config.Config, store storage.ExternalStorage) (*MDLoader, error) {
	var r *router.Table
	var err error
	if len(cfg.Routes) > 0 {
		r, err = router.NewTableRouter(cfg.Mydumper.CaseSensitive, cfg.Routes)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// use the legacy black-white-list if defined. otherwise use the new filter.
	var f filter.Filter
	if cfg.HasLegacyBlackWhiteList() {
		f, err = filter.ParseMySQLReplicationRules(&cfg.BWList)
	} else {
		f, err = filter.Parse(cfg.Mydumper.Filter)
	}
	if err != nil {
		return nil, err
	}
	if !cfg.Mydumper.CaseSensitive {
		f = filter.CaseInsensitive(f)
	}

	mdl := &MDLoader{
		store:    store,
		noSchema: cfg.Mydumper.NoSchema,
		filter:   f,
		router:   r,
		charSet:  cfg.Mydumper.CharacterSet,
	}

	setup := mdLoaderSetup{
		loader:        mdl,
		dbIndexMap:    make(map[string]int),
		tableIndexMap: make(map[filter.Table]int),
	}

	if err := setup.setup(ctx, mdl.store); err != nil {
		return nil, errors.Trace(err)
	}

	return mdl, nil
}

type fileType int

const (
	fileTypeDatabaseSchema fileType = iota
	fileTypeTableSchema
	fileTypeTableData
)

func (ftype fileType) String() string {
	switch ftype {
	case fileTypeDatabaseSchema:
		return "database schema"
	case fileTypeTableSchema:
		return "table schema"
	case fileTypeTableData:
		return "table data"
	default:
		return "(unknown)"
	}
}

type tableFileInfo struct {
	tableName filter.Table
	fileInfo
}

type fileInfo struct {
	Path string
	Size int64
}

func NewFileInfo(path string, size int64) fileInfo {
	return fileInfo{Path: path, Size: size}
}

var tableNameRegexp = regexp.MustCompile(`^([^.]+)\.(.*?)(?:\.[0-9]+)?$`)

// setup the `s.loader.dbs` slice by scanning all *.sql files inside `dir`.
//
// The database and tables are inserted in a consistent order, so creating an
// MDLoader twice with the same data source is going to produce the same array,
// even after killing Lightning.
//
// This is achieved by using `filepath.Walk` internally which guarantees the
// files are visited in lexicographical order (note that this does not mean the
// databases and tables in the end are ordered lexicographically since they may
// be stored in different subdirectories).
//
// Will sort tables by table Size, this means that the big table is imported
// at the latest, which to avoid large table take a long time to import and block
// small table to release index worker.
func (s *mdLoaderSetup) setup(ctx context.Context, store storage.ExternalStorage) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/
	if err := s.listFiles(ctx, store); err != nil {
		return errors.Annotate(err, "list file failed")
	}
	if err := s.route(); err != nil {
		return errors.Trace(err)
	}

	if !s.loader.noSchema {
		// setup database schema
		if len(s.dbSchemas) == 0 {
			return errors.New("missing {schema}-schema-create.sql")
		}
		for _, fileInfo := range s.dbSchemas {
			if _, dbExists := s.insertDB(fileInfo.tableName.Schema, fileInfo.Path); dbExists && s.loader.router == nil {
				return errors.Errorf("invalid database schema file, duplicated item - %s", fileInfo.Path)
			}
		}

		// setup table schema
		for _, fileInfo := range s.tableSchemas {
			_, dbExists, tableExists := s.insertTable(fileInfo)
			if !dbExists {
				return errors.Errorf("invalid table schema file, cannot find db - %s", fileInfo.Path)
			} else if tableExists && s.loader.router == nil {
				return errors.Errorf("invalid table schema file, duplicated item - %s", fileInfo.Path)
			}
		}
	}

	// Sql file for restore data
	for _, info := range s.tableDatas {
		schemaInfo := tableFileInfo{tableName: info.tableName}
		tableMeta, dbExists, tableExists := s.insertTable(schemaInfo)
		if !s.loader.noSchema {
			if !dbExists {
				return errors.Errorf("invalid data file, miss host db - %s", info.Path)
			} else if !tableExists {
				return errors.Errorf("invalid data file, miss host table - %s", info.Path)
			}
		}
		tableMeta.DataFiles = append(tableMeta.DataFiles, info.fileInfo)
		tableMeta.TotalSize += info.Size
	}

	// Put the small table in the front of the slice which can avoid large table
	// take a long time to import and block small table to release index worker.
	for _, dbMeta := range s.loader.dbs {
		sort.SliceStable(dbMeta.Tables, func(i, j int) bool {
			return dbMeta.Tables[i].TotalSize < dbMeta.Tables[j].TotalSize
		})
	}

	return nil
}

func (s *mdLoaderSetup) listFiles(ctx context.Context, store storage.ExternalStorage) error {
	// `filepath.Walk` yields the paths in a deterministic (lexicographical) order,
	// meaning the file and chunk orders will be the same everytime it is called
	// (as long as the source is immutable).
	err := store.WalkDir(ctx, func(name string, size int64) error {
		lowerFName := strings.ToLower(name)

		info := tableFileInfo{fileInfo: fileInfo{Path: name, Size: size}}
		logger := log.With(zap.String("name", name))

		var (
			ftype         fileType
			qualifiedName string
		)
		switch {
		case strings.HasSuffix(lowerFName, "-schema-create.sql"):
			ftype = fileTypeDatabaseSchema
			qualifiedName = name[:len(name)-18] + "."

		case strings.HasSuffix(lowerFName, "-schema.sql"):
			ftype = fileTypeTableSchema
			qualifiedName = name[:len(name)-11]

			// ignore functionality :
			// 		- view
			//		- triggers
		case strings.HasSuffix(lowerFName, "-schema-view.sql"),
			strings.HasSuffix(lowerFName, "-schema-trigger.sql"),
			strings.HasSuffix(lowerFName, "-schema-post.sql"):
			logger.Warn("[loader] ignore unsupport view/trigger")
			return nil
		case strings.HasSuffix(lowerFName, ".sql"), strings.HasSuffix(lowerFName, ".csv"):
			ftype = fileTypeTableData
			qualifiedName = name[:len(name)-4]
		default:
			return nil
		}

		matchRes := tableNameRegexp.FindStringSubmatch(qualifiedName)
		if len(matchRes) != 3 {
			logger.Debug("[loader] ignore almost " + ftype.String() + " file")
			return nil
		}
		info.tableName.Schema = matchRes[1]
		info.tableName.Name = matchRes[2]

		if s.loader.shouldSkip(&info.tableName) {
			logger.Debug("[filter] ignoring table file")
			return nil
		}

		switch ftype {
		case fileTypeDatabaseSchema:
			s.dbSchemas = append(s.dbSchemas, info)
		case fileTypeTableSchema:
			s.tableSchemas = append(s.tableSchemas, info)
		case fileTypeTableData:
			s.tableDatas = append(s.tableDatas, info)
		}
		return nil
	})

	return errors.Trace(err)
}

func (l *MDLoader) shouldSkip(table *filter.Table) bool {
	if len(table.Name) == 0 {
		return !l.filter.MatchSchema(table.Schema)
	}
	return !l.filter.MatchTable(table.Schema, table.Name)
}

func (s *mdLoaderSetup) route() error {
	r := s.loader.router
	if r == nil {
		return nil
	}

	type dbInfo struct {
		info  fileInfo
		count int
	}

	knownDBNames := make(map[string]dbInfo)
	for _, info := range s.dbSchemas {
		knownDBNames[info.tableName.Schema] = dbInfo{
			info:  info.fileInfo,
			count: 1,
		}
	}
	for _, info := range s.tableSchemas {
		dbInfo := knownDBNames[info.tableName.Schema]
		dbInfo.count++
		knownDBNames[info.tableName.Schema] = dbInfo
	}

	run := func(arr []tableFileInfo) error {
		for i, info := range arr {
			dbName, tableName, err := r.Route(info.tableName.Schema, info.tableName.Name)
			if err != nil {
				return errors.Trace(err)
			}
			if dbName != info.tableName.Schema {
				oldInfo := knownDBNames[info.tableName.Schema]
				oldInfo.count--
				knownDBNames[info.tableName.Schema] = oldInfo

				newInfo, ok := knownDBNames[dbName]
				newInfo.count++
				if !ok {
					newInfo.info = oldInfo.info
					s.dbSchemas = append(s.dbSchemas, tableFileInfo{
						tableName: filter.Table{Schema: dbName},
						fileInfo:  oldInfo.info,
					})
				}
				knownDBNames[dbName] = newInfo
			}
			arr[i].tableName = filter.Table{Schema: dbName, Name: tableName}
		}
		return nil
	}

	if err := run(s.tableSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := run(s.tableDatas); err != nil {
		return errors.Trace(err)
	}

	// remove all schemas which has been entirely routed away
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	remainingSchemas := s.dbSchemas[:0]
	for _, info := range s.dbSchemas {
		if knownDBNames[info.tableName.Schema].count > 0 {
			remainingSchemas = append(remainingSchemas, info)
		}
	}
	s.dbSchemas = remainingSchemas

	return nil
}

func (s *mdLoaderSetup) insertDB(dbName string, path string) (*MDDatabaseMeta, bool) {
	dbIndex, ok := s.dbIndexMap[dbName]
	if ok {
		return s.loader.dbs[dbIndex], true
	} else {
		s.dbIndexMap[dbName] = len(s.loader.dbs)
		ptr := &MDDatabaseMeta{
			Name:       dbName,
			SchemaFile: path,
			charSet:    s.loader.charSet,
		}
		s.loader.dbs = append(s.loader.dbs, ptr)
		return ptr, false
	}
}

func (s *mdLoaderSetup) insertTable(schemaInfo tableFileInfo) (*MDTableMeta, bool, bool) {
	dbMeta, dbExists := s.insertDB(schemaInfo.tableName.Schema, "")
	tableIndex, ok := s.tableIndexMap[schemaInfo.tableName]
	if ok {
		return dbMeta.Tables[tableIndex], dbExists, true
	} else {
		s.tableIndexMap[schemaInfo.tableName] = len(dbMeta.Tables)
		ptr := &MDTableMeta{
			DB:         schemaInfo.tableName.Schema,
			Name:       schemaInfo.tableName.Name,
			SchemaFile: schemaInfo.fileInfo,
			DataFiles:  make([]fileInfo, 0, 16),
			charSet:    s.loader.charSet,
		}
		dbMeta.Tables = append(dbMeta.Tables, ptr)
		return ptr, dbExists, false
	}
}

func (l *MDLoader) GetDatabases() []*MDDatabaseMeta {
	return l.dbs
}
