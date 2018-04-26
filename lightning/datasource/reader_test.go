package datasource

import (
	"database/sql"
	"fmt"
	"io"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

const (
	utestDB         string = "_mydump_reader_utest_"
	utestDataSource string = "./examples"
)

////////////////////////////
type dbManager struct {
	database string
	db       *sql.DB
}

func newDBManager(c *C) *dbManager {
	db, err := common.ConnectDB("localhost", 3306, "root", "")
	c.Assert(err, IsNil)
	mgr := &dbManager{
		database: utestDB,
		db:       db,
	}
	return mgr.init("")
}

func (d *dbManager) init(schema string) *dbManager {
	d.db.Exec("create database if not exists " + d.database)
	d.db.Exec("use " + d.database)
	if len(schema) > 0 {
		d.db.Exec(schema)
	}
	return d
}

func (d *dbManager) clear() *dbManager {
	d.db.Exec("drop database " + utestDB)
	return d
}

func (d *dbManager) close() {
	if d.db != nil {
		d.db.Close()
		d.db = nil
	}
}

//////////////////////////////////////////////////////////

var _ = Suite(&testMydumpReaderSuite{})

type testMydumpReaderSuite struct{}

func (s *testMydumpReaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpReaderSuite) TearDownSuite(c *C) {}

func checkTableData(c *C, db *sql.DB) {
	sql := "select count(distinct ID) cnt from `tbl_autoid`"
	count := 0
	db.QueryRow(sql).Scan(&count)
	c.Assert(count, Equals, 10000)

	sql = "select count(distinct Name) cnt from `tbl_multi_index`"
	db.QueryRow(sql).Scan(&count)
	c.Assert(count, Equals, 10000)
}

func mydump2mysql(c *C, dbMeta *MDDatabaseMeta, minBlockSize int64) {
	dbMgr := newDBManager(c)
	defer func() {
		dbMgr.clear().close()
	}()

	db := dbMgr.db
	for _, tblMeta := range dbMeta.Tables {
		sqlCreteTable, err := ExportStatement(tblMeta.SchemaFile)
		c.Assert(err, IsNil)
		dbMgr.init(string(sqlCreteTable))

		for _, file := range tblMeta.DataFiles {
			reader, err := NewDataReader("mydumper", file, 0)
			c.Assert(err, IsNil)
			defer reader.Close()

			for {
				statements, err := reader.Read(minBlockSize)
				if err == io.EOF {
					break
				}
				for _, stmt := range statements {
					_, err = db.Exec(string(stmt))
					c.Assert(err, IsNil)
				}
			}
			fileSize, err := common.GetFileSize(file)
			c.Assert(err, IsNil)
			c.Assert(reader.Tell(), Equals, fileSize)
		}
	}

	checkTableData(c, db)
	return
}

func (s *testMydumpReaderSuite) TestReader(c *C) {
	fmt.Println("Testing mydump reader ...")

	cfg := &config.Config{DataSource: config.DataSource{SourceDir: utestDataSource}}

	mdl, err := NewMyDumpLoader(cfg)
	c.Assert(err, IsNil)
	dbMeta := mdl.GetDatabase()

	var minSize int64 = 512
	var maxSize int64 = 1024 * 128
	for blockSize := minSize; blockSize <= maxSize; blockSize += 512 {
		mydump2mysql(c, dbMeta, blockSize)
	}
}
