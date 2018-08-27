package mydump_test

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	. "github.com/pingcap/tidb-lightning/lightning/mydump"
)

const (
	utestDB         string = "_mydump_reader_utest_"
	utestDataSrouce string = "./examples"
)

////////////////////////////
type dbManager struct {
	database string
	db       *sql.DB
}

func newDBManager() (*dbManager, error) {
	db, err := common.ConnectDB("127.0.0.1", 3306, "root", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	mgr := &dbManager{
		database: utestDB,
		db:       db,
	}
	return mgr.init(""), nil
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
	dbMgr, err := newDBManager()
	c.Assert(err, IsNil)
	defer func() {
		dbMgr.clear().close()
	}()

	db := dbMgr.db
	for _, tblMeta := range dbMeta.Tables {
		sqlCreteTable, _ := ExportStatement(tblMeta.SchemaFile)
		dbMgr.init(string(sqlCreteTable))

		for _, file := range tblMeta.DataFiles {
			reader, err := NewMDDataReader(file, 0)
			c.Assert(err, IsNil)
			defer reader.Close()

			for {
				statements, err := reader.Read(minBlockSize)
				if errors.Cause(err) == io.EOF {
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

	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: utestDataSrouce}}

	mdl, err := NewMyDumpLoader(cfg)
	c.Assert(err, IsNil)
	dbMeta := mdl.GetDatabases()["mocker_test"]

	for _, blockSize := range []int64{512, 1024, 2048, 5120, 20000, 64000, 131072} {
		c.Log("blockSize = ", blockSize)
		mydump2mysql(c, dbMeta, blockSize)
	}
}

func (s *testMydumpReaderSuite) TestReaderNoTrailingNewLine(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("CREATE DATABASE whatever;"))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name())
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE DATABASE whatever;"))
}

