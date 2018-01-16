package mydump_test

import (
	"database/sql"
	"fmt"
	"io"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/ingest/common"
	"github.com/pingcap/tidb-lightning/ingest/config"
	. "github.com/pingcap/tidb-lightning/ingest/mydump"
)

const (
	utestDB         string = "_mydump_reader_utest_"
	utestDataSrouce string = "./examples"
)

var _ = Suite(&testMydumpReaderSuite{})

////////////////////////////
type dbManager struct {
	database string
	db       *sql.DB
}

func newDBManager() *dbManager {
	mgr := &dbManager{
		database: utestDB,
		db:       common.ConnectDB("localhost", 3306, "root", ""),
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
	dbMgr := newDBManager()
	defer dbMgr.clear().close()

	db := dbMgr.db
	dbMgr.clear()
	for _, tblMeta := range dbMeta.Tables {
		sqlCreteTable, _ := ExportStatment(tblMeta.SchemaFile)
		dbMgr.init(string(sqlCreteTable))

		for _, file := range tblMeta.DataFiles {
			reader, _ := NewMDDataReader(file, 0)
			defer reader.Close()

			for {
				statments, err := reader.Read(minBlockSize)
				if err == io.EOF {
					break
				}
				for _, stmt := range statments {
					_, err = db.Exec(string(stmt))
					c.Assert(err, IsNil)
				}
			}
			c.Assert(reader.Tell(), Equals, common.GetFileSize(file))
		}
	}

	checkTableData(c, db)
	return
}

func (s *testMydumpReaderSuite) TestReader(c *C) {
	fmt.Println("Testing mydump reader ...")

	cfg := &config.Config{SourceDir: utestDataSrouce}
	mdl, _ := NewMyDumpLoader(cfg)
	dbMeta := mdl.GetDatabase()

	var minSize int64 = 512
	var maxSize int64 = 1024 * 128

	start := time.Now()
	for blockSize := minSize; blockSize <= maxSize; blockSize += 512 {
		mydump2mysql(c, dbMeta, blockSize)
	}
	fmt.Printf("tol cost = %.2f sec\n", time.Since(start).Seconds())
}
