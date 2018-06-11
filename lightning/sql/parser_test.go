package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pkg/errors"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	. "github.com/pingcap/tidb-lightning/lightning/mydump"
	. "github.com/pingcap/tidb-lightning/lightning/sql"
)

var _ = Suite(&testParserSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

type testParserSuite struct{}

func (s *testParserSuite) SetUpSuite(c *C)    {}
func (s *testParserSuite) TearDownSuite(c *C) {}

func (s *testParserSuite) TestBasicParse(c *C) {
	sql := `/*!40101 SET NAMES binary*/;
INSERT INTO ` + "`tbl_autoid`" + ` VALUES (1,"0-0-0"), (2,"0-0-1"),
(3,"0-0-2"),
(4,  "0-0-3" ) , (100,     "HELLO
World" ),
 ( 1 ),(2);`

	values := make([]interface{}, 0, 1024)
	ParseInsertStmt([]byte(sql), &values)

	c.Assert(len(values), Equals, 12)

	for _, val := range values {
		v, _ := val.(string)
		for _, ch := range []string{"(", ")", "\"", ",", " "} {
			idx := strings.Index(v, ch)
			c.Assert(idx, Equals, -1)
		}
	}
}

////////////////////////////////////////////////////////////

type storage struct {
	database string
	db       *sql.DB
}

func newStorage() *storage {
	database := "_test_parser_"
	db := common.ConnectDB("localhost", 3306, "root", "")
	db.Exec("create database if not exists " + database)
	db.Exec("use " + database)

	return &storage{
		database: database,
		db:       db,
	}
}

func (s *storage) close() {
	s.db.Exec("drop database " + s.database)
	s.db.Close()
}

func (s *storage) init(schema string) {
	s.db.Exec(schema)
}

func (s *storage) verifyDistinctCount(c *C, table string, column string, expectCount int) {
	sql := fmt.Sprintf("select count(distinct %s) cnt from `%s`", column, table)
	count := 0
	s.db.QueryRow(sql).Scan(&count)
	c.Assert(count, Equals, expectCount)
}

func countColumns(stmt []byte) int {
	colsNum := 0xfffff
	for i := 0; i < 10; i++ {
		s := bytes.Index(stmt, []byte("("))
		e := bytes.Index(stmt, []byte(")"))
		if s > 0 && e > 0 && e > s {
			cols := bytes.Count(stmt[s:e], []byte(",")) + 1
			if cols < colsNum {
				colsNum = cols
			}
			stmt = stmt[e+1:]
		}
	}
	return colsNum
}

func makePrepareStatement(sql []byte) (int, int, string) {
	// count rows
	sql = bytes.TrimSpace(sql)
	rowsNum := bytes.Count(sql, []byte("),")) + bytes.Count(sql, []byte(");"))
	if sql[len(sql)-1] == ')' {
		rowsNum += 1
	}

	// count columns --> (?,?,?,?)
	colsNum := countColumns(sql)
	stmtVals := ""
	if colsNum > 1 {
		stmtVals = strings.Repeat("?,", colsNum-1)
	}
	stmtVals = fmt.Sprintf("(%s?)", stmtVals)

	//	join statement
	var buffer bytes.Buffer

	end := bytes.Index(sql, []byte("("))
	buffer.WriteString(string(sql[:end])) // "INSERT ... VALUES "

	for i := 0; i < rowsNum; i++ {
		buffer.WriteString(stmtVals)
		if i != rowsNum-1 {
			buffer.WriteString(",")
		}
	}

	statement := buffer.String()
	return rowsNum, colsNum, statement
}

func sql2storage(c *C, sql []byte, store *storage) {
	rows, cols, stmtSQL := makePrepareStatement(sql)
	stmt, err := store.db.Prepare(stmtSQL)
	c.Assert(err, IsNil)

	values := make([]interface{}, 0, 1024)
	err = ParseInsertStmt(sql, &values)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, rows*cols)

	r, err := stmt.Exec(values...)
	c.Assert(err, IsNil)
	n, err := r.RowsAffected()
	c.Assert(n, Equals, int64(rows))
}

func (s *testParserSuite) testParseRealFile(c *C) {
	store := newStorage()
	defer store.close()

	cfg := &config.Config{SourceDir: "../mydump/examples"}
	loader := NewMyDumpLoader(cfg)

	dbMeta := loader.GetTree()
	for _, tblMeta := range dbMeta.Tables {
		sqlCreteTable, _ := ExportStatment(tblMeta.SchemaFile)
		store.init(string(sqlCreteTable))

		// read from file
		for _, file := range tblMeta.DataFiles {
			reader, _ := NewMDDataReader(file, 0)
			defer reader.Close()

			for {
				statments, err := reader.Read(4 * 1024)
				if errors.Cause(err) == io.EOF {
					break
				}

				for _, sql := range statments {
					sql2storage(c, sql, store)
				}
			}
		}
	}

	store.verifyDistinctCount(c, "tbl_autoid", "ID", 10000)
	store.verifyDistinctCount(c, "tbl_multi_index", "Name", 10000)

	return
}
