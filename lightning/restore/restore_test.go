package restore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/hack"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
)

var _ = Suite(&restoreSuite{})

type restoreSuite struct{}

func (s *restoreSuite) TestNewTableRestore(c *C) {
	testCases := []struct {
		name       string
		createStmt string
		errRegexp  string
	}{
		{"t1", "CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", ""},
		{"t2", "CREATE TABLE `t2` (`c1` varchar(30000) NOT NULL)", "failed to ExecDDLSQL `mockdb`.`t2`:.*"},
	}

	dbInfo := &TidbDBInfo{Name: "mockdb", Tables: map[string]*TidbTableInfo{}}
	for _, c := range testCases {
		dbInfo.Tables[c.name] = &TidbTableInfo{
			Name:            c.name,
			CreateTableStmt: c.createStmt,
		}
	}

	for _, tc := range testCases {
		tableInfo := dbInfo.Tables[tc.name]
		tableName := common.UniqueTable("mockdb", tableInfo.Name)
		tr, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &TableCheckpoint{})
		if tc.errRegexp != "" {
			c.Assert(err, ErrorMatches, tc.errRegexp)
		} else {
			c.Assert(tr, NotNil)
			c.Assert(err, IsNil)
		}
	}
}

func BenchmarkChunkRestoreReuseBuffer(b *testing.B) {
	sql, err := ioutil.ReadFile("../kv/testdata/schr.s11.1.sql")
	if err != nil {
		b.Fatalf("read sql data: %v", err)
	}
	endOff := int64(len(sql))
	tableName := "s11"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(sql)
		parser := mydump.NewChunkParser(reader)
		for {
			endOffset := mathutil.MinInt64(endOff, parser.Pos()+config.ReadBlockSize)
			if parser.Pos() >= endOffset {
				break
			}
			var sqls strings.Builder
			sqls.WriteString("INSERT INTO ")
			sqls.WriteString(tableName)
			sqls.WriteString(" VALUES")
			var sep byte = ' '
			for parser.Pos() < endOffset {
				err := parser.ReadRow()
				switch errors.Cause(err) {
				case nil:
					sqls.WriteByte(sep)
					sep = ','
					lastRow := parser.LastRow()
					sqls.Write(lastRow.Row[:len(lastRow.Row)-1])
					fmt.Fprintf(&sqls, ",%d)", lastRow.RowID)
				case io.EOF:
					parser.SetPos(endOff, 0)
				default:
					b.Fatalf("read row: %v", err)
				}
			}
			if sep != ',' { // quick and dirty way to check if `sqls` actually contained any values
				continue
			}
			sqls.WriteByte(';')
			sqls.String()
		}
	}
}

func BenchmarkChunkRestoreReuseBuffer2(b *testing.B) {
	sql, err := ioutil.ReadFile("../kv/testdata/schr.s11.1.sql")
	if err != nil {
		b.Fatalf("read sql data: %v", err)
	}
	endOff := int64(len(sql))
	tableName := "s11"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(sql)
		parser := mydump.NewChunkParser(reader)
		var sqls bytes.Buffer
		for {
			endOffset := mathutil.MinInt64(endOff, parser.Pos()+config.ReadBlockSize)
			if parser.Pos() >= endOffset {
				break
			}
			sqls.Reset()
			sqls.WriteString("INSERT INTO ")
			sqls.WriteString(tableName)
			sqls.WriteString(" VALUES")
			var sep byte = ' '
			for parser.Pos() < endOffset {
				err := parser.ReadRow()
				switch errors.Cause(err) {
				case nil:
					sqls.WriteByte(sep)
					sep = ','
					lastRow := parser.LastRow()
					sqls.Write(lastRow.Row[:len(lastRow.Row)-1])
					fmt.Fprintf(&sqls, ",%d)", lastRow.RowID)
				case io.EOF:
					parser.SetPos(endOff, 0)
				default:
					b.Fatalf("read row: %v", err)
				}
			}
			if sep != ',' { // quick and dirty way to check if `sqls` actually contained any values
				continue
			}
			sqls.WriteByte(';')
			sqls.String()
		}
	}
}

func BenchmarkChunkRestoreReuseBuffer3(b *testing.B) {
	sql, err := ioutil.ReadFile("../kv/testdata/schr.s11.1.sql")
	if err != nil {
		b.Fatalf("read sql data: %v", err)
	}
	endOff := int64(len(sql))
	tableName := "s11"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(sql)
		parser := mydump.NewChunkParser(reader)
		var sqls bytes.Buffer
		for {
			endOffset := mathutil.MinInt64(endOff, parser.Pos()+config.ReadBlockSize)
			if parser.Pos() >= endOffset {
				break
			}
			sqls.Reset()
			sqls.WriteString("INSERT INTO ")
			sqls.WriteString(tableName)
			sqls.WriteString(" VALUES")
			var sep byte = ' '
			for parser.Pos() < endOffset {
				err := parser.ReadRow()
				switch errors.Cause(err) {
				case nil:
					sqls.WriteByte(sep)
					sep = ','
					lastRow := parser.LastRow()
					sqls.Write(lastRow.Row[:len(lastRow.Row)-1])
					fmt.Fprintf(&sqls, ",%d)", lastRow.RowID)
				case io.EOF:
					parser.SetPos(endOff, 0)
				default:
					b.Fatalf("read row: %v", err)
				}
			}
			if sep != ',' { // quick and dirty way to check if `sqls` actually contained any values
				continue
			}
			sqls.WriteByte(';')
			hack.String(sqls.Bytes())
		}
	}
}
