package mydump

import (
	"io"
	"os"
	"strconv"

	"github.com/pingcap/tidb/types"

	. "github.com/pingcap/check"
	"github.com/xitongsys/parquet-go-source/local"
	writer2 "github.com/xitongsys/parquet-go/writer"
)

type testParquetParserSuite struct{}

var _ = Suite(testParquetParserSuite{})

func (s testParquetParserSuite) TestParquetParser(c *C) {
	type Test struct {
		S string `parquet:"name=s, type=UTF8, encoding=PLAIN_DICTIONARY"`
		A int32  `parquet:"name=a, type=INT32"`
	}

	// prepare data
	name := "test123.parquet"
	f, err := os.Create(name)
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)
	defer os.Remove(name)
	pf, err := local.NewLocalFileWriter(name)
	c.Assert(err, IsNil)
	test := &Test{}
	writer, err := writer2.NewParquetWriter(pf, test, 2)
	c.Assert(err, IsNil)

	for i := 0; i < 100; i++ {
		test.A = int32(i)
		test.S = strconv.Itoa(i)
		c.Assert(writer.Write(test), IsNil)
	}

	c.Assert(writer.WriteStop(), IsNil)
	c.Assert(pf.Close(), IsNil)

	reader, err := NewParquetParser(name)
	c.Assert(err, IsNil)
	defer reader.Close()

	c.Assert(reader.Columns(), DeepEquals, []string{"s", "a"})

	verifyRow := func(i int) {
		c.Assert(reader.lastRow.RowID, Equals, int64(i+1))
		c.Assert(len(reader.lastRow.Row), Equals, 2)
		c.Assert(reader.lastRow.Row[0], DeepEquals, types.NewCollationStringDatum(strconv.Itoa(i), "", 0))
		c.Assert(reader.lastRow.Row[1], DeepEquals, types.NewIntDatum(int64(i)))
	}

	// test read some rows
	for i := 0; i < 10; i++ {
		c.Assert(reader.ReadRow(), IsNil)
		verifyRow(i)
	}

	// test set pos to pos < curpos + batchReadRowSize
	reader.SetPos(15, 15)
	c.Assert(reader.ReadRow(), IsNil)
	verifyRow(15)

	// test set pos to pos > curpos + batchReadRowSize
	reader.SetPos(80, 80)
	for i := 80; i < 100; i++ {
		c.Assert(reader.ReadRow(), IsNil)
		verifyRow(i)
	}

	c.Assert(reader.ReadRow(), Equals, io.EOF)
}
