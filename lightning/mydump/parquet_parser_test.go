package mydump

import (
	"context"
	"io"
	"path/filepath"
	"strconv"

	"github.com/pingcap/br/pkg/storage"

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

	dir := c.MkDir()
	// prepare data
	name := "test123.parquet"
	testPath := filepath.Join(dir, name)
	pf, err := local.NewLocalFileWriter(testPath)
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

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	r, err := store.Open(context.TODO(), name)
	c.Assert(err, IsNil)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
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
	c.Assert(reader.SetPos(15, 15), IsNil)
	c.Assert(reader.ReadRow(), IsNil)
	verifyRow(15)

	// test set pos to pos > curpos + batchReadRowSize
	c.Assert(reader.SetPos(80, 80), IsNil)
	for i := 80; i < 100; i++ {
		c.Assert(reader.ReadRow(), IsNil)
		verifyRow(i)
	}

	c.Assert(reader.ReadRow(), Equals, io.EOF)
}
