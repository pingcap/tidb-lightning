package mydump

import (
	"context"
	"io"
	"path/filepath"
	"strconv"

	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
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

func (s testParquetParserSuite) TestParquetVariousTypes(c *C) {
	type Test struct {
		Date            int32 `parquet:"name=date, type=DATE"`
		TimeMillis      int32 `parquet:"name=timemillis, type=TIME_MILLIS"`
		TimeMicros      int64 `parquet:"name=timemicros, type=TIME_MICROS"`
		TimestampMillis int64 `parquet:"name=timestampmillis, type=TIMESTAMP_MILLIS"`
		TimestampMicros int64 `parquet:"name=timestampmicros, type=TIMESTAMP_MICROS"`

		Decimal1 int32  `parquet:"name=decimal1, type=DECIMAL, scale=2, precision=9, basetype=INT32"`
		Decimal2 int32  `parquet:"name=decimal2, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
		Decimal3 int64  `parquet:"name=decimal3, type=DECIMAL, scale=2, precision=18, basetype=INT64"`
		Decimal4 string `parquet:"name=decimal4, type=DECIMAL, scale=2, precision=10, basetype=FIXED_LEN_BYTE_ARRAY, length=12"`
		Decimal5 string `parquet:"name=decimal5, type=DECIMAL, scale=2, precision=20, basetype=BYTE_ARRAY"`
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

	v := &Test{
		Date:            18564,              //2020-10-29
		TimeMillis:      62775123,           // 17:26:15.123
		TimeMicros:      62775123000,        // 17:26:15.123
		TimestampMillis: 1603963672356,      // 2020-10-29T17:27:52.356
		TimestampMicros: 1603963672356956,   //2020-10-29T17:27:52.356956
		Decimal1:        -12345678,          // -123456.78
		Decimal2:        456,                // 0.0456
		Decimal3:        123456789012345678, //1234567890123456.78
		Decimal4:        "-12345678.09",
		Decimal5:        "-1234567890123456.78",
	}
	c.Assert(writer.Write(v), IsNil)
	c.Assert(writer.WriteStop(), IsNil)
	c.Assert(pf.Close(), IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	r, err := store.Open(context.TODO(), name)
	c.Assert(err, IsNil)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	c.Assert(err, IsNil)
	defer reader.Close()

	c.Assert(len(reader.columns), Equals, 10)

	c.Assert(reader.ReadRow(), IsNil)
	c.Assert(reader.lastRow.Row, DeepEquals, []types.Datum{
		types.NewStringDatum("2020-10-29"),
		types.NewStringDatum("17:26:15.123"),
		types.NewStringDatum("17:26:15.123"),
		types.NewStringDatum("2020-10-29 17:27:52.356"),
		types.NewStringDatum("2020-10-29 17:27:52.356"),
		types.NewStringDatum("-123456.78"),
		types.NewStringDatum("0.0456"),
		types.NewStringDatum("1234567890123456.78"),
		types.NewStringDatum("-12345678.09"),
		types.NewStringDatum("-1234567890123456.78"),
	})
}
