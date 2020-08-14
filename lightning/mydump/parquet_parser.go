package mydump

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb/types"
	preader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

const (
	batchReadRowSize = 256
)

type ParquetParser struct {
	Reader   *preader.ParquetReader
	columns  []string
	rows     []interface{}
	readRows int64
	curStart int64
	curIndex int
	lastRow  Row
	logger   log.Logger
}

type ReaderWrapper struct {
	ReadSeekCloser
}

func (r *ReaderWrapper) Write(p []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (r *ReaderWrapper) Open(name string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}
func (r *ReaderWrapper) Create(name string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}

func NewParquetParser(path string) (*ParquetParser, error) {
	r, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//wrapper := &ReaderWrapper{r}
	reader, err := preader.NewParquetReader(r, nil, 2)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columns := make([]string, 0, len(reader.Footer.Schema))
	for _, c := range reader.Footer.Schema {
		if c.GetNumChildren() == 0 {
			columns = append(columns, strings.ToLower(c.Name))
		}

	}

	//reader.GetNumRows()

	return &ParquetParser{
		Reader:  reader,
		columns: columns,
		logger:  log.L(),
	}, nil
}

// Pos returns the currently row number of the parquet file
func (pp *ParquetParser) Pos() (pos int64, rowID int64) {
	return pp.curStart + int64(pp.curIndex), pp.lastRow.RowID
}

func (pp *ParquetParser) SetPos(pos int64, rowID int64) {
	if pos < pp.curStart {
		panic("don't support seek back yet")
	}
	pp.lastRow.RowID = rowID

	if pos < pp.curStart+int64(len(pp.rows)) {
		pp.curIndex = int(pos - pp.curStart)
		pp.readRows = pos
		return
	}

	if pos > pp.curStart+int64(len(pp.rows)) {
		pp.Reader.SkipRows(pos - pp.curStart - int64(len(pp.rows)))
	}
	pp.curStart = pos
	pp.readRows = pos
	pp.curIndex = 0
	if len(pp.rows) > 0 {
		pp.rows = pp.rows[:0]
	}
}

func (pp *ParquetParser) Close() error {
	return pp.Reader.PFile.Close()
}

func (pp *ParquetParser) ReadRow() error {
	pp.lastRow.RowID++
	if pp.curIndex >= len(pp.rows) {
		if pp.readRows >= pp.Reader.GetNumRows() {
			return io.EOF
		}
		count := batchReadRowSize
		if pp.Reader.GetNumRows()-pp.readRows < int64(count) {
			count = int(pp.Reader.GetNumRows() - pp.readRows)
		}

		var err error
		var rows []interface{}
		//if len(pp.rows) < count {
		//	rows, err = pp.Reader.ReadByNumber(count)
		//	pp.rows = rows
		//} else {
		//	pp.rows = pp.rows[:count]
		//	err = pp.Reader.Read(&pp.rows)
		//}
		rows, err = pp.Reader.ReadByNumber(count)
		pp.rows = rows
		if err != nil {
			return errors.Trace(err)
		}
		pp.curStart = pp.readRows
		pp.readRows += int64(len(pp.rows))
		pp.curIndex = 0
	}

	row := pp.rows[pp.curIndex]
	pp.curIndex++

	v := reflect.ValueOf(row)
	length := v.NumField()
	if cap(pp.lastRow.Row) < length {
		pp.lastRow.Row = make([]types.Datum, length)
	} else {
		pp.lastRow.Row = pp.lastRow.Row[:length]
	}
	for i := 0; i < length; i++ {
		setDatumValue(&pp.lastRow.Row[i], v.Field(i))
	}
	return nil
}

func setDatumValue(d *types.Datum, v reflect.Value) {
	switch v.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d.SetUint64(v.Uint())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		d.SetInt64(v.Int())
	case reflect.String:
		d.SetString(v.String(), "")
	case reflect.Float32, reflect.Float64:
		d.SetFloat64(v.Float())
	case reflect.Ptr:
		if v.IsNil() {
			d.SetNull()
		} else {
			setDatumValue(d, v.Elem())
		}
	default:
		panic(fmt.Sprintf("unknow value, kind: %d, type: %s, value: %s", v.Kind(), v.Type(), v.Interface()))
	}
}

func (pp *ParquetParser) LastRow() Row {
	return pp.lastRow
}

func (pp *ParquetParser) RecycleRow(row Row) {

}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (pp *ParquetParser) Columns() []string {
	return pp.columns
}

// SetColumns set restored column names to parser
func (pp *ParquetParser) SetColumns(cols []string) {
	// just do nothing
}

func (pp *ParquetParser) SetLogger(l log.Logger) {
	pp.logger = l
}
