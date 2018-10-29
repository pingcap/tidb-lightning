package mydump_test

import (
	"bytes"
	"fmt"
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	. "github.com/pingcap/tidb-lightning/lightning/mydump"
)

const (
	defMinRegionSize int64 = 1024 * 4
)

var _ = Suite(&testMydumpRegionSuite{})

type testMydumpRegionSuite struct{}

func (s *testMydumpRegionSuite) SetUpSuite(c *C)    {}
func (s *testMydumpRegionSuite) TearDownSuite(c *C) {}

var expectedTuplesCount = map[string]int64{
	"i":                     1,
	"report_case_high_risk": 1,
	"tbl_autoid":            10000,
	"tbl_multi_index":       10000,
}

/*
	TODO : test with specified 'regionBlockSize' ...
*/
func (s *testMydumpRegionSuite) TestTableRegion(c *C) {
	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./examples"}}
	loader, _ := NewMyDumpLoader(cfg)
	dbMeta := loader.GetDatabases()["mocker_test"]
	founder := NewRegionFounder(defMinRegionSize)

	for _, meta := range dbMeta.Tables {
		regions := founder.MakeTableRegions(meta)

		table := meta.Name
		fmt.Printf("[%s] region count ===============> %d\n", table, len(regions))
		for _, region := range regions {
			fname := filepath.Base(region.File)
			fmt.Printf("[%s] rowID = %5d / rows = %5d / offset = %10d / size = %10d \n",
				fname,
				region.RowIDMin(),
				region.Rows(),
				region.Offset(),
				region.Size())
		}

		// check - region-size vs file-size
		var tolFileSize int64 = 0
		for _, file := range meta.DataFiles {
			fileSize, err := common.GetFileSize(file)
			c.Assert(err, IsNil)
			tolFileSize += fileSize
		}
		// var tolRegionSize int64 = 0
		// for _, region := range regions {
		// 	tolRegionSize += region.Size()
		// }
		// c.Assert(tolRegionSize, Equals, tolFileSize)
		// (The size will not be equal since the comments at the end are omitted)

		// check - rows num
		var tolRows int64 = 0
		for _, region := range regions {
			tolRows += region.Rows()
		}
		c.Assert(tolRows, Equals, expectedTuplesCount[table])

		// check - range
		regionNum := len(regions)
		preReg := regions[0]
		for i := 1; i < regionNum; i++ {
			reg := regions[i]
			if preReg.File == reg.File {
				c.Assert(reg.Offset(), Equals, preReg.Offset()+preReg.Size())
				c.Assert(reg.RowIDMin(), Equals, preReg.RowIDMin()+preReg.Rows())
			} else {
				c.Assert(reg.Offset, Equals, 0)
				c.Assert(reg.RowIDMin(), Equals, 1)
			}
			preReg = reg
		}
	}

	return
}

func (s *testMydumpRegionSuite) TestRegionReader(c *C) {
	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./examples"}}
	loader, _ := NewMyDumpLoader(cfg)
	dbMeta := loader.GetDatabases()["mocker_test"]
	founder := NewRegionFounder(defMinRegionSize)

	for _, meta := range dbMeta.Tables {
		regions := founder.MakeTableRegions(meta)

		tolValTuples := 0
		for _, reg := range regions {
			regReader, _ := NewRegionReader(reg.File, reg.Offset(), reg.Size())
			stmts, _ := regReader.Read(reg.Size())
			for _, stmt := range stmts {
				parts := bytes.Split(stmt, []byte("),"))
				tolValTuples += len(parts)
			}
		}

		c.Assert(int64(tolValTuples), Equals, expectedTuplesCount[meta.Name])
	}

	return
}
