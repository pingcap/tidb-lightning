package mydump_test

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/ingest/common"
	"github.com/pingcap/tidb-lightning/ingest/config"
	. "github.com/pingcap/tidb-lightning/ingest/mydump"
)

const (
	defMinRegionSize int64 = 1024 * 4
)

var _ = Suite(&testMydumpRegionSuite{})

type testMydumpRegionSuite struct{}

func (s *testMydumpRegionSuite) SetUpSuite(c *C)    {}
func (s *testMydumpRegionSuite) TearDownSuite(c *C) {}

/*
	TODO : test with specified 'fuzzyRegionSize' & 'regionBlockSize' ...
*/
func (s *testMydumpRegionSuite) TestTableRegion(c *C) {
	cfg := &config.Config{SourceDir: "./examples"}
	loader, _ := NewMyDumpLoader(cfg)
	dbMeta := loader.GetDatabase()
	founder := NewRegionFounder(defMinRegionSize)

	for _, meta := range dbMeta.Tables {
		regions := founder.MakeTableRegions(meta)

		// table := meta.Name
		// fmt.Printf("[%s] region count ===============> %d\n", table, len(regions))
		// for _, region := range regions {
		// 	fname := filepath.Base(region.File)
		// 	fmt.Printf("[%s] rowID = %5d / rows = %5d / offset = %10d / size = %10d \n",
		// 		fname, region.BeginRowID, region.Rows, region.Offset, region.Size)
		// }

		// check - region-size vs file-size
		var tolFileSize int64 = 0
		var tolRegionSize int64 = 0
		for _, file := range meta.DataFiles {
			tolFileSize += common.GetFileSize(file)
		}
		for _, region := range regions {
			tolRegionSize += region.Size
		}
		c.Assert(tolRegionSize, Equals, tolFileSize)

		// check - rows num
		var tolRows int64 = 0
		for _, region := range regions {
			tolRows += region.Rows
		}
		c.Assert(tolRows, Equals, int64(10000))

		// check - range
		regionNum := len(regions)
		preReg := regions[0]
		for i := 1; i < regionNum; i++ {
			reg := regions[i]
			if preReg.File == reg.File {
				c.Assert(reg.Offset, Equals, preReg.Offset+preReg.Size)
				c.Assert(reg.BeginRowID, Equals, preReg.BeginRowID+preReg.Rows)
			} else {
				c.Assert(reg.Offset, Equals, 0)
				c.Assert(reg.BeginRowID, Equals, 1)
			}
			preReg = reg
		}
	}

	return
}

func (s *testMydumpRegionSuite) TestRegionReader(c *C) {
	cfg := &config.Config{SourceDir: "./examples"}
	loader, _ := NewMyDumpLoader(cfg)
	dbMeta := loader.GetDatabase()
	founder := NewRegionFounder(defMinRegionSize)

	for _, meta := range dbMeta.Tables {
		regions := founder.MakeTableRegions(meta)

		tolValTuples := 0
		for _, reg := range regions {
			regReader, _ := NewRegionReader(reg.File, reg.Offset, reg.Size)
			stmts, _ := regReader.Read(reg.Size)
			for _, stmt := range stmts {
				parts := bytes.Split(stmt, []byte("),"))
				tolValTuples += len(parts)
			}
		}

		c.Assert(tolValTuples, Equals, 10000)
	}

	return
}
