package mydump_test

import (
	"fmt"
	"os"
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
	. "github.com/pingcap/tidb-lightning/lightning/mydump"
)

var _ = Suite(&testMydumpRegionSuite{})

type testMydumpRegionSuite struct{}

func (s *testMydumpRegionSuite) SetUpSuite(c *C)    {}
func (s *testMydumpRegionSuite) TearDownSuite(c *C) {}

// var expectedTuplesCount = map[string]int64{
// 	"i":                     1,
// 	"report_case_high_risk": 1,
// 	"tbl_autoid":            10000,
// 	"tbl_multi_index":       10000,
// }

func getFileSize(file string) (int64, error) {
	fd, err := os.Open(file)
	if err != nil {
		return -1, err
	}
	defer fd.Close()

	fstat, err := fd.Stat()
	if err != nil {
		return -1, err
	}

	return fstat.Size(), nil
}

/*
	TODO : test with specified 'regionBlockSize' ...
*/
func (s *testMydumpRegionSuite) TestTableRegion(c *C) {
	cfg := &config.Config{Mydumper: config.MydumperRuntime{SourceDir: "./examples"}}
	loader, _ := NewMyDumpLoader(cfg)
	dbMeta := loader.GetDatabases()[0]

	for _, meta := range dbMeta.Tables {
		regions, err := MakeTableRegions(meta, 1, 1, 0, 1)
		c.Assert(err, IsNil)

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
			fileSize, err := getFileSize(file)
			c.Assert(err, IsNil)
			tolFileSize += fileSize
		}
		var tolRegionSize int64 = 0
		for _, region := range regions {
			tolRegionSize += region.Size()
		}
		c.Assert(tolRegionSize, Equals, tolFileSize)

		// // check - rows num
		// var tolRows int64 = 0
		// for _, region := range regions {
		// 	tolRows += region.Rows()
		// }
		// c.Assert(tolRows, Equals, expectedTuplesCount[table])

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
}

func (s *testMydumpRegionSuite) TestAllocateEngineIDs(c *C) {
	dataFileSizes := make([]float64, 700)
	for i := range dataFileSizes {
		dataFileSizes[i] = 1.0
	}
	filesRegions := make([]*TableRegion, 0, len(dataFileSizes))
	for range dataFileSizes {
		filesRegions = append(filesRegions, new(TableRegion))
	}

	checkEngineSizes := func(what string, expected map[int]int) {
		actual := make(map[int]int)
		for _, region := range filesRegions {
			actual[region.EngineID]++
		}
		c.Assert(actual, DeepEquals, expected, Commentf("%s", what))
	}

	// Batch size > Total size => Everything in the zero batch.
	AllocateEngineIDs(filesRegions, dataFileSizes, 1000, 0.5, 1000)
	checkEngineSizes("no batching", map[int]int{
		0: 700,
	})

	// Allocate 3 engines.
	AllocateEngineIDs(filesRegions, dataFileSizes, 200, 0.5, 1000)
	checkEngineSizes("batch size = 200", map[int]int{
		0: 170,
		1: 213,
		2: 317,
	})

	// Allocate 3 engines with an alternative ratio
	AllocateEngineIDs(filesRegions, dataFileSizes, 200, 0.6, 1000)
	checkEngineSizes("batch size = 200, ratio = 0.6", map[int]int{
		0: 160,
		1: 208,
		2: 332,
	})

	// Allocate 5 engines.
	AllocateEngineIDs(filesRegions, dataFileSizes, 100, 0.5, 1000)
	checkEngineSizes("batch size = 100", map[int]int{
		0: 93,
		1: 105,
		2: 122,
		3: 153,
		4: 227,
	})

	// Number of engines > table concurrency
	AllocateEngineIDs(filesRegions, dataFileSizes, 50, 0.5, 4)
	checkEngineSizes("batch size = 50, limit table conc = 4", map[int]int{
		0:  50,
		1:  59,
		2:  73,
		3:  110,
		4:  50,
		5:  50,
		6:  50,
		7:  50,
		8:  50,
		9:  50,
		10: 50,
		11: 50,
		12: 8,
	})

	// Zero ratio = Uniform
	AllocateEngineIDs(filesRegions, dataFileSizes, 100, 0.0, 1000)
	checkEngineSizes("batch size = 100, ratio = 0", map[int]int{
		0: 100,
		1: 100,
		2: 100,
		3: 100,
		4: 100,
		5: 100,
		6: 100,
	})
}
