package datasource

import (
	"runtime"
	"sort"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
	log "github.com/sirupsen/logrus"
)

////////////////////////////////////////////////////////////////

type RegionFounder struct {
	processors    chan int
	minRegionSize int64
}

func NewRegionFounder(minRegionSize int64) *RegionFounder {
	concurrency := runtime.NumCPU() >> 1
	if concurrency == 0 {
		concurrency = 1
	}

	processors := make(chan int, concurrency)
	for i := 0; i < concurrency; i++ {
		processors <- i
	}

	return &RegionFounder{
		processors:    processors,
		minRegionSize: minRegionSize,
	}
}

func (f *RegionFounder) MakeTableRegions(meta *MDTableMeta, sourceType string) ([]*base.TableRegion, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup

	db := meta.DB
	table := meta.Name
	processors := f.processors
	minRegionSize := f.minRegionSize

	errChs := make(chan error, len(meta.DataFiles))

	// Split files into regions
	filesRegions := make(base.RegionSlice, 0, len(meta.DataFiles))
	for _, dataFile := range meta.DataFiles {
		wg.Add(1)
		go func(pid int, file string) {
			defer wg.Done()
			log.Debugf("[%s] loading file's region (%s) ...", table, file)

			regions, err := splitFuzzyRegion(sourceType, db, table, file, minRegionSize)
			if err != nil {
				errChs <- errors.Trace(err)
				return
			}
			lock.Lock()
			filesRegions = append(filesRegions, regions...)
			lock.Unlock()
			processors <- pid
			errChs <- nil
		}(<-processors, dataFile)
	}
	wg.Wait()
	close(errChs)
	for err := range errChs {
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Setup files' regions
	sort.Sort(filesRegions) // ps : sort region by - (fileName, fileOffset)
	for i, region := range filesRegions {
		region.ID = i
	}

	return filesRegions, nil
}

func splitFuzzyRegion(sourceType string, db string, table string, file string, minRegionSize int64) ([]*base.TableRegion, error) {
	reader, err := NewDataReader(sourceType, db, table, file, 0)
	if err != nil {
		log.Errorf("failed to generate file's regions  (%s) : %s", file, err.Error())
		return nil, nil
	}
	defer reader.Close()

	regions, err := reader.SplitRegions(minRegionSize)
	return regions, errors.Trace(err)

}
