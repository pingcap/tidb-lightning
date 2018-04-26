package datasource

import (
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type TableRegion struct {
	ID int

	DB    string
	Table string
	File  string

	Offset     int64
	Size       int64
	SourceType string
}

func (reg *TableRegion) Name() string {
	return fmt.Sprintf("%s|%s|%d|%d",
		reg.DB, reg.Table, reg.ID, reg.Offset)
}

type regionSlice []*TableRegion

func (rs regionSlice) Len() int {
	return len(rs)
}
func (rs regionSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs regionSlice) Less(i, j int) bool {
	if rs[i].File == rs[j].File {
		return rs[i].Offset < rs[j].Offset
	}
	return rs[i].File < rs[j].File
}

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

func (f *RegionFounder) MakeTableRegions(meta *MDTableMeta, sourceType string) ([]*TableRegion, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup

	db := meta.DB
	table := meta.Name
	processors := f.processors
	minRegionSize := f.minRegionSize

	errChs := make(chan error, len(meta.DataFiles))

	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
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

func splitFuzzyRegion(sourceType string, db string, table string, file string, minRegionSize int64) ([]*TableRegion, error) {
	reader, err := NewDataReader(sourceType, db, table, file, 0)
	if err != nil {
		log.Errorf("failed to generate file's regions  (%s) : %s", file, err.Error())
		return nil, nil
	}
	defer reader.Close()

	regions, err := reader.SplitRegions(minRegionSize)
	return regions, errors.Trace(err)

}
