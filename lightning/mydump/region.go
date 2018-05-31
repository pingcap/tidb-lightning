package mydump

import (
	"fmt"
	"io"
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

	Offset int64
	Size   int64
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

func (f *RegionFounder) MakeTableRegions(meta *MDTableMeta) []*TableRegion {
	var lock sync.Mutex
	var wg sync.WaitGroup

	db := meta.DB
	table := meta.Name
	processors := f.processors
	minRegionSize := f.minRegionSize

	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	for _, dataFile := range meta.DataFiles {
		wg.Add(1)
		go func(pid int, file string) {
			log.Debugf("[%s] loading file's region (%s) ...", table, file)

			var regions []*TableRegion
			regions = splitFuzzyRegion(db, table, file, minRegionSize)

			lock.Lock()
			filesRegions = append(filesRegions, regions...)
			lock.Unlock()

			processors <- pid
			wg.Done()
		}(<-processors, dataFile)
	}
	wg.Wait()

	// Setup files' regions
	sort.Sort(filesRegions) // ps : sort region by - (fileName, fileOffset)
	for i, region := range filesRegions {
		region.ID = i
	}

	return filesRegions
}

func splitFuzzyRegion(db string, table string, file string, minRegionSize int64) []*TableRegion {
	reader, err := NewMDDataReader(file, 0)
	if err != nil {
		log.Errorf("failed to generate file's regions  (%s) : %s", file, err.Error())
		return nil
	}
	defer reader.Close()

	newRegion := func(off int64) *TableRegion {
		return &TableRegion{
			ID:     -1,
			DB:     db,
			Table:  table,
			File:   file,
			Offset: off,
			Size:   0,
		}
	}

	regions := make([]*TableRegion, 0)

	var extendSize = int64(4 << 10) // 4 K
	var offset int64
	for {
		reader.Seek(offset + minRegionSize)
		_, err := reader.Read(extendSize)
		pos := reader.Tell()

		region := newRegion(offset)
		region.Size = pos - offset
		if region.Size > 0 {
			regions = append(regions, region)
		}

		if errors.Cause(err) == io.EOF {
			break
		}
		offset = pos
	}

	return regions
}
