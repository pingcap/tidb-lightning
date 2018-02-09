package mydump

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/ngaut/log"
)

type TableRegion struct {
	ID int

	DB    string
	Table string
	File  string

	Offset     int64
	Size       int64
	BeginRowID int64
	Rows       int64
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
	} else {
		return rs[i].File < rs[j].File
	}
}

////////////////////////////////////////////////////////////////

type RegionFounder struct {
	processors    chan int
	minRegionSize int64
}

func NewRegionFounder(minRegionSize int64) *RegionFounder {
	concurrency := runtime.NumCPU() / 4
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

	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	for _, file := range meta.DataFiles {
		wg.Add(1)
		go func(pid int, f string) {
			log.Debugf("[%s] loading file's region (%s) ...", table, f)
			regions := makeFileRegions(db, table, f, minRegionSize)

			lock.Lock()
			filesRegions = append(filesRegions, regions...)
			lock.Unlock()

			processors <- pid
			wg.Done()
		}(<-processors, file)
	}

	wg.Wait()
	sort.Sort(filesRegions)

	var tableRows int64 = 0
	for i, region := range filesRegions {
		region.ID = i
		region.BeginRowID = tableRows + 1
		tableRows += region.Rows
	}

	return filesRegions
}

func makeFileRegions(db string, table string, file string, minRegionSize int64) []*TableRegion {
	reader, err := NewMDDataReader(file, 0)
	if err != nil {
		log.Errorf("failed to generate file's regions  (%s) : %s", file, err.Error())
		return nil
	}
	defer reader.Close()

	var offset int64
	var readSize int64

	newRegion := func(off int64) *TableRegion {
		return &TableRegion{
			ID:         -1,
			DB:         db,
			Table:      table,
			File:       file,
			Offset:     off,
			BeginRowID: 0,
			Size:       0,
			Rows:       0,
		}
	}

	blockSize := defReadBlockSize
	if blockSize > minRegionSize {
		blockSize = minRegionSize
	}

	regions := make([]*TableRegion, 0)
	region := newRegion(0)
	for {
		// read file content
		statements, err := reader.Read(blockSize)
		if err == io.EOF {
			break
		}
		readSize = reader.Tell() - offset
		offset = reader.Tell()

		// update region status
		region.Size += readSize
		for _, stmt := range statements {
			region.Rows += int64(countValues(stmt))
		}

		// generate new region once is necessary ~
		if region.Size >= minRegionSize {
			regions = append(regions, region)
			region = newRegion(offset)
		}
	}

	// finally
	if region.Size > 0 {
		regions = append(regions, region)
	}

	return regions
}
