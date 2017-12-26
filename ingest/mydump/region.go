package mydump

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/ngaut/log"
)

const (
	// TODO ....
	fuzzyRegionSize int64 = 1024 * 64
	// fuzzyRegionSize int64 = 1024 * 1024 * 256
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
	processors chan int
}

func NewRegionFounder() *RegionFounder {
	concurrency := runtime.NumCPU() / 4
	if concurrency == 0 {
		concurrency = 1
	}

	processors := make(chan int, concurrency)
	for i := 0; i < concurrency; i++ {
		processors <- i
	}

	return &RegionFounder{
		processors: processors,
	}
}

func (f *RegionFounder) MakeTableRegions(meta *MDTableMeta) []*TableRegion {
	var lock sync.Mutex
	var wg sync.WaitGroup

	db := meta.DB
	table := meta.Name
	processors := f.processors

	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	for _, file := range meta.DataFiles {
		wg.Add(1)
		go func(pid int, f string) {
			log.Infof("[%s] loading file's region (%s) ...", table, f)
			regions := makeFileRegions(db, table, f)

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
	tableRegions := make([]*TableRegion, 0, len(filesRegions))
	for i, region := range filesRegions {
		region.ID = i
		region.BeginRowID = tableRows + 1
		tableRows += region.Rows
		tableRegions = append(tableRegions, region)
	}

	return tableRegions
}

func makeFileRegions(db string, table string, file string) []*TableRegion {
	reader, _ := NewMDDataReader(file, 0) // TODO ... error
	defer reader.Close()

	var offset int64 = reader.Tell()
	var readSize int64
	var blockSize int64 = 1024 * 8 // TODO ...

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

	regions := make([]*TableRegion, 0)
	region := newRegion(offset)
	for {
		// read file content
		statments, err := reader.Read(blockSize)
		if err == io.EOF {
			break
		}
		readSize = reader.Tell() - offset
		offset = reader.Tell()

		// update region status
		region.Size += readSize
		for _, stmt := range statments {
			region.Rows += int64(countValues(stmt))
		}

		// generate new region once is necessary ~
		if region.Size >= fuzzyRegionSize {
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
