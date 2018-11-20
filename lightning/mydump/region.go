package mydump

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pkg/errors"
)

type TableRegion struct {
	ID int

	DB    string
	Table string
	File  string

	Columns []byte
	Chunk   Chunk
}

func (reg *TableRegion) Name() string {
	return fmt.Sprintf("%s|%s|%d|%d",
		reg.DB, reg.Table, reg.ID, reg.Chunk.Offset)
}

func (reg *TableRegion) RowIDMin() int64 {
	return reg.Chunk.PrevRowIDMax + 1
}
func (reg *TableRegion) Rows() int64 {
	return reg.Chunk.RowIDMax - reg.Chunk.PrevRowIDMax
}
func (reg *TableRegion) Offset() int64 {
	return reg.Chunk.Offset
}
func (reg *TableRegion) Size() int64 {
	return reg.Chunk.EndOffset - reg.Chunk.Offset
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
		return rs[i].Chunk.Offset < rs[j].Chunk.Offset
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

func (f *RegionFounder) MakeTableRegions(meta *MDTableMeta) ([]*TableRegion, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup

	db := meta.DB
	table := meta.Name
	processors := f.processors
	minRegionSize := f.minRegionSize

	var chunkErr error

	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	for _, dataFile := range meta.DataFiles {
		wg.Add(1)
		go func(pid int, file string) {
			common.AppLogger.Debugf("[%s] loading file's region (%s) ...", table, file)

			chunks, err := splitExactChunks(db, table, file, minRegionSize)
			lock.Lock()
			if err == nil {
				filesRegions = append(filesRegions, chunks...)
			} else {
				chunkErr = errors.Annotatef(err, "%s", file)
				common.AppLogger.Errorf("failed to extract chunks from file: %v", chunkErr)
			}
			lock.Unlock()

			processors <- pid
			wg.Done()
		}(<-processors, dataFile)
	}
	wg.Wait()

	if chunkErr != nil {
		return nil, chunkErr
	}

	// Setup files' regions
	sort.Sort(filesRegions) // ps : sort region by - (fileName, fileOffset)
	var totalRowCount int64
	for i, region := range filesRegions {
		region.ID = i

		// Every chunk's PrevRowIDMax was uninitialized (set to 0). We need to
		// re-adjust the row IDs so they won't be overlapping.
		chunkRowCount := region.Chunk.RowIDMax - region.Chunk.PrevRowIDMax
		region.Chunk.PrevRowIDMax = totalRowCount
		totalRowCount += chunkRowCount
		region.Chunk.RowIDMax = totalRowCount
	}

	return filesRegions, nil
}

func splitExactChunks(db string, table string, file string, minChunkSize int64) ([]*TableRegion, error) {
	reader, err := os.Open(file)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer reader.Close()

	parser := NewChunkParser(reader)
	chunks, err := parser.ReadChunks(minChunkSize)
	if err != nil {
		return nil, errors.Trace(err)
	}

	annotatedChunks := make([]*TableRegion, len(chunks))
	for i, chunk := range chunks {
		annotatedChunks[i] = &TableRegion{
			ID:      -1,
			DB:      db,
			Table:   table,
			File:    file,
			Columns: parser.Columns,
			Chunk:   chunk,
		}
	}

	return annotatedChunks, nil
}
