package mydump

import (
	"os"

	"github.com/pkg/errors"
)

type TableRegion struct {
	EngineID int

	DB    string
	Table string
	File  string

	Chunk Chunk
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

func MakeTableRegions(meta *MDTableMeta, columns int, batchSize, batchSizeScale int64) ([]*TableRegion, error) {
	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))

	prevRowIDMax := int64(0)
	curEngineID := 0
	curEngineSize := int64(0)
	curBatchSize := batchSize / batchSizeScale
	for _, dataFile := range meta.DataFiles {
		dataFileInfo, err := os.Stat(dataFile)
		if err != nil {
			return nil, errors.Annotatef(err, "cannot stat %s", dataFile)
		}
		dataFileSize := dataFileInfo.Size()
		rowIDMax := prevRowIDMax + dataFileSize/(int64(columns)+2)
		filesRegions = append(filesRegions, &TableRegion{
			EngineID: curEngineID,
			DB:       meta.DB,
			Table:    meta.Name,
			File:     dataFile,
			Chunk: Chunk{
				Offset:       0,
				EndOffset:    dataFileSize,
				PrevRowIDMax: prevRowIDMax,
				RowIDMax:     rowIDMax,
			},
		})
		prevRowIDMax = rowIDMax

		curEngineSize += dataFileSize
		if curEngineSize > curBatchSize {
			curEngineSize = 0
			curEngineID++

			// import() step will not be concurrent.
			// If multiple Batch end times are close, it will result in multiple
			// Batch import serials. `batchSizeScale` used to implement non-uniform
			// batch size: curBatchSize = batchSize / int64(batchSizeScale-curEngineID)
			if int64(curEngineID) < batchSizeScale {
				curBatchSize = batchSize / (batchSizeScale - int64(curEngineID))
			} else {
				curBatchSize = batchSize
			}
		}
	}

	return filesRegions, nil
}
