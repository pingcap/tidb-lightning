package mydump

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
)

type TableRegion struct {
	ID int

	DB    string
	Table string
	File  string

	Chunk Chunk
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

func MakeTableRegions(meta *MDTableMeta, columns int) ([]*TableRegion, error) {
	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))

	prevRowIDMax := int64(0)
	for i, dataFile := range meta.DataFiles {
		dataFileInfo, err := os.Stat(dataFile)
		if err != nil {
			return nil, errors.Annotatef(err, "cannot stat %s", dataFile)
		}
		dataFileSize := dataFileInfo.Size()
		rowIDMax := prevRowIDMax + dataFileSize/(int64(columns)+2)
		filesRegions = append(filesRegions, &TableRegion{
			ID:    i,
			DB:    meta.DB,
			Table: meta.Name,
			File:  dataFile,
			Chunk: Chunk{
				Offset:       0,
				EndOffset:    dataFileSize,
				PrevRowIDMax: prevRowIDMax,
				RowIDMax:     rowIDMax,
			},
		})
		prevRowIDMax = rowIDMax
	}

	return filesRegions, nil
}
