// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"math"
	"os"
	"strings"

	"github.com/pingcap/errors"
)

type TableRegion struct {
	EngineID int32

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

func AllocateEngineIDs(
	filesRegions []*TableRegion,
	dataFileSizes []float64,
	batchSize float64,
	batchImportRatio float64,
	tableConcurrency float64,
) {
	totalDataFileSize := 0.0
	for _, dataFileSize := range dataFileSizes {
		totalDataFileSize += dataFileSize
	}

	// No need to batch if the size is too small :)
	if totalDataFileSize <= batchSize {
		return
	}

	curEngineID := int32(0)
	curEngineSize := 0.0
	curBatchSize := batchSize

	// import() step will not be concurrent.
	// If multiple Batch end times are close, it will result in multiple
	// Batch import serials. We need use a non-uniform batch size to create a pipeline effect.
	// Here we calculate the total number of engines, which is needed to compute the scale up
	//
	//     Total/B1 = 1/(1-R) * (N - 1/beta(N, R))
	//              ≲ N/(1-R)
	//
	// We use a simple brute force search since the search space is extremely small.
	ratio := totalDataFileSize * (1 - batchImportRatio) / batchSize
	n := math.Ceil(ratio)
	logGammaNPlusR, _ := math.Lgamma(n + batchImportRatio)
	logGammaN, _ := math.Lgamma(n)
	logGammaR, _ := math.Lgamma(batchImportRatio)
	invBetaNR := math.Exp(logGammaNPlusR - logGammaN - logGammaR) // 1/B(N, R) = Γ(N+R)/Γ(N)Γ(R)
	for {
		if n <= 0 || n > tableConcurrency {
			n = tableConcurrency
			break
		}
		realRatio := n - invBetaNR
		if realRatio >= ratio {
			// we don't have enough engines. reduce the batch size to keep the pipeline smooth.
			curBatchSize = totalDataFileSize * (1 - batchImportRatio) / realRatio
			break
		}
		invBetaNR *= 1 + batchImportRatio/n // Γ(X+1) = X * Γ(X)
		n += 1.0
	}

	for i, dataFileSize := range dataFileSizes {
		filesRegions[i].EngineID = curEngineID
		curEngineSize += dataFileSize

		if curEngineSize >= curBatchSize {
			curEngineSize = 0
			curEngineID++

			i := float64(curEngineID)
			// calculate the non-uniform batch size
			if i >= n {
				curBatchSize = batchSize
			} else {
				// B_(i+1) = B_i * (I/W/(N-i) + 1)
				curBatchSize *= batchImportRatio/(n-i) + 1.0
			}
		}
	}
}

func MakeTableRegions(
	meta *MDTableMeta,
	columns int,
	batchSize int64,
	batchImportRatio float64,
	tableConcurrency int,
) ([]*TableRegion, error) {
	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	dataFileSizes := make([]float64, 0, len(meta.DataFiles))

	prevRowIDMax := int64(0)
	for _, dataFile := range meta.DataFiles {
		dataFileInfo, err := os.Stat(dataFile)
		if err != nil {
			return nil, errors.Annotatef(err, "cannot stat %s", dataFile)
		}
		dataFileSize := dataFileInfo.Size()

		divisor := int64(columns)
		if strings.HasSuffix(strings.ToLower(dataFile), ".sql") {
			divisor += 2
		}
		rowIDMax := prevRowIDMax + dataFileSize/divisor
		filesRegions = append(filesRegions, &TableRegion{
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
		dataFileSizes = append(dataFileSizes, float64(dataFileSize))
	}

	AllocateEngineIDs(filesRegions, dataFileSizes, float64(batchSize), batchImportRatio, float64(tableConcurrency))
	return filesRegions, nil
}
