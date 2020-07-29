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
	"context"
	"math"
	"strings"

	"github.com/pingcap/br/pkg/storage"

	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

const tableRegionSizeWarningThreshold int64 = 1024 * 1024 * 1024

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

	// No need to batch if the Size is too small :)
	if totalDataFileSize <= batchSize {
		return
	}

	curEngineID := int32(0)
	curEngineSize := 0.0
	curBatchSize := batchSize

	// import() step will not be concurrent.
	// If multiple Batch end times are close, it will result in multiple
	// Batch import serials. We need use a non-uniform batch Size to create a pipeline effect.
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
			// we don't have enough engines. reduce the batch Size to keep the pipeline smooth.
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
			// calculate the non-uniform batch Size
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
	ctx context.Context,
	meta *MDTableMeta,
	columns int,
	cfg *config.Config,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
) ([]*TableRegion, error) {
	// Split files into regions
	filesRegions := make(regionSlice, 0, len(meta.DataFiles))
	dataFileSizes := make([]float64, 0, len(meta.DataFiles))
	prevRowIDMax := int64(0)
	var err error
	for _, dataFile := range meta.DataFiles {
		divisor := int64(columns)
		isCsvFile := strings.HasSuffix(strings.ToLower(dataFile.Path), ".csv")
		if !isCsvFile {
			divisor += 2
		}
		// If a csv file is overlarge, we need to split it into mutiple regions.
		// Note: We can only split a csv file whose format is strict and header is empty.
		if isCsvFile && dataFile.Size > cfg.Mydumper.MaxRegionSize && cfg.Mydumper.StrictFormat && !cfg.Mydumper.CSV.Header {
			var (
				regions      []*TableRegion
				subFileSizes []float64
			)
			prevRowIDMax, regions, subFileSizes, err = SplitLargeFile(ctx, meta, cfg, dataFile.Path, dataFile.Size, divisor, prevRowIDMax, ioWorkers, store)
			if err != nil {
				return nil, err
			}
			dataFileSizes = append(dataFileSizes, subFileSizes...)
			filesRegions = append(filesRegions, regions...)
			continue
		}
		rowIDMax := prevRowIDMax + dataFile.Size/divisor
		tableRegion := &TableRegion{
			DB:    meta.DB,
			Table: meta.Name,
			File:  dataFile.Path,
			Chunk: Chunk{
				Offset:       0,
				EndOffset:    dataFile.Size,
				PrevRowIDMax: prevRowIDMax,
				RowIDMax:     rowIDMax,
			},
		}
		filesRegions = append(filesRegions, tableRegion)
		if tableRegion.Size() > tableRegionSizeWarningThreshold {
			log.L().Warn(
				"file is too big to be processed efficiently; we suggest splitting it at 256 MB each",
				zap.String("file", dataFile.Path),
				zap.Int64("Size", dataFile.Size))
		}
		prevRowIDMax = rowIDMax
		dataFileSizes = append(dataFileSizes, float64(dataFile.Size))
	}

	log.L().Debug("in makeTableRegions",
		zap.Int64("maxRegionSize", cfg.Mydumper.MaxRegionSize),
		zap.Int("len fileRegions", len(filesRegions)))

	AllocateEngineIDs(filesRegions, dataFileSizes, float64(cfg.Mydumper.BatchSize), cfg.Mydumper.BatchImportRatio, float64(cfg.App.TableConcurrency))
	return filesRegions, nil
}

// SplitLargeFile splits a large csv file into multiple regions, the Size of
// each regions is specified by `config.MaxRegionSize`.
// Note: We split the file coarsely, thus the format of csv file is needed to be
// strict.
// e.g.
// - CSV file with header is invalid
// - a complete tuple split into multiple lines is invalid
func SplitLargeFile(
	ctx context.Context,
	meta *MDTableMeta,
	cfg *config.Config,
	dataFilePath string,
	dataFileSize int64,
	divisor int64,
	prevRowIdxMax int64,
	ioWorker *worker.Pool,
	store storage.ExternalStorage,
) (prevRowIdMax int64, regions []*TableRegion, dataFileSizes []float64, err error) {
	maxRegionSize := cfg.Mydumper.MaxRegionSize
	dataFileSizes = make([]float64, 0, dataFileSize/maxRegionSize+1)
	startOffset, endOffset := int64(0), maxRegionSize
	for {
		curRowsCnt := (endOffset - startOffset) / divisor
		rowIDMax := prevRowIdxMax + curRowsCnt
		if endOffset != dataFileSize {
			reader, err := store.Open(ctx, dataFilePath)
			if err != nil {
				return 0, nil, nil, err
			}
			parser := NewCSVParser(&cfg.Mydumper.CSV, reader, cfg.Mydumper.ReadBlockSize, ioWorker)
			parser.SetPos(endOffset, prevRowIdMax)
			pos, err := parser.ReadUntilTokNewLine()
			if err != nil {
				return 0, nil, nil, err
			}
			endOffset = pos
			parser.Close()
		}
		regions = append(regions,
			&TableRegion{
				DB:    meta.DB,
				Table: meta.Name,
				File:  dataFilePath,
				Chunk: Chunk{
					Offset:       startOffset,
					EndOffset:    endOffset,
					PrevRowIDMax: prevRowIdxMax,
					RowIDMax:     rowIDMax,
				},
			})
		dataFileSizes = append(dataFileSizes, float64(endOffset-startOffset))
		prevRowIdxMax = rowIDMax
		if endOffset == dataFileSize {
			break
		}
		startOffset = endOffset
		if endOffset += maxRegionSize; endOffset > dataFileSize {
			endOffset = dataFileSize
		}
	}
	return prevRowIdxMax, regions, dataFileSizes, nil
}
