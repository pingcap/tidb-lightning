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

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

const tableRegionSizeWarningThreshold int64 = 1024 * 1024 * 1024

type TableRegion struct {
	EngineID int32

	DB       string
	Table    string
	FileMeta SourceFileMeta

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
	ctx context.Context,
	meta *MDTableMeta,
	columns int,
	cfg *config.Config,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
) ([]*TableRegion, error) {
	// Split files into regions
	filesRegions := make([]*TableRegion, 0, len(meta.DataFiles))
	dataFileSizes := make([]float64, 0, len(meta.DataFiles))
	prevRowIDMax := int64(0)
	var err error
	for _, dataFile := range meta.DataFiles {
		if dataFile.FileMeta.Type == SourceTypeParquet {
			rowIDMax, region, err := makeParquetFileRegion(ctx, store, meta, dataFile, prevRowIDMax)
			if err != nil {
				return nil, err
			}
			prevRowIDMax = rowIDMax
			filesRegions = append(filesRegions, region)
			// TODO: how to estimate raw data size for parquet file
			dataFileSizes = append(dataFileSizes, float64(dataFile.Size))
			continue
		}

		dataFileSize := dataFile.Size

		divisor := int64(columns)
		isCsvFile := dataFile.FileMeta.Type == SourceTypeCSV
		if !isCsvFile {
			divisor += 2
		}

		// If a csv file is overlarge, we need to split it into multiple regions.
		// Note: We can only split a csv file whose format is strict.
		if isCsvFile && dataFileSize > cfg.Mydumper.MaxRegionSize && cfg.Mydumper.StrictFormat {
			var (
				regions      []*TableRegion
				subFileSizes []float64
			)
			prevRowIDMax, regions, subFileSizes, err = SplitLargeFile(ctx, meta, cfg, dataFile, divisor, prevRowIDMax, ioWorkers, store)
			if err != nil {
				return nil, err
			}
			dataFileSizes = append(dataFileSizes, subFileSizes...)
			filesRegions = append(filesRegions, regions...)
			continue
		}
		rowIDMax := prevRowIDMax + dataFile.Size/divisor
		tableRegion := &TableRegion{
			DB:       meta.DB,
			Table:    meta.Name,
			FileMeta: dataFile.FileMeta,
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
				zap.String("file", dataFile.FileMeta.Path),
				zap.Int64("size", dataFileSize))
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

// because parquet files can't seek efficiently, there is no benefit in split.
// parquet file are column orient, so the offset is read line number
func makeParquetFileRegion(
	ctx context.Context,
	store storage.ExternalStorage,
	meta *MDTableMeta,
	dataFile FileInfo,
	prevRowIdxMax int64,
) (int64, *TableRegion, error) {
	r, err := store.Open(ctx, dataFile.FileMeta.Path)
	if err != nil {
		return prevRowIdxMax, nil, errors.Trace(err)
	}
	pr, err := NewParquetParser(ctx, store, r, dataFile.FileMeta.Path)
	if err != nil {
		return prevRowIdxMax, nil, errors.Trace(err)
	}
	defer pr.Close()

	// EndOffset for parquet files are the number of rows
	numberRows := pr.Reader.GetNumRows()
	rowIDMax := prevRowIdxMax + numberRows
	region := &TableRegion{
		DB:       meta.DB,
		Table:    meta.Name,
		FileMeta: dataFile.FileMeta,
		Chunk: Chunk{
			Offset:       0,
			EndOffset:    numberRows,
			PrevRowIDMax: prevRowIdxMax,
			RowIDMax:     rowIDMax,
		},
	}
	return rowIDMax, region, nil
}

// SplitLargeFile splits a large csv file into multiple regions, the size of
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
	dataFile FileInfo,
	divisor int64,
	prevRowIdxMax int64,
	ioWorker *worker.Pool,
	store storage.ExternalStorage,
) (prevRowIdMax int64, regions []*TableRegion, dataFileSizes []float64, err error) {
	maxRegionSize := cfg.Mydumper.MaxRegionSize
	dataFileSizes = make([]float64, 0, dataFile.Size/maxRegionSize+1)
	startOffset, endOffset := int64(0), maxRegionSize
	var columns []string
	if cfg.Mydumper.CSV.Header {
		r, err := store.Open(ctx, dataFile.FileMeta.Path)
		if err != nil {
			return 0, nil, nil, err
		}
		parser := NewCSVParser(&cfg.Mydumper.CSV, r, cfg.Mydumper.ReadBlockSize, ioWorker, true)
		if err = parser.ReadColumns(); err != nil {
			return 0, nil, nil, err
		}
		columns = parser.Columns()
		startOffset, _ = parser.Pos()
		endOffset = startOffset + maxRegionSize
	}
	for {
		curRowsCnt := (endOffset - startOffset) / divisor
		rowIDMax := prevRowIdxMax + curRowsCnt
		if endOffset != dataFile.Size {
			r, err := store.Open(ctx, dataFile.FileMeta.Path)
			if err != nil {
				return 0, nil, nil, err
			}
			parser := NewCSVParser(&cfg.Mydumper.CSV, r, cfg.Mydumper.ReadBlockSize, ioWorker, false)
			if err = parser.SetPos(endOffset, prevRowIdMax); err != nil {
				return 0, nil, nil, err
			}
			pos, err := parser.ReadUntilTokNewLine()
			if err != nil {
				return 0, nil, nil, err
			}
			endOffset = pos
			parser.Close()
		}
		regions = append(regions,
			&TableRegion{
				DB:       meta.DB,
				Table:    meta.Name,
				FileMeta: dataFile.FileMeta,
				Chunk: Chunk{
					Offset:       startOffset,
					EndOffset:    endOffset,
					PrevRowIDMax: prevRowIdxMax,
					RowIDMax:     rowIDMax,
					Columns:      columns,
				},
			})
		dataFileSizes = append(dataFileSizes, float64(endOffset-startOffset))
		prevRowIdxMax = rowIDMax
		if endOffset == dataFile.Size {
			break
		}
		startOffset = endOffset
		if endOffset += maxRegionSize; endOffset > dataFile.Size {
			endOffset = dataFile.Size
		}
	}
	return prevRowIdxMax, regions, dataFileSizes, nil
}
