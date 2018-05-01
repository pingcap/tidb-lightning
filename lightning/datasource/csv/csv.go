package csv

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"io"
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
)

type CSVDataReader struct {
	db     string
	table  string
	file   string
	fd     *os.File
	br     *bufio.Reader
	fsize  int64
	buffer bytes.Buffer
	batch  int64
}

func NewCSVDataReader(db, table, file string, offset int64, batch int64) (*CSVDataReader, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		fd.Close()
		return nil, errors.Trace(err)
	}

	fstat, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, errors.Trace(err)
	}

	return &CSVDataReader{
		db:    db,
		table: table,
		file:  file,
		fd:    fd,
		fsize: fstat.Size(),
		batch: batch,
	}, nil
}

func (r *CSVDataReader) Read(minSize int64, endPos int64) ([]*base.Payload, error) {
	var readSize int64
	beginPos := r.Tell()

	br := bufio.NewReaderSize(r.fd, int(minSize))

	var lastRecord string
	// read block
	var count int64
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			log.Infof("EOF, last record %s", lastRecord)
			break
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, err = r.buffer.WriteString(line)
		if err != nil {
			return nil, errors.Trace(err)
		}
		readSize += int64(len(line))
		lastRecord = line

		count++
		// if r.buffer.Len() >= int(minSize) {

		// if base.CurrOffset(r.fd) >= endPos {
		// 	return
		// }

		if count >= r.batch || base.CurrOffset(r.fd) >= endPos {
			_, err = r.fd.Seek(beginPos+readSize, io.SeekStart)
			if err != nil {
				return nil, errors.Trace(err)
			}
			break
		}
	}

	// parse csv
	rd := csv.NewReader(&r.buffer)
	defer r.buffer.Reset()

	records, err := rd.ReadAll()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(records) == 0 {
		return []*base.Payload{}, nil
	}
	params := make([]interface{}, 0, len(records)*len(records[0]))
	for _, record := range records {
		for _, value := range record {
			params = append(params, value)
		}
	}
	return []*base.Payload{&base.Payload{Params: params}}, nil
}

func (r *CSVDataReader) Tell() int64 {
	return base.CurrOffset(r.fd)
}

func (r *CSVDataReader) Seek(offset int64) int64 {
	_, err := r.fd.Seek(offset, io.SeekStart)
	if err != nil {
		log.Errorf("csv seek error %s", errors.ErrorStack(err))
		return -1
	}
	return base.CurrOffset(r.fd)
}

func (r *CSVDataReader) Close() error {
	if r.fd != nil {
		if err := r.fd.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (r *CSVDataReader) SplitRegions(regionSize int64) ([]*base.TableRegion, error) {
	regions := make([]*base.TableRegion, 0)

	appendRegion := func(offset, size int64, rows int64) {
		region := &base.TableRegion{
			ID:         -1,
			DB:         r.db,
			Table:      r.table,
			File:       r.file,
			Offset:     offset,
			Size:       size,
			SourceType: r.sourceType(),
			Rows:       rows,
		}
		regions = append(regions, region)
	}

	rd := bufio.NewReader(r.fd)

	// only one region
	if r.fsize <= regionSize {
		rows, err := countRows(rd)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendRegion(0, r.fsize, rows)
		return regions, nil
	}

	var (
		offset           int64
		lastRegionOffset int64
		regionRows       int64
		lastRecord       string
	)
	for {
		line, err := rd.ReadString('\n')
		if err == io.EOF {
			if size := offset - lastRegionOffset; size > 0 {
				appendRegion(lastRegionOffset, size, regionRows)
				regionRows = 0
				log.Infof("last record %s", lastRecord)
			}
			break
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		offset += int64(len(line))
		regionRows++
		lastRecord = line
		if size := offset - lastRegionOffset; size > regionSize {
			appendRegion(lastRegionOffset, size, regionRows)
			lastRegionOffset = offset
			regionRows = 0
			log.Infof("last record %s", lastRecord)
		}
	}

	return regions, nil
}

func (r *CSVDataReader) sourceType() string {
	return base.TypeCSV
}

func countRows(rd *bufio.Reader) (int64, error) {
	var rows int64
	for {
		_, err := rd.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		rows++

	}
	return rows, nil
}
