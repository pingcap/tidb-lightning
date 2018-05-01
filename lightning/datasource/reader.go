package datasource

import (
	"io"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
	"github.com/pingcap/tidb-lightning/lightning/datasource/csv"
	"github.com/pingcap/tidb-lightning/lightning/datasource/mydumper"
	log "github.com/sirupsen/logrus"
)

type DataReader interface {
	Read(minSize int64, endPos int64) ([]*base.Payload, error)
	Tell() int64
	Seek(offset int64) int64
	Close() error
	SplitRegions(regionSize int64) ([]*base.TableRegion, error)
}

func NewDataReader(sourceType, db, table, file string, offset int64, batch int64) (DataReader, error) {
	switch sourceType {
	case base.TypeMydumper:
		return mydumper.NewMDDataReader(db, table, file, offset)
	case base.TypeCSV:
		return csv.NewCSVDataReader(db, table, file, offset, batch)
	}
	return nil, errors.Errorf("unknown source type %s", sourceType)
}

/////////////////////////////////////////////////////////////////////////

type RegionReader struct {
	fileReader DataReader
	pos        int64
	region     *base.TableRegion
}

func NewRegionReader(region *base.TableRegion, batch int64) (*RegionReader, error) {
	log.Infof("[%s] offset = %d / size = %d / rows = %d", region.File, region.Offset, region.Size, region.Rows)

	fileReader, err := NewDataReader(region.SourceType, region.DB, region.Table, region.File, region.Offset, batch)
	if err != nil {
		return nil, err
	}

	return &RegionReader{
		fileReader: fileReader,
		pos:        fileReader.Tell(),
		region:     region,
	}, nil
}

func (r *RegionReader) Read(maxBlockSize int64) ([]*base.Payload, error) {
	if r.pos >= r.region.Offset+r.region.Size {
		log.Infof("region %+v returns EOF", r.region)
		return nil, io.EOF
	}

	readSize := r.region.Offset + r.region.Size - r.pos
	if maxBlockSize < readSize {
		readSize = maxBlockSize
	}

	datas, err := r.fileReader.Read(readSize, r.region.Offset+r.region.Size)
	r.pos = r.fileReader.Tell()

	return datas, errors.Trace(err)
}

func (r *RegionReader) Close() error {
	return r.fileReader.Close()
}
