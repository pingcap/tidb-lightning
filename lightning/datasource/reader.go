package datasource

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

var (
	insStmtRegex = regexp.MustCompile(`INSERT INTO .* VALUES`)
)

type DataReader interface {
	Read(minSize int64) ([]*Payload, error)
	Tell() int64
	Seek(offset int64) int64
	Close() error
	SplitRegions(regionSize int64) ([]*TableRegion, error)
}

func NewDataReader(sourceType, db, table, file string, offset int64) (DataReader, error) {
	switch sourceType {
	case TypeMydumper:
		return newMDDataReader(db, table, file, offset)
	case TypeCSV:
		return newCSVDataReader(db, table, file, offset)
	}
	return nil, errors.Errorf("unknown source type %s", sourceType)
}

func ExportStatement(sqlFile string) ([]byte, error) {
	fd, err := os.Open(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	f, err := os.Stat(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	data := make([]byte, 0, f.Size()+1)
	buffer := make([]byte, 0, f.Size()+1)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}

		buffer = append(buffer, []byte(line)...)
		if buffer[len(buffer)-1] == ';' {
			statement := string(buffer)
			if !(strings.HasPrefix(statement, "/*") && strings.HasSuffix(statement, "*/;")) {
				data = append(data, buffer...)
			}
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, '\n')
		}
	}

	return data, nil
}

type MDDataReader struct {
	db         string
	table      string
	fd         *os.File
	file       string
	fsize      int64
	stmtHeader []byte

	bufferSize int64
	br         *bufio.Reader
}

func newMDDataReader(db, table, file string, offset int64) (DataReader, error) {
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

	mdr := &MDDataReader{
		db:         db,
		table:      table,
		fd:         fd,
		fsize:      fstat.Size(),
		file:       file,
		stmtHeader: getInsertStatementHeader(file),
	}

	if len(mdr.stmtHeader) == 0 {
		return nil, errors.New("can not find any insert statement")
	}

	mdr.skipAnnotation(offset)
	return mdr, nil
}

func (r *MDDataReader) sourceType() string {
	return TypeMydumper
}

func (r *MDDataReader) Close() error {
	if r.fd != nil {
		if err := r.fd.Close(); err != nil {
			return errors.Trace(err)
		}
	}

	r.br = nil
	r.bufferSize = 0
	return nil
}

func (r *MDDataReader) skipAnnotation(offset int64) int64 {
	br := bufio.NewReader(r.fd)
	for skipSize := 0; ; {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		size := len(line)
		line = strings.TrimSpace(line[:size-1])
		if !(strings.HasPrefix(line, "/*") && strings.HasSuffix(line, "*/;")) {
			// backward seek to the last pos
			r.fd.Seek(offset+int64(skipSize), io.SeekStart)
			break
		}
		skipSize += size
	}

	return currOffset(r.fd)
}

func (r *MDDataReader) Seek(offset int64) int64 {
	return r.skipAnnotation(offset)
}

func (r *MDDataReader) Tell() int64 {
	return currOffset(r.fd)
}

func (r *MDDataReader) SplitRegions(regionSize int64) ([]*TableRegion, error) {
	newRegion := func(offset, size int64) *TableRegion {
		return &TableRegion{
			ID:         -1,
			DB:         r.db,
			Table:      r.table,
			File:       r.file,
			Offset:     offset,
			Size:       size,
			SourceType: r.sourceType(),
		}
	}

	regions := make([]*TableRegion, 0)

	var extendSize = int64(4 << 10) // 4 K
	var offset int64
	for {
		r.Seek(offset + regionSize)
		_, err := r.Read(extendSize)
		pos := r.Tell()

		size := pos - offset
		region := newRegion(offset, size)
		if region.Size > 0 {
			regions = append(regions, region)
		}
		if errors.Cause(err) == io.EOF {
			break
		}
		offset = pos
	}
	return regions, nil
}

func getInsertStatementHeader(file string) []byte {
	f, err := os.Open(file)
	if err != nil {
		log.Errorf("open file failed (%s) : %v", file, err)
		return []byte{}
	}
	defer f.Close()

	header := ""
	br := bufio.NewReaderSize(f, int(defReadBlockSize))
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		data := strings.ToUpper(line)
		if loc := insStmtRegex.FindStringIndex(data); len(loc) > 0 {
			header = line[loc[0]:loc[1]]
			break
		}
	}

	return []byte(header)
}

func (r *MDDataReader) acquireBufferReader(fd *os.File, size int64) *bufio.Reader {
	if size > r.bufferSize {
		r.br = bufio.NewReaderSize(fd, int(size))
		r.bufferSize = size
	}
	return r.br
}

func (r *MDDataReader) Read(minSize int64) ([]*Payload, error) {
	fd, beginPos := r.fd, currOffset(r.fd)
	if beginPos >= r.fsize {
		return nil, io.EOF
	}

	reader := r.acquireBufferReader(fd, minSize<<1)
	defer reader.Reset(fd)

	// split file's content into multi sql statement
	var stmts = make([]string, 0, 8)
	appendSQL := func(sql []byte) {
		sql = bytes.TrimSpace(sql)
		sqlLen := len(sql)
		if sqlLen != 0 {
			// TODO : check  "/* xxx */;"

			// check prefix
			if !bytes.HasPrefix(sql, r.stmtHeader) {
				log.Errorf("Unexpect sql starting : '%s ..'", string(sql)[:10])
				return
			}
			if sqlLen == len(r.stmtHeader) {
				return // ps : empty sql statement without any actual values ~
			}

			// check suffix
			if !bytes.HasSuffix(sql, []byte(";")) {
				if bytes.HasSuffix(sql, []byte(",")) {
					sql[sqlLen-1] = ';'
				} else {
					log.Errorf("Unexpect sql ending : '.. %s'", string(sql)[sqlLen-10:])
					return
				}
			}

			stmts = append(stmts, string(sql))
		}
	}

	/*
		Read file in specified format like :
		'''
			INSERT INTO xxx VALUES
			(...),
			(...),
			(...);
		'''
	*/
	var statement = make([]byte, 0, minSize+4096)
	var readSize, lineSize int64
	var line []byte
	var err error

	/*
		TODO :
			1. "(...);INSERT INTO .."
			2. huge line
	*/
	for end := false; !end; {
		line, err = reader.ReadBytes('\n')
		lineSize = int64(len(line))
		end = (err == io.EOF)

		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			if line[0] == '/' &&
				bytes.HasPrefix(line, []byte("/*")) && bytes.HasSuffix(line, []byte("*/")) {
				// ps : is a comment, ignored it
				// TODO : what if comment with span on multi lines ?
				continue
			}

			if len(statement) == 0 && !bytes.HasPrefix(line, r.stmtHeader) {
				statement = append(statement, r.stmtHeader...)
			}
			statement = append(statement, line...)

			if statement[len(statement)-1] == ';' {
				appendSQL(statement)
				statement = make([]byte, 0, minSize+4096)
			}
		}

		readSize += lineSize
		if readSize >= minSize {
			fd.Seek(beginPos+readSize, io.SeekStart) // ps : as buffer reader over readed !
			break
		}
	}

	if len(statement) > 0 {
		appendSQL(statement)
	}

	payloads := make([]*Payload, 0, len(stmts))
	for _, stmt := range stmts {
		payloads = append(payloads, &Payload{SQL: stmt})
	}
	return payloads, nil
}

type CSVDataReader struct {
	db     string
	table  string
	file   string
	fd     *os.File
	br     *bufio.Reader
	fsize  int64
	buffer bytes.Buffer
}

func newCSVDataReader(db, table, file string, offset int64) (DataReader, error) {
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
	}, nil
}

func (r *CSVDataReader) Read(minSize int64) ([]*Payload, error) {
	var readSize int64
	beginPos := r.Tell()

	br := bufio.NewReaderSize(r.fd, int(minSize))

	var lastRecord string
	// read block
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
		if r.buffer.Len() >= int(minSize) {
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

	payloads := make([]*Payload, 0, len(records))
	for _, record := range records {
		params := make([]interface{}, 0, len(record))
		for _, value := range record {
			params = append(params, value)
		}
		payloads = append(payloads, &Payload{Params: params})
	}

	return payloads, nil
}

func (r *CSVDataReader) Tell() int64 {
	return currOffset(r.fd)
}

func (r *CSVDataReader) Seek(offset int64) int64 {
	_, err := r.fd.Seek(offset, io.SeekStart)
	if err != nil {
		log.Errorf("csv seek error %s", errors.ErrorStack(err))
		return -1
	}
	return currOffset(r.fd)
}

func (r *CSVDataReader) Close() error {
	if r.fd != nil {
		if err := r.fd.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (r *CSVDataReader) SplitRegions(regionSize int64) ([]*TableRegion, error) {
	regions := make([]*TableRegion, 0)

	appendRegion := func(offset, size int64, rows int64) {
		region := &TableRegion{
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
	)
	var lastRecord string
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
	return TypeCSV
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

/////////////////////////////////////////////////////////////////////////

// Payload has either SQL or Params and they are mutually exclusive
type Payload struct {
	SQL    string
	Params []interface{}
}

type RegionReader struct {
	fileReader DataReader
	pos        int64
	region     *TableRegion
}

func NewRegionReader(region *TableRegion) (*RegionReader, error) {
	log.Infof("[%s] offset = %d / size = %d", region.File, region.Offset, region.Size)

	fileReader, err := NewDataReader(region.SourceType, region.DB, region.Table, region.File, region.Offset)
	if err != nil {
		return nil, err
	}

	return &RegionReader{
		fileReader: fileReader,
		pos:        fileReader.Tell(),
		region:     region,
	}, nil
}

func (r *RegionReader) Read(maxBlockSize int64) ([]*Payload, error) {
	if r.pos >= r.region.Offset+r.region.Size {
		log.Infof("region %+v returns EOF", r.region)
		return nil, io.EOF
	}

	readSize := r.region.Offset + r.region.Size - r.pos
	if maxBlockSize < readSize {
		readSize = maxBlockSize
	}

	datas, err := r.fileReader.Read(readSize)
	r.pos = r.fileReader.Tell()

	return datas, errors.Trace(err)
}

func (r *RegionReader) Close() error {
	return r.fileReader.Close()
}

func currOffset(seeker io.Seeker) int64 {
	off, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get file offset failed (%s) : %v", err)
		return -1
	}
	return off
}
