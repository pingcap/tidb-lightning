package mydumper

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-lightning/lightning/datasource/base"
)

var (
	insStmtRegex = regexp.MustCompile(`INSERT INTO .* VALUES`)
)

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

func NewMDDataReader(db, table, file string, offset int64) (*MDDataReader, error) {
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
	return base.TypeMydumper
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

	return base.CurrOffset(r.fd)
}

func (r *MDDataReader) Seek(offset int64) int64 {
	return r.skipAnnotation(offset)
}

func (r *MDDataReader) Tell() int64 {
	return base.CurrOffset(r.fd)
}

func (r *MDDataReader) SplitRegions(regionSize int64) ([]*base.TableRegion, error) {
	newRegion := func(offset, size int64) *base.TableRegion {
		return &base.TableRegion{
			ID:         -1,
			DB:         r.db,
			Table:      r.table,
			File:       r.file,
			Offset:     offset,
			Size:       size,
			SourceType: r.sourceType(),
		}
	}

	regions := make([]*base.TableRegion, 0)

	var extendSize = int64(4 << 10) // 4 K
	var offset int64
	for {
		r.Seek(offset + regionSize)
		_, err := r.Read(extendSize, 0)
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
	br := bufio.NewReader(f)
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

func (r *MDDataReader) Read(minSize int64, endPos int64) ([]*base.Payload, error) {
	fd, beginPos := r.fd, base.CurrOffset(r.fd)
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

	payloads := make([]*base.Payload, 0, len(stmts))
	for _, stmt := range stmts {
		payloads = append(payloads, &base.Payload{SQL: stmt})
	}
	return payloads, nil
}
