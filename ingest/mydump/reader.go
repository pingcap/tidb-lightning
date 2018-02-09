package mydump

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	insStmtRegex = regexp.MustCompile(`INSERT INTO .* VALUES`)
)

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
			statment := string(buffer)
			if !(strings.HasPrefix(statment, "/*") && strings.HasSuffix(statment, "*/;")) {
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
	fd         *os.File
	file       string
	fsize      int64
	start      int64
	stmtHeader []byte

	bufferSize int64
	buffer     *bufio.Reader
	// readBuffer []byte
}

func NewMDDataReader(file string, offset int64) (*MDDataReader, error) {
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
		fd:         fd,
		fsize:      fstat.Size(),
		file:       file,
		start:      offset,
		stmtHeader: getInsertStatmentHeader(file),
	}

	if len(mdr.stmtHeader) == 0 {
		return nil, errors.New("can not find any insert statment !")
	}

	mdr.skipAnnotation(offset)
	return mdr, nil
}

func (r *MDDataReader) Close() error {
	if r.fd != nil {
		if err := r.fd.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	r.buffer = nil
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

	return r.currOffset()
}

func (r *MDDataReader) Tell() int64 {
	return r.currOffset()
}

func (r *MDDataReader) currOffset() int64 {
	if off, err := r.fd.Seek(0, io.SeekCurrent); err != nil {
		log.Errorf("get file offset failed (%s) : %v", r.file, err)
		return -1
	} else {
		return off
	}
}

func getInsertStatmentHeader(file string) []byte {
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
		r.buffer = bufio.NewReaderSize(fd, int(size))
		r.bufferSize = size
	}
	return r.buffer
}

func (r *MDDataReader) Read(minSize int64) ([][]byte, error) {
	fd, beginPos := r.fd, r.currOffset()
	if beginPos >= r.fsize {
		return nil, io.EOF
	}

	reader := r.acquireBufferReader(fd, minSize<<1)
	defer reader.Reset(fd)

	// split file's content into multi sql statement
	var stmts [][]byte = make([][]byte, 0, 8)
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
				return // ps : empty sql statment without any actual values ~
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

			stmts = append(stmts, sql)
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
	var statment []byte = make([]byte, 0, minSize+4096)
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

			if len(statment) == 0 && !bytes.HasPrefix(line, r.stmtHeader) {
				statment = append(statment, r.stmtHeader...)
			}
			statment = append(statment, line...)

			if statment[len(statment)-1] == ';' {
				appendSQL(statment)
				statment = make([]byte, 0, minSize+4096)
			}
		}

		readSize += lineSize
		if readSize >= minSize {
			fd.Seek(beginPos+readSize, io.SeekStart) // ps : as buffer reader over readed !
			break
		}
	}

	if len(statment) > 0 {
		appendSQL(statment)
	}

	return stmts, nil
}

// func (r *MDDataReader) Read(maxSize int64) ([][]byte, error) {
// 	beginPos := r.currOffset()
// 	fd := r.fd

// 	if maxSize > r.bufferSize {
// 		r.readBuffer = make([]byte, maxSize, maxSize+maxSize>>1)
// 		r.bufferSize = maxSize
// 	}

// 	// 1. read data in block
// 	data := r.readBuffer[:maxSize]
// 	nbytes, err := fd.Read(data)
// 	if err == io.EOF {
// 		return [][]byte{}, err
// 	}

// 	if r.currOffset() != r.fsize {
// 		// ps :
// 		//		As we treat rows sperated by lines,
// 		// 		So if not end of file, should seek the last ending of line as the split mark ~
// 		idx := bytes.LastIndex(data, []byte{'\n'})
// 		if idx < 0 {
// 			// might be end of file ?
// 			log.Errorf("Opps ! Not found a line ending (pos = %d / size = %d) !", beginPos, maxSize)
// 			return [][]byte{}, nil
// 		}

// 		data = data[:idx]
// 		if bytes.HasSuffix(data, r.stmtHeader) {
// 			idx = bytes.LastIndex(data, r.stmtHeader)
// 			data = data[:idx]
// 		} else {
// 			data = append(data, '\n')
// 		}
// 	} else {
// 		data = data[:nbytes]
// 	}

// 	size := len(data)
// 	if size == 0 {
// 		// ps : maybe maxsize too small to extract any row data ~
// 		return [][]byte{}, nil
// 	}
// 	fd.Seek(beginPos+int64(size), io.SeekStart)

// 	// 2. Convert data into valid sql statment
// 	sql := bytes.TrimSpace(data)
// 	sqlLen := len(sql)
// 	if !bytes.HasSuffix(sql, []byte(");")) {
// 		if bytes.HasSuffix(sql, []byte("),")) {
// 			sql[sqlLen-1] = ';'
// 		} else {
// 			str := string(sql)
// 			log.Errorf("Opps ! Unexpect data ending : '%s' (pos = %d / size = %d) !",
// 				str, beginPos, maxSize)
// 			// TODO : f.seek()
// 			return [][]byte{}, nil
// 		}
// 	}

// 	// 3. The whole sql might contains multi statment,
// 	//	  split it into couple of statements ~
// 	sep := r.stmtHeader // TODO : or []byte(");\n") ?
// 	statements := make([][]byte, 0, 1)
// 	for content := sql; ; {
// 		content = bytes.TrimSpace(content)
// 		end := bytes.Index(content[1:], sep)
// 		if end < 0 {
// 			statements = append(statements, content)
// 			break
// 		}

// 		stmt := bytes.TrimSpace(content[:end])
// 		if len(stmt) > 0 {
// 			statements = append(statements, stmt)
// 		}
// 		content = content[end:]
// 	}

// 	if len(statements) > 0 {
// 		stmt := statements[0]
// 		if !bytes.HasPrefix(stmt, r.stmtHeader) {
// 			fixStmt := make([]byte, 0, len(stmt)+len(r.stmtHeader)+1)
// 			fixStmt = append(fixStmt, r.stmtHeader...)
// 			fixStmt = append(fixStmt, ' ')
// 			fixStmt = append(fixStmt, stmt...)
// 			statements[0] = fixStmt
// 		}
// 	}

// 	return statements, nil
// }

/////////////////////////////////////////////////////////////////////////

type RegionReader struct {
	fileReader *MDDataReader
	offset     int64
	size       int64
	pos        int64
}

func NewRegionReader(file string, offset int64, size int64) (*RegionReader, error) {
	fileReader, err := NewMDDataReader(file, offset)
	if err != nil {
		return nil, err
	}

	return &RegionReader{
		fileReader: fileReader,
		size:       size,
		offset:     offset,
		pos:        fileReader.Tell(),
	}, nil
}

func (r *RegionReader) Read(maxBlockSize int64) ([][]byte, error) {
	if r.pos >= r.offset+r.size {
		return [][]byte{}, io.EOF
	}

	readSize := r.offset + r.size - r.pos
	if maxBlockSize < readSize {
		readSize = maxBlockSize
	}

	datas, err := r.fileReader.Read(readSize)
	r.pos = r.fileReader.Tell()

	return datas, err
}

func (r *RegionReader) Close() error {
	return r.fileReader.Close()
}
