package ingest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	_ "golang.org/x/net/context"
)

func ExportStatment(sqlFile string) ([]byte, error) {
	fp, err := os.Open(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer fp.Close()

	br := bufio.NewReader(fp)
	f, _ := os.Stat(sqlFile)

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
	fd    *os.File
	file  string
	fsize int64
	start int64

	sqlFront   []byte
	readBuffer []byte // TODO ..........
}

func NewMDDataReader(file string, offset int64) (*MDDataReader, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		fd.Close()
		return nil, err
	}

	fstat, _ := fd.Stat()

	mdr := &MDDataReader{
		fd:       fd,
		fsize:    fstat.Size(),
		file:     file,
		start:    offset,
		sqlFront: extractInsertFront(file),
	}

	if len(mdr.sqlFront) == 0 {
		return nil, errors.New("can not find any insert statment !")
	}

	mdr.skipAnnotation()

	return mdr, nil
}

func (r *MDDataReader) Close() error {
	if r.fd != nil {
		if err := r.fd.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (r *MDDataReader) AssertEnd() { // TODO : remove ..
	if r.currOffset() != r.fsize {
		panic(fmt.Sprintf("pos = %d VS size = %d", r.currOffset(), r.fsize))
	}
}

func (r *MDDataReader) skipAnnotation() int64 {
	br := bufio.NewReader(r.fd)
	for skipSize := 0; ; {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break // TODO ..................... len(line) > 0 ??????????
		}

		size := len(line)
		line = strings.TrimSpace(line[:size-1])
		if !(strings.HasPrefix(line, "/*") && strings.HasSuffix(line, "*/;")) {
			// backward seek to the last pos
			r.fd.Seek(int64(skipSize), io.SeekStart)
			break
		}
		skipSize += size
	}

	// log.Infof("after skip = %d", r.currOffset())

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

func extractInsertFront(file string) []byte {
	f, err := os.Open(file)
	if err != nil {
		log.Errorf("open file failed (%s) : %v", file, err)
		return []byte{}
	}
	defer f.Close()

	front := ""
	br := bufio.NewReaderSize(f, 16*1024)
	reg := regexp.MustCompile(`INSERT INTO .* VALUES`)
	for l := 0; l < 100; l++ {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		data := strings.ToUpper(line)
		if loc := reg.FindStringIndex(data); len(loc) > 0 {
			front = line[loc[0]:loc[1]]
			break
		}
	}

	return []byte(front)
}

func (r *MDDataReader) Read(maxSize int64) ([]byte, error) {
	fd := r.fd
	beginPos := r.currOffset()
	// log.Infof("begin pos - %d", beginPos)

	/*
		buffer := make([]byte, maxSize+len(r.sqlFront)+1)
		// 0. "INSERT INTO xx VALUES" + " "
		front := append(buffer[:0], r.sqlFront...)
		front = append(buffer, ' ')
	*/

	// 1. read data in block
	data := make([]byte, maxSize)
	nbytes, err := fd.Read(data)
	if err == io.EOF {
		return []byte{}, err
	}

	// log.Infof("pos - %d", r.currOffset())

	if r.currOffset() != r.fsize {
		// ps :
		//		As we treat rows sperated by lines,
		// 		So if not end of file, should seek the last ending of line as the split mark ~
		idx := bytes.LastIndex(data, []byte{'\n'})
		if idx < 0 {
			// might be end of file ?
			log.Errorf("Opps ! Not found a line ending (pos = %d / size = %d) !", beginPos, maxSize)
			return []byte{}, nil
		}

		data = data[:idx]
		if bytes.HasSuffix(data, r.sqlFront) {
			idx = bytes.LastIndex(data, r.sqlFront)
			data = data[:idx]
		}

	} else {
		data = data[:nbytes]
	}

	size := len(data)
	if size == 0 {
		// ps : maybe maxsize too small to extract any row data ~
		return []byte{}, nil
	}
	fd.Seek(beginPos+int64(size), io.SeekStart)

	// 2. Convert data into valid sql statment
	sql := bytes.TrimSpace(data)
	sqlLen := len(sql)
	if !bytes.HasSuffix(sql, []byte(");")) {
		if bytes.HasSuffix(sql, []byte("),")) {
			sql[sqlLen-1] = ';'
		} else {
			str := string(sql)
			log.Infof("<%s>", str)

			log.Errorf("Opps ! Unexpect data ending : '%s',,'%s' (pos = %d / size = %d) !",
				str[:32], str[len(str)-20:], beginPos, maxSize)
			// TODO ............ fd.Seek() !!!!!!!!!!!!!!!!!
			return []byte{}, nil
		}
	}

	/*
		TODO .... if contains multi insert statment ????
	*/

	if bytes.HasPrefix(sql, r.sqlFront) {
		return sql, nil
	} else {
		completeSql := make([]byte, 0, len(sql)+len(r.sqlFront)+1)
		completeSql = append(completeSql, r.sqlFront...)
		completeSql = append(completeSql, ' ')
		completeSql = append(completeSql, sql...)

		return completeSql, nil
	}
}
