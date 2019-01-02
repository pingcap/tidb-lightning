package mydump

import (
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/pingcap/tidb-lightning/lightning/worker"
)

// ChunkParser is a parser of the data files (the file containing only INSERT
// statements).
type ChunkParser struct {
	// states for the lexer
	reader      io.Reader
	buf         []byte
	blockBuf    []byte
	isLastChunk bool

	lastRow Row
	// Current file offset.
	pos int64
	// The (quoted) table name used in the last INSERT statement. Assumed to be
	// constant throughout the entire file.
	TableName []byte
	// The list of columns in the form `(a, b, c)` in the last INSERT statement.
	// Assumed to be constant throughout the entire file.
	Columns []byte

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer
	ioWorkers *worker.RestoreWorkerPool
}

// Chunk represents a portion of the data file.
type Chunk struct {
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
}

// Row is the content of a row.
type Row struct {
	RowID int64
	Row   []byte
}

// NewChunkParser creates a new parser which can read chunks out of a file.
func NewChunkParser(reader io.Reader, blockBufSize int64, ioWorkers *worker.RestoreWorkerPool) *ChunkParser {
	return &ChunkParser{
		reader:    reader,
		blockBuf:  make([]byte, blockBufSize*config.BufferSizeScale),
		remainBuf: &bytes.Buffer{},
		appendBuf: &bytes.Buffer{},
		ioWorkers: ioWorkers,
	}
}

// Reader returns the underlying reader of this parser.
func (parser *ChunkParser) Reader() io.Reader {
	return parser.reader
}

// SetPos changes the reported position and row ID.
func (parser *ChunkParser) SetPos(pos int64, rowID int64) {
	parser.pos = pos
	parser.lastRow.RowID = rowID
}

// Pos returns the current file offset.
func (parser *ChunkParser) Pos() int64 {
	return parser.pos
}

type token byte

const (
	tokNil token = iota
	tokValues
	tokRow
	tokName
)

func (parser *ChunkParser) readBlock() error {
	startTime := time.Now()

	// limit IO concurrency
	w := parser.ioWorkers.Apply()
	n, err := parser.reader.Read(parser.blockBuf)
	parser.ioWorkers.Recycle(w)

	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		parser.isLastChunk = true
		fallthrough
	case nil:
		// `parser.buf` reference to `appendBuf.Bytes`, so should use remainBuf to
		// hold the `parser.buf` rest data to prevent slice overlap
		parser.remainBuf.Reset()
		parser.remainBuf.Write(parser.buf)
		parser.appendBuf.Reset()
		parser.appendBuf.Write(parser.remainBuf.Bytes())
		parser.appendBuf.Write(parser.blockBuf[:n])
		parser.buf = parser.appendBuf.Bytes()
		metric.ChunkParserReadBlockSecondsHistogram.Observe(time.Since(startTime).Seconds())
		return nil
	default:
		return errors.Trace(err)
	}
}

// ReadRow reads a row from the datafile.
func (parser *ChunkParser) ReadRow() error {
	// This parser will recognize contents like:
	//
	// 		`tableName` (...) VALUES (...) (...) (...)
	//
	// Keywords like INSERT, INTO and separators like ',' and ';' are treated
	// like comments and ignored. Therefore, this parser will accept some
	// nonsense input. The advantage is the parser becomes extremely simple,
	// suitable for us where we just want to quickly and accurately split the
	// file apart, not to validate the content.

	type state byte

	const (
		// the state after reading "VALUES"
		stateRow state = iota
		// the state after reading the table name, before "VALUES"
		stateColumns
	)

	row := &parser.lastRow
	st := stateRow

	for {
		tok, content, err := parser.lex()
		if err != nil {
			return errors.Trace(err)
		}
		switch tok {
		case tokRow:
			switch st {
			case stateRow:
				row.RowID++
				row.Row = content
				return nil
			case stateColumns:
				parser.Columns = content
				continue
			}

		case tokName:
			st = stateColumns
			parser.TableName = content
			parser.Columns = nil
			continue

		case tokValues:
			st = stateRow
			continue

		default:
			return errors.Errorf("Syntax error at position %d", parser.pos)
		}
	}
}

// LastRow is the copy of the row parsed by the last call to ReadRow().
func (parser *ChunkParser) LastRow() Row {
	return parser.lastRow
}

// ReadChunks parses the entire file and splits it into continuous chunks of
// size >= minSize.
func (parser *ChunkParser) ReadChunks(minSize int64) ([]Chunk, error) {
	var chunks []Chunk

	cur := Chunk{
		Offset:       parser.pos,
		EndOffset:    parser.pos,
		PrevRowIDMax: parser.lastRow.RowID,
		RowIDMax:     parser.lastRow.RowID,
	}

	for {
		switch err := parser.ReadRow(); errors.Cause(err) {
		case nil:
			cur.EndOffset = parser.pos
			cur.RowIDMax = parser.lastRow.RowID
			if cur.EndOffset-cur.Offset >= minSize {
				chunks = append(chunks, cur)
				cur.Offset = cur.EndOffset
				cur.PrevRowIDMax = cur.RowIDMax
			}

		case io.EOF:
			if cur.Offset < cur.EndOffset {
				chunks = append(chunks, cur)
			}
			return chunks, nil

		default:
			return nil, errors.Trace(err)
		}
	}
}
