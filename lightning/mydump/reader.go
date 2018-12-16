package mydump

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
)

var (
	insStmtRegex = regexp.MustCompile(`(?i)INSERT INTO .* VALUES`)
)

var (
	ErrInsertStatementNotFound = errors.New("insert statement not found")
	errInvalidSchemaEncoding   = errors.New("invalid schema encoding")
)

var (
	supportedSchemaEncodings = []encoding.Encoding{
		simplifiedchinese.GB18030,
	}
)

func decodeCharacterSet(data []byte, characterSet string) ([]byte, error) {
	switch characterSet {
	case "binary":
		// do nothing
	case "auto", "utf8mb4":
		if utf8.Valid(data) {
			break
		}
		if characterSet == "utf8mb4" {
			return nil, errInvalidSchemaEncoding
		}
		// try gb18030 next if the encoding is "auto"
		// if we support too many encodings, consider switching strategy to
		// perform `chardet` first.
		fallthrough
	case "gb18030":
		decoded, err := simplifiedchinese.GB18030.NewDecoder().Bytes(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// check for U+FFFD to see if decoding contains errors.
		// https://groups.google.com/d/msg/golang-nuts/pENT3i4zJYk/v2X3yyiICwAJ
		if bytes.ContainsRune(decoded, '\ufffd') {
			return nil, errInvalidSchemaEncoding
		}
		data = decoded
	default:
		return nil, errors.Errorf("Unsupported encoding %s", characterSet)
	}
	return data, nil
}

func ExportStatement(sqlFile string, characterSet string) ([]byte, error) {
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
		if errors.Cause(err) == io.EOF && len(line) == 0 { // it will return EOF if there is no trailing new line.
			break
		}

		line = strings.TrimSpace(line)
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

	data, err = decodeCharacterSet(data, characterSet)
	if err != nil {
		common.AppLogger.Errorf("cannot decode input file as %s encoding, please convert it manually: %s", characterSet, sqlFile)
		return nil, errors.Annotatef(err, "failed to decode %s as %s", sqlFile, characterSet)
	}
	return data, nil
}
