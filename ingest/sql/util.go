package sql

import (
	"bytes"
	"fmt"
	"strings"
)

func MakePrepareStatement(table string, columns int, rows int) string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("INSERT INTO `%s` VALUES ", table))

	stmtVals := ""
	if columns > 1 {
		stmtVals = strings.Repeat("?,", columns-1)
	}
	stmtVals = fmt.Sprintf("(%s?)", stmtVals)

	for i := 0; i < rows; i++ {
		buf.WriteString(stmtVals)
		if i != rows-1 {
			buf.WriteString(",")
		}
	}

	return buf.String()
}
