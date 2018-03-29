package sql

import (
	"bytes"
	"fmt"
	"unsafe"
)

var (
	// valuesExp = `(?U)(\((.+,*)+\)\s*[,|;]+)+`
	// valsReg   = regexp.MustCompile(valuesExp)

	unescapeChar = map[byte]byte{
		'n': '\n',
		'r': '\r',
		't': '\t',
		'0': 0,
		'b': 8,
		'Z': 26,
	}
)

func bytes2str(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

func ParseInsertStmt(sql []byte, values *[]interface{}) error {
	var s, e, size int
	var ch byte

	for {
		sql = sql[s:]
		size = len(sql)

		// seek start "("
		s = bytes.IndexByte(sql, '(')
		if s < 0 {
			break
		}

		// seek end ")"
		stack := 0
		for e = s; e < size; e++ {
			if sql[e] == '(' {
				stack++
			} else if sql[e] == ')' {
				stack--
				if stack == 0 {
					break
				}
			}
		}

		if e == size {
			return fmt.Errorf("not found cooresponding ending of sql : ')'")
		}

		// extract columns' values
		_ = parseRowValues(sql[s+1:e], values)
		e++ // skip ')'

		// check ending ")," or ");"
		for ; e < size; e++ {
			ch = sql[e]
			if !(ch == ' ' || ch == '\r' || ch == '\n') {
				if ch == ',' || ch == ';' {
					break
				} else {
					return fmt.Errorf("unexpect ending of sql : '%s'", string(ch))
				}
			}
		}

		s = e + 1
		if s >= size {
			break
		}
	}

	return nil
}

func parseRowValues(str []byte, values *[]interface{}) error {
	// values are seperated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a simple implementation, may be more robust later.

	// values := make([]string, 0, 8)

	size := len(str)
	var ch byte
	for i := 0; i < size; {
		ch = str[i]
		if ch == ' ' || ch == '\n' {
			i++
			continue
		}

		if ch != '\'' && ch != '"' {
			// no string, read until comma
			j := i + 1
			for ; j < size && str[j] != ','; j++ {
			}

			val := bytes.TrimSpace(str[i:j])

			*values = append(*values, bytes2str(val)) // ?? no need to trim ??
			// skip ,
			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			sch := ch
			escaped := false
			for j < size {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					escaped = true
					continue
				} else if str[j] == sch {
					// matchup ending
					break
				} else {
					j++
				}
			}

			if j >= size {
				return fmt.Errorf("parse quote values error")
			}

			val := bytes.TrimSpace(str[i : j+1])
			s, e := bytes.IndexByte(val, sch), bytes.LastIndexByte(val, sch)
			val = val[s+1 : e]
			if escaped {
				val = unescapeString(val)
			}

			*values = append(*values, bytes2str(val))

			i = j + 2 // skip ' and ,
		}
		// need skip blank ???
	}

	return nil
}

// unescapeString un-escapes the string.
// mysqldump will escape the string when dumps,
// Refer http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
func unescapeString(s []byte) []byte {
	size := len(s)
	value := make([]byte, 0, size)
	for i := 0; i < size; {
		if s[i] == '\\' {
			j := i + 1
			if j == size {
				break // The last char is \, remove
			}

			ch, ok := unescapeChar[s[j]]
			if !ok {
				ch = s[j]
			}

			value = append(value, ch)
			i += 2
		} else {
			value = append(value, s[i])
			i++
		}
	}

	return value
}
