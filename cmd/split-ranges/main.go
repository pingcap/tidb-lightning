// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	flagStartKey = "start"
	flagEndKey   = "end"
)

func main() {
	var start, end string
	var count int
	var format string
	flag.StringVar(&start, flagStartKey, "", "")
	flag.StringVar(&end, flagEndKey, "", "")
	flag.IntVar(&count, "count", 2, "")
	flag.StringVar(&format, "format", "hex", "")
	flag.Parse()

	cf := &log.Config{File: "split.log"}
	log.InitLogger(cf, "info")
	startKey, err := ParseKey(format, start)
	if err != nil {
		log.L().Error("parse start failed", zap.Error(err))
		os.Exit(1)
	}
	endKey, err := ParseKey(format, end)
	if err != nil {
		log.L().Error("parse end failed", zap.Error(err))
		os.Exit(1)
	}

	log.L().Info("keys",
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey),
	)

	v := splitValuesToRange2(startKey, endKey, int64(count))
	for _, i := range v {
		log.L().Info("return values", zap.Binary("key", i))
	}
}

// splitValuesToRange try to cut [start, end] to count range approximately
// just like [start, v1], [v1, v2]... [vCount, end]
// return value []{v1, v2... vCount}
func splitValuesToRange2(start []byte, end []byte, count int64) [][]byte {
	if bytes.Compare(start, end) == 0 {
		log.L().Info("couldn't split range due to start end are same",
			zap.Binary("start", start),
			zap.Binary("end", end),
			zap.Int64("count", count))
		return [][]byte{end}
	}

	startBytes := make([]byte, 8)
	endBytes := make([]byte, 8)

	minLen := len(start)
	if minLen > len(end) {
		minLen = len(end)
	}

	offset := 0
	for i := 0; i < minLen; i++ {
		if start[i] != end[i] {
			offset = i
			break
		}
	}

	copy(startBytes, start[offset:])
	copy(endBytes, end[offset:])

	sValue := binary.BigEndian.Uint64(startBytes)
	eValue := binary.BigEndian.Uint64(endBytes)

	log.L().Info("s",
		zap.Binary("sb", start[:offset]),
		zap.Binary("eb", end[:offset]),
		zap.Uint64("start", sValue),
		zap.Uint64("end", eValue),
	)

	step := (eValue - sValue) / uint64(count)
	if step == uint64(0) {
		step = uint64(1)
	}

	res := make([][]byte, 0, count)
	for cur := sValue + step; cur <= eValue; cur += step {
		curBytes := make([]byte, offset+8)
		copy(curBytes, start[:offset])
		binary.BigEndian.PutUint64(curBytes[offset:], cur)
		res = append(res, curBytes)
	}
	res = append(res, end)

	return res
}

// splitValuesToRange try to cut [start, end] to count range approximately
// just like [start, v1], [v1, v2]... [vCount, end]
// return value []{v1, v2... vCount}
func splitValuesToRange(start []byte, end []byte, count int64) [][]byte {
	if bytes.Compare(start, end) == 0 {
		log.L().Info("couldn't split range due to start end are same",
			zap.Binary("start", start),
			zap.Binary("end", end),
			zap.Int64("count", count))
		return [][]byte{end}
	}
	minLen := len(start)
	if minLen > len(end) {
		minLen = len(end)
	}
	v := int64(0)
	if v >= count {
		return [][]byte{end}
	}

	s := append([]byte{}, start...)
	e := append([]byte{}, end...)
	preIndex := 0
	postIndex := 0
	log.L().Info("minLen", zap.Int("len", minLen), zap.Int64("count", count))
	for i := 0; i < minLen; i++ {
		log.L().Info("e[i], s[i]",
			zap.Int("i", i),
			zap.Int64("v", v),
			zap.Binary("e[i]", []byte{e[i]}),
			zap.Binary("s[i]", []byte{s[i]}),
		)
		if e[i] > s[i] {
			v = (v * 256) + int64(e[i]-s[i])
			if preIndex == 0 {
				preIndex = i
			}
		} else if e[i] < s[i] {
			v = (v-1)*256 + (int64(e[i]-s[i]) + 256)
			if preIndex == 0 {
				preIndex = i
			}
		}
		if v >= count {
			postIndex = i
			break
		}
		postIndex++
	}

	step := v / count
	reverseStepBytes := make([]byte, 0, step/256+1)
	for step > 0 {
		reverseStepBytes = append(reverseStepBytes, byte(step%256))
		step /= 256
	}

	stepLen := len(reverseStepBytes)

	commonPrefix := append([]byte{}, s[:preIndex]...)

	s = s[preIndex : postIndex+1]
	e = e[preIndex : postIndex+1]
	log.L().Info("len",
		zap.Int("ls", len(s)),
		zap.Int("le", len(e)),
	)

	for v < count {
		s = append(s, byte(0))
		e = append(e, byte(0))
		v = v * 256
	}

	log.L().Info("splitValuesToRange",
		zap.Int64("v", v),
		zap.Int64("count", count),
		zap.Int64("step", step),
		zap.Int("stepLen", stepLen),
		zap.Int("preIndex", preIndex),
		zap.Binary("start", start),
		zap.Binary("end", end),
		zap.Binary("s", s),
		zap.Binary("e", e),
	)

	checkpoint := append([]byte{}, s...)
	res := make([][]byte, 0)
	preIndex = 0
	for {
		preIndex++
		log.L().Info("seek checkpoint",
			zap.Binary("cp", checkpoint),
			zap.Binary("common", commonPrefix),
			zap.Int("length of res", len(res)),
			zap.Int("preIndex", preIndex),
			zap.Binary("ck", append(commonPrefix, checkpoint...)),
		)
		reverseCheckpoint := reverseBytes(checkpoint)
		carry := 0
		for i := 0; i < stepLen; i++ {
			value := int(reverseStepBytes[i] + reverseCheckpoint[i])
			reverseCheckpoint[i] = byte(value + carry - 256)
			if value > 255 {
				carry = 1
			} else {
				break
			}
		}
		if carry == 1 {
			reverseCheckpoint[stepLen] += 1
		}
		checkpoint = reverseBytes(reverseCheckpoint)
		if int64(preIndex) >= count {
			break
		}
		res = append(res, append([]byte{}, append(commonPrefix, checkpoint...)...))
	}
	res = append(res, append([]byte{}, append(commonPrefix, e...)...))
	return res
}

func reverseBytes(b []byte) []byte {
	s := append([]byte{}, b...)
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func ParseKey(format, key string) ([]byte, error) {
	switch format {
	case "raw":
		return []byte(key), nil
	case "escaped":
		return unescapedKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return key, nil
	}
	return nil, errors.New("unknown format")
}

// Ref PD: https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/region_command.go#L334
func unescapedKey(text string) ([]byte, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return nil, errors.WithStack(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return nil, io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			buf = append(buf, c)
		}
	}
	return buf, nil
}

// CompareEndKey compared two keys that BOTH represent the EXCLUSIVE ending of some range. An empty end key is the very
// end, so an empty key is greater than any other keys.
// Please note that this function is not applicable if any one argument is not an EXCLUSIVE ending of a range.
func CompareEndKey(a, b []byte) int {
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return 1
	}

	if len(b) == 0 {
		return -1
	}

	return bytes.Compare(a, b)
}
