// Code generated by ragel DO NOT EDIT.

//.... lightning/mydump/parser.rl:1
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

// Please edit `parser.rl` if you want to modify this file. To generate
// `parser_generated.go`, please execute
//
// ```sh
// make data_parsers
// ```

package mydump

import (
	"io"

	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)


//.... lightning/mydump/parser.rl:140



//.... tmp_parser.go:39
const chunk_parser_start int = 21
const chunk_parser_first_final int = 21
const chunk_parser_error int = 0

const chunk_parser_en_main int = 21


//.... lightning/mydump/parser.rl:143

func (parser *ChunkParser) lex() (token, []byte, error) {
	var cs, ts, te, act, p int
	
//.... tmp_parser.go:52
	{
	cs = chunk_parser_start
	ts = 0
	te = 0
	act = 0
	}

//.... lightning/mydump/parser.rl:147

	for {
		data := parser.buf
		consumedToken := tokNil
		pe := len(data)
		eof := -1
		if parser.isLastChunk {
			eof = pe
		}

		
//.... tmp_parser.go:72
	{
	var _widec int16
	if p == pe {
		goto _test_eof
	}
	switch cs {
	case 21:
		goto st_case_21
	case 22:
		goto st_case_22
	case 1:
		goto st_case_1
	case 2:
		goto st_case_2
	case 23:
		goto st_case_23
	case 3:
		goto st_case_3
	case 0:
		goto st_case_0
	case 4:
		goto st_case_4
	case 5:
		goto st_case_5
	case 24:
		goto st_case_24
	case 6:
		goto st_case_6
	case 25:
		goto st_case_25
	case 26:
		goto st_case_26
	case 7:
		goto st_case_7
	case 27:
		goto st_case_27
	case 28:
		goto st_case_28
	case 29:
		goto st_case_29
	case 8:
		goto st_case_8
	case 9:
		goto st_case_9
	case 30:
		goto st_case_30
	case 31:
		goto st_case_31
	case 32:
		goto st_case_32
	case 33:
		goto st_case_33
	case 34:
		goto st_case_34
	case 10:
		goto st_case_10
	case 35:
		goto st_case_35
	case 36:
		goto st_case_36
	case 37:
		goto st_case_37
	case 38:
		goto st_case_38
	case 39:
		goto st_case_39
	case 40:
		goto st_case_40
	case 41:
		goto st_case_41
	case 42:
		goto st_case_42
	case 43:
		goto st_case_43
	case 44:
		goto st_case_44
	case 45:
		goto st_case_45
	case 46:
		goto st_case_46
	case 47:
		goto st_case_47
	case 48:
		goto st_case_48
	case 49:
		goto st_case_49
	case 50:
		goto st_case_50
	case 51:
		goto st_case_51
	case 52:
		goto st_case_52
	case 53:
		goto st_case_53
	case 54:
		goto st_case_54
	case 55:
		goto st_case_55
	case 56:
		goto st_case_56
	case 57:
		goto st_case_57
	case 58:
		goto st_case_58
	case 59:
		goto st_case_59
	case 11:
		goto st_case_11
	case 12:
		goto st_case_12
	case 13:
		goto st_case_13
	case 14:
		goto st_case_14
	case 15:
		goto st_case_15
	case 16:
		goto st_case_16
	case 17:
		goto st_case_17
	case 18:
		goto st_case_18
	case 60:
		goto st_case_60
	case 61:
		goto st_case_61
	case 62:
		goto st_case_62
	case 63:
		goto st_case_63
	case 64:
		goto st_case_64
	case 65:
		goto st_case_65
	case 19:
		goto st_case_19
	case 20:
		goto st_case_20
	case 66:
		goto st_case_66
	}
	goto st_out
tr4:
//.... NONE:1
	switch act {
	case 0:
	{{goto st0 }}
	case 4:
	{p = (te) - 1

		consumedToken = tokValues
		{p++; cs = 21; goto _out }
	}
	case 5:
	{p = (te) - 1

		consumedToken = tokNull
		{p++; cs = 21; goto _out }
	}
	case 6:
	{p = (te) - 1

		consumedToken = tokTrue
		{p++; cs = 21; goto _out }
	}
	case 7:
	{p = (te) - 1

		consumedToken = tokFalse
		{p++; cs = 21; goto _out }
	}
	case 9:
	{p = (te) - 1

		consumedToken = tokHexString
		{p++; cs = 21; goto _out }
	}
	case 10:
	{p = (te) - 1

		consumedToken = tokBinString
		{p++; cs = 21; goto _out }
	}
	case 11:
	{p = (te) - 1

		consumedToken = tokSingleQuoted
		{p++; cs = 21; goto _out }
	}
	case 12:
	{p = (te) - 1

		consumedToken = tokDoubleQuoted
		{p++; cs = 21; goto _out }
	}
	case 13:
	{p = (te) - 1

		consumedToken = tokBackQuoted
		{p++; cs = 21; goto _out }
	}
	case 14:
	{p = (te) - 1

		consumedToken = tokUnquoted
		{p++; cs = 21; goto _out }
	}
	default:
	{p = (te) - 1
}
	}
	
	goto st21
tr8:
//.... lightning/mydump/parser.rl:133
p = (te) - 1
{
		consumedToken = tokUnquoted
		{p++; cs = 21; goto _out }
	}
	goto st21
tr10:
//.... lightning/mydump/parser.rl:71
te = p+1

	goto st21
tr13:
//.... lightning/mydump/parser.rl:113
te = p+1
{
		consumedToken = tokBinString
		{p++; cs = 21; goto _out }
	}
	goto st21
tr22:
//.... lightning/mydump/parser.rl:108
te = p+1
{
		consumedToken = tokHexString
		{p++; cs = 21; goto _out }
	}
	goto st21
tr29:
//.... lightning/mydump/parser.rl:73
te = p+1
{
		consumedToken = tokRowBegin
		{p++; cs = 21; goto _out }
	}
	goto st21
tr30:
//.... lightning/mydump/parser.rl:78
te = p+1
{
		consumedToken = tokRowEnd
		{p++; cs = 21; goto _out }
	}
	goto st21
tr44:
//.... lightning/mydump/parser.rl:123
te = p
p--
{
		consumedToken = tokDoubleQuoted
		{p++; cs = 21; goto _out }
	}
	goto st21
tr45:
//.... lightning/mydump/parser.rl:118
te = p
p--
{
		consumedToken = tokSingleQuoted
		{p++; cs = 21; goto _out }
	}
	goto st21
tr46:
//.... lightning/mydump/parser.rl:133
te = p
p--
{
		consumedToken = tokUnquoted
		{p++; cs = 21; goto _out }
	}
	goto st21
tr48:
//.... lightning/mydump/parser.rl:103
te = p
p--
{
		consumedToken = tokInteger
		{p++; cs = 21; goto _out }
	}
	goto st21
tr84:
//.... lightning/mydump/parser.rl:128
te = p
p--
{
		consumedToken = tokBackQuoted
		{p++; cs = 21; goto _out }
	}
	goto st21
	st21:
//.... NONE:1
ts = 0

//.... NONE:1
act = 0

		if p++; p == pe {
			goto _test_eof21
		}
	st_case_21:
//.... NONE:1
ts = p

//.... tmp_parser.go:390
		switch data[p] {
		case 32:
			goto tr10
		case 34:
			goto st1
		case 39:
			goto st4
		case 40:
			goto tr29
		case 41:
			goto tr30
		case 44:
			goto tr10
		case 45:
			goto st25
		case 47:
			goto st28
		case 48:
			goto st31
		case 59:
			goto tr10
		case 66:
			goto tr35
		case 67:
			goto st35
		case 70:
			goto st42
		case 73:
			goto st46
		case 78:
			goto st49
		case 84:
			goto st52
		case 85:
			goto st55
		case 86:
			goto st60
		case 88:
			goto tr43
		case 96:
			goto st20
		case 98:
			goto tr35
		case 99:
			goto st35
		case 102:
			goto st42
		case 105:
			goto st46
		case 110:
			goto st49
		case 116:
			goto st52
		case 117:
			goto st55
		case 118:
			goto st60
		case 120:
			goto tr43
		}
		switch {
		case data[p] > 13:
			if 49 <= data[p] && data[p] <= 57 {
				goto st27
			}
		case data[p] >= 9:
			goto tr10
		}
		goto tr26
tr26:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:133
act = 14;
	goto st22
tr51:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:71
act = 1;
	goto st22
tr65:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:98
act = 7;
	goto st22
tr70:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:88
act = 5;
	goto st22
tr73:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:93
act = 6;
	goto st22
tr83:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:83
act = 4;
	goto st22
	st22:
		if p++; p == pe {
			goto _test_eof22
		}
	st_case_22:
//.... tmp_parser.go:507
		switch data[p] {
		case 32:
			goto tr4
		case 34:
			goto tr4
		case 44:
			goto tr4
		case 59:
			goto tr4
		case 96:
			goto tr4
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr4
			}
		case data[p] >= 9:
			goto tr4
		}
		goto tr26
	st1:
		if p++; p == pe {
			goto _test_eof1
		}
	st_case_1:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 34:
			goto tr1
		case 348:
			goto st2
		case 604:
			goto st3
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st2
			}
		default:
			goto st2
		}
		goto st0
	st2:
		if p++; p == pe {
			goto _test_eof2
		}
	st_case_2:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 34:
			goto tr1
		case 348:
			goto st2
		case 604:
			goto st3
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st2
			}
		default:
			goto st2
		}
		goto tr4
tr1:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:123
act = 12;
	goto st23
	st23:
		if p++; p == pe {
			goto _test_eof23
		}
	st_case_23:
//.... tmp_parser.go:599
		if data[p] == 34 {
			goto st2
		}
		goto tr44
	st3:
		if p++; p == pe {
			goto _test_eof3
		}
	st_case_3:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 348:
			goto st2
		case 604:
			goto st2
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st2
			}
		default:
			goto st2
		}
		goto tr4
st_case_0:
	st0:
		cs = 0
		goto _out
	st4:
		if p++; p == pe {
			goto _test_eof4
		}
	st_case_4:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 39:
			goto tr6
		case 348:
			goto st5
		case 604:
			goto st6
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st5
			}
		default:
			goto st5
		}
		goto st0
	st5:
		if p++; p == pe {
			goto _test_eof5
		}
	st_case_5:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 39:
			goto tr6
		case 348:
			goto st5
		case 604:
			goto st6
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st5
			}
		default:
			goto st5
		}
		goto tr4
tr6:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:118
act = 11;
	goto st24
	st24:
		if p++; p == pe {
			goto _test_eof24
		}
	st_case_24:
//.... tmp_parser.go:705
		if data[p] == 39 {
			goto st5
		}
		goto tr45
	st6:
		if p++; p == pe {
			goto _test_eof6
		}
	st_case_6:
		_widec = int16(data[p])
		if 92 <= data[p] && data[p] <= 92 {
			_widec = 256 + (int16(data[p]) - 0)
			if  parser.escFlavor != backslashEscapeFlavorNone  {
				_widec += 256
			}
		}
		switch _widec {
		case 348:
			goto st5
		case 604:
			goto st5
		}
		switch {
		case _widec > 91:
			if 93 <= _widec {
				goto st5
			}
		default:
			goto st5
		}
		goto tr4
	st25:
		if p++; p == pe {
			goto _test_eof25
		}
	st_case_25:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 45:
			goto tr47
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] < 39:
			if 9 <= data[p] && data[p] <= 13 {
				goto tr46
			}
		case data[p] > 41:
			if 48 <= data[p] && data[p] <= 57 {
				goto st27
			}
		default:
			goto tr46
		}
		goto tr26
tr47:
//.... NONE:1
te = p+1

	goto st26
	st26:
		if p++; p == pe {
			goto _test_eof26
		}
	st_case_26:
//.... tmp_parser.go:779
		switch data[p] {
		case 10:
			goto tr10
		case 32:
			goto st7
		case 34:
			goto st7
		case 44:
			goto st7
		case 59:
			goto st7
		case 96:
			goto st7
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto st7
			}
		case data[p] >= 9:
			goto st7
		}
		goto tr47
	st7:
		if p++; p == pe {
			goto _test_eof7
		}
	st_case_7:
		if data[p] == 10 {
			goto tr10
		}
		goto st7
	st27:
		if p++; p == pe {
			goto _test_eof27
		}
	st_case_27:
		switch data[p] {
		case 32:
			goto tr48
		case 34:
			goto tr48
		case 44:
			goto tr48
		case 59:
			goto tr48
		case 96:
			goto tr48
		}
		switch {
		case data[p] < 39:
			if 9 <= data[p] && data[p] <= 13 {
				goto tr48
			}
		case data[p] > 41:
			if 48 <= data[p] && data[p] <= 57 {
				goto st27
			}
		default:
			goto tr48
		}
		goto tr26
	st28:
		if p++; p == pe {
			goto _test_eof28
		}
	st_case_28:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 42:
			goto tr49
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
tr49:
//.... NONE:1
te = p+1

	goto st29
	st29:
		if p++; p == pe {
			goto _test_eof29
		}
	st_case_29:
//.... tmp_parser.go:880
		switch data[p] {
		case 32:
			goto st8
		case 34:
			goto st8
		case 42:
			goto tr50
		case 44:
			goto st8
		case 59:
			goto st8
		case 96:
			goto st8
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto st8
			}
		case data[p] >= 9:
			goto st8
		}
		goto tr49
	st8:
		if p++; p == pe {
			goto _test_eof8
		}
	st_case_8:
		if data[p] == 42 {
			goto st9
		}
		goto st8
	st9:
		if p++; p == pe {
			goto _test_eof9
		}
	st_case_9:
		switch data[p] {
		case 42:
			goto st9
		case 47:
			goto tr10
		}
		goto st8
tr50:
//.... NONE:1
te = p+1

	goto st30
	st30:
		if p++; p == pe {
			goto _test_eof30
		}
	st_case_30:
//.... tmp_parser.go:935
		switch data[p] {
		case 32:
			goto st8
		case 34:
			goto st8
		case 42:
			goto tr50
		case 44:
			goto st8
		case 47:
			goto tr51
		case 59:
			goto st8
		case 96:
			goto st8
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto st8
			}
		case data[p] >= 9:
			goto st8
		}
		goto tr49
	st31:
		if p++; p == pe {
			goto _test_eof31
		}
	st_case_31:
		switch data[p] {
		case 32:
			goto tr48
		case 34:
			goto tr48
		case 44:
			goto tr48
		case 59:
			goto tr48
		case 96:
			goto tr48
		case 98:
			goto tr52
		case 120:
			goto tr53
		}
		switch {
		case data[p] < 39:
			if 9 <= data[p] && data[p] <= 13 {
				goto tr48
			}
		case data[p] > 41:
			if 48 <= data[p] && data[p] <= 57 {
				goto st27
			}
		default:
			goto tr48
		}
		goto tr26
tr52:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:133
act = 14;
	goto st32
tr54:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:113
act = 10;
	goto st32
	st32:
		if p++; p == pe {
			goto _test_eof32
		}
	st_case_32:
//.... tmp_parser.go:1014
		switch data[p] {
		case 32:
			goto tr4
		case 34:
			goto tr4
		case 44:
			goto tr4
		case 59:
			goto tr4
		case 96:
			goto tr4
		}
		switch {
		case data[p] < 39:
			if 9 <= data[p] && data[p] <= 13 {
				goto tr4
			}
		case data[p] > 41:
			if 48 <= data[p] && data[p] <= 49 {
				goto tr54
			}
		default:
			goto tr4
		}
		goto tr26
tr53:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:133
act = 14;
	goto st33
tr55:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:108
act = 9;
	goto st33
	st33:
		if p++; p == pe {
			goto _test_eof33
		}
	st_case_33:
//.... tmp_parser.go:1059
		switch data[p] {
		case 32:
			goto tr4
		case 34:
			goto tr4
		case 44:
			goto tr4
		case 59:
			goto tr4
		case 96:
			goto tr4
		}
		switch {
		case data[p] < 48:
			switch {
			case data[p] > 13:
				if 39 <= data[p] && data[p] <= 41 {
					goto tr4
				}
			case data[p] >= 9:
				goto tr4
			}
		case data[p] > 57:
			switch {
			case data[p] > 70:
				if 97 <= data[p] && data[p] <= 102 {
					goto tr55
				}
			case data[p] >= 65:
				goto tr55
			}
		default:
			goto tr55
		}
		goto tr26
tr35:
//.... NONE:1
te = p+1

	goto st34
	st34:
		if p++; p == pe {
			goto _test_eof34
		}
	st_case_34:
//.... tmp_parser.go:1105
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 39:
			goto st10
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] > 13:
			if 40 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st10:
		if p++; p == pe {
			goto _test_eof10
		}
	st_case_10:
		if data[p] == 39 {
			goto tr13
		}
		if 48 <= data[p] && data[p] <= 49 {
			goto st10
		}
		goto tr8
	st35:
		if p++; p == pe {
			goto _test_eof35
		}
	st_case_35:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 79:
			goto st36
		case 96:
			goto tr46
		case 111:
			goto st36
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st36:
		if p++; p == pe {
			goto _test_eof36
		}
	st_case_36:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 78:
			goto st37
		case 96:
			goto tr46
		case 110:
			goto st37
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st37:
		if p++; p == pe {
			goto _test_eof37
		}
	st_case_37:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 86:
			goto st38
		case 96:
			goto tr46
		case 118:
			goto st38
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st38:
		if p++; p == pe {
			goto _test_eof38
		}
	st_case_38:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 69:
			goto st39
		case 96:
			goto tr46
		case 101:
			goto st39
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st39:
		if p++; p == pe {
			goto _test_eof39
		}
	st_case_39:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 82:
			goto st40
		case 96:
			goto tr46
		case 114:
			goto st40
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st40:
		if p++; p == pe {
			goto _test_eof40
		}
	st_case_40:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 84:
			goto st41
		case 96:
			goto tr46
		case 116:
			goto st41
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st41:
		if p++; p == pe {
			goto _test_eof41
		}
	st_case_41:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 40:
			goto tr10
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st42:
		if p++; p == pe {
			goto _test_eof42
		}
	st_case_42:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 65:
			goto st43
		case 96:
			goto tr46
		case 97:
			goto st43
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st43:
		if p++; p == pe {
			goto _test_eof43
		}
	st_case_43:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 76:
			goto st44
		case 96:
			goto tr46
		case 108:
			goto st44
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st44:
		if p++; p == pe {
			goto _test_eof44
		}
	st_case_44:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 83:
			goto st45
		case 96:
			goto tr46
		case 115:
			goto st45
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st45:
		if p++; p == pe {
			goto _test_eof45
		}
	st_case_45:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 69:
			goto tr65
		case 96:
			goto tr46
		case 101:
			goto tr65
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st46:
		if p++; p == pe {
			goto _test_eof46
		}
	st_case_46:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 78:
			goto st47
		case 96:
			goto tr46
		case 110:
			goto st47
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st47:
		if p++; p == pe {
			goto _test_eof47
		}
	st_case_47:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 84:
			goto st48
		case 96:
			goto tr46
		case 116:
			goto st48
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st48:
		if p++; p == pe {
			goto _test_eof48
		}
	st_case_48:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 79:
			goto tr51
		case 96:
			goto tr46
		case 111:
			goto tr51
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st49:
		if p++; p == pe {
			goto _test_eof49
		}
	st_case_49:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 85:
			goto st50
		case 96:
			goto tr46
		case 117:
			goto st50
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st50:
		if p++; p == pe {
			goto _test_eof50
		}
	st_case_50:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 76:
			goto st51
		case 96:
			goto tr46
		case 108:
			goto st51
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st51:
		if p++; p == pe {
			goto _test_eof51
		}
	st_case_51:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 76:
			goto tr70
		case 96:
			goto tr46
		case 108:
			goto tr70
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st52:
		if p++; p == pe {
			goto _test_eof52
		}
	st_case_52:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 82:
			goto st53
		case 96:
			goto tr46
		case 114:
			goto st53
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st53:
		if p++; p == pe {
			goto _test_eof53
		}
	st_case_53:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 85:
			goto st54
		case 96:
			goto tr46
		case 117:
			goto st54
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st54:
		if p++; p == pe {
			goto _test_eof54
		}
	st_case_54:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 69:
			goto tr73
		case 96:
			goto tr46
		case 101:
			goto tr73
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st55:
		if p++; p == pe {
			goto _test_eof55
		}
	st_case_55:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 83:
			goto st56
		case 96:
			goto tr46
		case 115:
			goto st56
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st56:
		if p++; p == pe {
			goto _test_eof56
		}
	st_case_56:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 73:
			goto st57
		case 96:
			goto tr46
		case 105:
			goto st57
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st57:
		if p++; p == pe {
			goto _test_eof57
		}
	st_case_57:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 78:
			goto st58
		case 96:
			goto tr46
		case 110:
			goto st58
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st58:
		if p++; p == pe {
			goto _test_eof58
		}
	st_case_58:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 71:
			goto tr77
		case 96:
			goto tr46
		case 103:
			goto tr77
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
tr77:
//.... NONE:1
te = p+1

	goto st59
	st59:
		if p++; p == pe {
			goto _test_eof59
		}
	st_case_59:
//.... tmp_parser.go:1869
		switch data[p] {
		case 32:
			goto st11
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st11:
		if p++; p == pe {
			goto _test_eof11
		}
	st_case_11:
		switch data[p] {
		case 85:
			goto st12
		case 117:
			goto st12
		}
		goto tr8
	st12:
		if p++; p == pe {
			goto _test_eof12
		}
	st_case_12:
		switch data[p] {
		case 84:
			goto st13
		case 116:
			goto st13
		}
		goto tr8
	st13:
		if p++; p == pe {
			goto _test_eof13
		}
	st_case_13:
		switch data[p] {
		case 70:
			goto st14
		case 102:
			goto st14
		}
		goto tr8
	st14:
		if p++; p == pe {
			goto _test_eof14
		}
	st_case_14:
		if data[p] == 56 {
			goto st15
		}
		goto tr8
	st15:
		if p++; p == pe {
			goto _test_eof15
		}
	st_case_15:
		switch data[p] {
		case 77:
			goto st16
		case 109:
			goto st16
		}
		goto tr8
	st16:
		if p++; p == pe {
			goto _test_eof16
		}
	st_case_16:
		switch data[p] {
		case 66:
			goto st17
		case 98:
			goto st17
		}
		goto tr8
	st17:
		if p++; p == pe {
			goto _test_eof17
		}
	st_case_17:
		if data[p] == 52 {
			goto st18
		}
		goto tr8
	st18:
		if p++; p == pe {
			goto _test_eof18
		}
	st_case_18:
		if data[p] == 41 {
			goto tr10
		}
		goto tr8
	st60:
		if p++; p == pe {
			goto _test_eof60
		}
	st_case_60:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 65:
			goto st61
		case 96:
			goto tr46
		case 97:
			goto st61
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st61:
		if p++; p == pe {
			goto _test_eof61
		}
	st_case_61:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 76:
			goto st62
		case 96:
			goto tr46
		case 108:
			goto st62
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st62:
		if p++; p == pe {
			goto _test_eof62
		}
	st_case_62:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 85:
			goto st63
		case 96:
			goto tr46
		case 117:
			goto st63
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st63:
		if p++; p == pe {
			goto _test_eof63
		}
	st_case_63:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 69:
			goto st64
		case 96:
			goto tr46
		case 101:
			goto st64
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st64:
		if p++; p == pe {
			goto _test_eof64
		}
	st_case_64:
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 83:
			goto tr83
		case 96:
			goto tr46
		case 115:
			goto tr83
		}
		switch {
		case data[p] > 13:
			if 39 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
tr43:
//.... NONE:1
te = p+1

	goto st65
	st65:
		if p++; p == pe {
			goto _test_eof65
		}
	st_case_65:
//.... tmp_parser.go:2138
		switch data[p] {
		case 32:
			goto tr46
		case 34:
			goto tr46
		case 39:
			goto st19
		case 44:
			goto tr46
		case 59:
			goto tr46
		case 96:
			goto tr46
		}
		switch {
		case data[p] > 13:
			if 40 <= data[p] && data[p] <= 41 {
				goto tr46
			}
		case data[p] >= 9:
			goto tr46
		}
		goto tr26
	st19:
		if p++; p == pe {
			goto _test_eof19
		}
	st_case_19:
		if data[p] == 39 {
			goto tr22
		}
		switch {
		case data[p] < 65:
			if 48 <= data[p] && data[p] <= 57 {
				goto st19
			}
		case data[p] > 70:
			if 97 <= data[p] && data[p] <= 102 {
				goto st19
			}
		default:
			goto st19
		}
		goto tr8
	st20:
		if p++; p == pe {
			goto _test_eof20
		}
	st_case_20:
		if data[p] == 96 {
			goto tr25
		}
		goto st20
tr25:
//.... NONE:1
te = p+1

//.... lightning/mydump/parser.rl:128
act = 13;
	goto st66
	st66:
		if p++; p == pe {
			goto _test_eof66
		}
	st_case_66:
//.... tmp_parser.go:2204
		if data[p] == 96 {
			goto st20
		}
		goto tr84
	st_out:
	_test_eof21: cs = 21; goto _test_eof
	_test_eof22: cs = 22; goto _test_eof
	_test_eof1: cs = 1; goto _test_eof
	_test_eof2: cs = 2; goto _test_eof
	_test_eof23: cs = 23; goto _test_eof
	_test_eof3: cs = 3; goto _test_eof
	_test_eof4: cs = 4; goto _test_eof
	_test_eof5: cs = 5; goto _test_eof
	_test_eof24: cs = 24; goto _test_eof
	_test_eof6: cs = 6; goto _test_eof
	_test_eof25: cs = 25; goto _test_eof
	_test_eof26: cs = 26; goto _test_eof
	_test_eof7: cs = 7; goto _test_eof
	_test_eof27: cs = 27; goto _test_eof
	_test_eof28: cs = 28; goto _test_eof
	_test_eof29: cs = 29; goto _test_eof
	_test_eof8: cs = 8; goto _test_eof
	_test_eof9: cs = 9; goto _test_eof
	_test_eof30: cs = 30; goto _test_eof
	_test_eof31: cs = 31; goto _test_eof
	_test_eof32: cs = 32; goto _test_eof
	_test_eof33: cs = 33; goto _test_eof
	_test_eof34: cs = 34; goto _test_eof
	_test_eof10: cs = 10; goto _test_eof
	_test_eof35: cs = 35; goto _test_eof
	_test_eof36: cs = 36; goto _test_eof
	_test_eof37: cs = 37; goto _test_eof
	_test_eof38: cs = 38; goto _test_eof
	_test_eof39: cs = 39; goto _test_eof
	_test_eof40: cs = 40; goto _test_eof
	_test_eof41: cs = 41; goto _test_eof
	_test_eof42: cs = 42; goto _test_eof
	_test_eof43: cs = 43; goto _test_eof
	_test_eof44: cs = 44; goto _test_eof
	_test_eof45: cs = 45; goto _test_eof
	_test_eof46: cs = 46; goto _test_eof
	_test_eof47: cs = 47; goto _test_eof
	_test_eof48: cs = 48; goto _test_eof
	_test_eof49: cs = 49; goto _test_eof
	_test_eof50: cs = 50; goto _test_eof
	_test_eof51: cs = 51; goto _test_eof
	_test_eof52: cs = 52; goto _test_eof
	_test_eof53: cs = 53; goto _test_eof
	_test_eof54: cs = 54; goto _test_eof
	_test_eof55: cs = 55; goto _test_eof
	_test_eof56: cs = 56; goto _test_eof
	_test_eof57: cs = 57; goto _test_eof
	_test_eof58: cs = 58; goto _test_eof
	_test_eof59: cs = 59; goto _test_eof
	_test_eof11: cs = 11; goto _test_eof
	_test_eof12: cs = 12; goto _test_eof
	_test_eof13: cs = 13; goto _test_eof
	_test_eof14: cs = 14; goto _test_eof
	_test_eof15: cs = 15; goto _test_eof
	_test_eof16: cs = 16; goto _test_eof
	_test_eof17: cs = 17; goto _test_eof
	_test_eof18: cs = 18; goto _test_eof
	_test_eof60: cs = 60; goto _test_eof
	_test_eof61: cs = 61; goto _test_eof
	_test_eof62: cs = 62; goto _test_eof
	_test_eof63: cs = 63; goto _test_eof
	_test_eof64: cs = 64; goto _test_eof
	_test_eof65: cs = 65; goto _test_eof
	_test_eof19: cs = 19; goto _test_eof
	_test_eof20: cs = 20; goto _test_eof
	_test_eof66: cs = 66; goto _test_eof

	_test_eof: {}
	if p == eof {
		switch cs {
		case 22:
			goto tr4
		case 2:
			goto tr4
		case 23:
			goto tr44
		case 3:
			goto tr4
		case 5:
			goto tr4
		case 24:
			goto tr45
		case 6:
			goto tr4
		case 25:
			goto tr46
		case 26:
			goto tr46
		case 7:
			goto tr8
		case 27:
			goto tr48
		case 28:
			goto tr46
		case 29:
			goto tr46
		case 8:
			goto tr8
		case 9:
			goto tr8
		case 30:
			goto tr46
		case 31:
			goto tr48
		case 32:
			goto tr4
		case 33:
			goto tr4
		case 34:
			goto tr46
		case 10:
			goto tr8
		case 35:
			goto tr46
		case 36:
			goto tr46
		case 37:
			goto tr46
		case 38:
			goto tr46
		case 39:
			goto tr46
		case 40:
			goto tr46
		case 41:
			goto tr46
		case 42:
			goto tr46
		case 43:
			goto tr46
		case 44:
			goto tr46
		case 45:
			goto tr46
		case 46:
			goto tr46
		case 47:
			goto tr46
		case 48:
			goto tr46
		case 49:
			goto tr46
		case 50:
			goto tr46
		case 51:
			goto tr46
		case 52:
			goto tr46
		case 53:
			goto tr46
		case 54:
			goto tr46
		case 55:
			goto tr46
		case 56:
			goto tr46
		case 57:
			goto tr46
		case 58:
			goto tr46
		case 59:
			goto tr46
		case 11:
			goto tr8
		case 12:
			goto tr8
		case 13:
			goto tr8
		case 14:
			goto tr8
		case 15:
			goto tr8
		case 16:
			goto tr8
		case 17:
			goto tr8
		case 18:
			goto tr8
		case 60:
			goto tr46
		case 61:
			goto tr46
		case 62:
			goto tr46
		case 63:
			goto tr46
		case 64:
			goto tr46
		case 65:
			goto tr46
		case 19:
			goto tr8
		case 20:
			goto tr4
		case 66:
			goto tr84
		}
	}

	_out: {}
	}

//.... lightning/mydump/parser.rl:158

		if cs == 0 {
			log.L().Error("syntax error",
				zap.Int64("pos", parser.pos),
				zap.ByteString("content", data),
			)
			return tokNil, nil, errors.New("Syntax error")
		}

		if consumedToken != tokNil {
			result := data[ts:te]
			parser.buf = data[te:]
			parser.pos += int64(te)
			return consumedToken, result, nil
		}

		if parser.isLastChunk {
			return tokNil, nil, io.EOF
		}

		parser.buf = parser.buf[ts:]
		parser.pos += int64(ts)
		p -= ts
		te -= ts
		ts = 0
		if err := parser.readBlock(); err != nil {
			return tokNil, nil, errors.Trace(err)
		}
	}

	return tokNil, nil, nil
}
