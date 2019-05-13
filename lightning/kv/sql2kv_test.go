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

package kv

import (
	"bytes"
	"fmt"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap/zapcore"
)

type mockEncoder struct {
	zapcore.ArrayEncoder
	zapcore.ObjectEncoder
	buf *bytes.Buffer
}

func (enc *mockEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	err := obj.MarshalLogObject(enc)
	return err
}

func (enc *mockEncoder) AddInt(key string, value int) {
	enc.buf.WriteString(fmt.Sprintf("[%v=%v]", key, value))
}

func (enc *mockEncoder) AddString(key string, value string) {
	enc.buf.WriteString(fmt.Sprintf("[%v=%v]", key, value))
}

func (s *kvSuite) TestMarshal(c *C) {
	nullDatum := types.Datum{}
	nullDatum.SetNull()
	minNotNull := types.Datum{}
	minNotNull.SetMinNotNull()
	datums := rowArrayMarshaler{types.NewStringDatum("1"), nullDatum, minNotNull, types.MaxValueDatum()}
	encoder := &mockEncoder{buf: &bytes.Buffer{}}
	err := datums.MarshalLogArray(encoder)
	c.Assert(err, IsNil)
	c.Assert(encoder.buf.String(), Equals, "[col=0][kind=string][val=1][col=1][kind=null][val=NULL][col=2][kind=min][val=-inf][col=3][kind=max][val=+inf]")

	invalid := []types.Datum{nullDatum, minNotNull}
	pkind := (*byte)(unsafe.Pointer(&invalid[1]))
	*pkind = 0x55
	encoder.buf.Reset()
	err = rowArrayMarshaler(invalid).MarshalLogArray(encoder)
	c.Assert(err, ErrorMatches, "cannot convert.*")
	c.Assert(encoder.buf.String(), Equals, "[col=0][kind=null][val=NULL]")
}
