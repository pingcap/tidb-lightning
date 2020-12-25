// Copyright 2020 PingCAP, Inc.
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

package log

import (
	"fmt"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitRedact inits the enableRedactLog
func InitRedact(redactLog bool) {
	errors.RedactLogEnabled.Store(redactLog)
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	return errors.RedactLogEnabled.Load()
}

// ZapRedactBinary receives zap.Binary and return omitted information if redact log enabled
func ZapRedactBinary(key string, val []byte) zapcore.Field {
	if NeedRedact() {
		return zap.String(key, "?")
	}
	return zap.Binary(key, val)
}

// ZapRedactArray receives zap.Array and return omitted information if redact log enabled
func ZapRedactArray(key string, val zapcore.ArrayMarshaler) zapcore.Field {
	if NeedRedact() {
		return zap.String(key, "?")
	}
	return zap.Array(key, val)
}

// ZapRedactReflect receives zap.Reflect and return omitted information if redact log enabled
func ZapRedactReflect(key string, val interface{}) zapcore.Field {
	if NeedRedact() {
		return zap.String(key, "?")
	}
	return zap.Reflect(key, val)
}

// ZapRedactStringer receives stringer argument and return omitted information in zap.Field  if redact log enabled
func ZapRedactStringer(key string, arg fmt.Stringer) zap.Field {
	return zap.Stringer(key, RedactStringer(arg))
}

// ZapRedactString receives stringer argument and return omitted information in zap.Field  if redact log enabled
func ZapRedactString(key string, arg string) zap.Field {
	return zap.String(key, RedactString(arg))
}

// RedactString receives string argument and return omitted information if redact log enabled
func RedactString(arg string) string {
	if NeedRedact() {
		return "?"
	}
	return arg
}

// RedactStringer receives stringer argument and return omitted information if redact log enabled
func RedactStringer(arg fmt.Stringer) fmt.Stringer {
	if NeedRedact() {
		return stringer{}
	}
	return arg
}

type stringer struct{}

// String implement fmt.Stringer
func (s stringer) String() string {
	return "?"
}
