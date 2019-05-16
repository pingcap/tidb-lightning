package restore

import (
	"github.com/pingcap/tidb-lightning/lightning/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// makeTestLogger creates a Logger instance which produces JSON logs.
func makeTestLogger() (log.Logger, *zaptest.Buffer) {
	buffer := new(zaptest.Buffer)
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			LevelKey:       "$lvl",
			MessageKey:     "$msg",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		}),
		buffer,
		zap.DebugLevel,
	))
	return log.Logger{Logger: logger}, buffer
}
