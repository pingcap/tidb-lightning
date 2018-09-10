package common

import (
	"context"
	"fmt"
	"os"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/pingcap/tidb/util/logutil"
)

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize uint `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays uint `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups uint `toml:"max-backups" json:"max-backups"`
}

// AppLogger is a logger for lightning, different from tidb logger.
var AppLogger = log.StandardLogger()

func InitLogger(cfg *LogConfig, tidbLoglevel string) error {
	return errors.Trace(logutil.InitLogger(&logutil.LogConfig{
		Level:            tidbLoglevel,
		Format:           "text",
		DisableTimestamp: false,
		File: logutil.FileLogConfig{
			Filename:   cfg.File,
			LogRotate:  true,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
	}))
}

var (
	copyCapturedErrorsLogRequestChannel chan<- struct{}
	copyCapturedErrorsLogReplyChannel   <-chan []string
)

type capturedErrorLogsHook struct {
	ch chan<- *log.Entry
}

func (hook *capturedErrorLogsHook) Fire(entry *log.Entry) error {
	hook.ch <- entry
	return nil
}
func (hook *capturedErrorLogsHook) Levels() []log.Level {
	return []log.Level{log.ErrorLevel, log.FatalLevel}
}

func CaptureErrorLogs(ctx context.Context) {
	entryCh := make(chan *log.Entry, 16)
	logReqCh := make(chan struct{}, 1)
	logReplyCh := make(chan []string, 1)

	copyCapturedErrorsLogRequestChannel = logReqCh
	copyCapturedErrorsLogReplyChannel = logReplyCh

	go func() {
		errorLogs := make([]string, 0, 32)
		for {
			select {
			case <-ctx.Done():
				logReplyCh <- append(errorLogs, "[User terminated]")
				return
			case entry := <-entryCh:
				msg, _ := entry.String()
				errorLogs = append(errorLogs, msg)
			case <-logReqCh:
				logReplyCh <- errorLogs
			}
		}
	}()

	log.AddHook(&capturedErrorLogsHook{ch: entryCh})
}

func PrintErrorLogs() {
	copyCapturedErrorsLogRequestChannel <- struct{}{}
	errorLogs := <-copyCapturedErrorsLogReplyChannel

	// \x1b[1;31m = make the subsequent text bold red
	// \x1b[1;32m = make the subsequent text bold green
	// \x1b[0m    = reset colors

	errorLogsLen := len(errorLogs)
	if errorLogsLen == 0 {
		fmt.Fprintln(os.Stderr, "\x1b[1;32mThe whole process completed successfully!\x1b[0m")
		return
	}

	fmt.Fprintf(os.Stderr, "\x1b[1;31mEncountered %d errors in the whole operation\x1b[0m\n", errorLogsLen)
	for _, log := range errorLogs {
		fmt.Fprintln(os.Stderr, log)
	}
}
