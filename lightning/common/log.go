package common

import (
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
