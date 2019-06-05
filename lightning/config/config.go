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

package config

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	tidbcfg "github.com/pingcap/tidb/config"
)

const (
	// ImportMode defines mode of import for tikv.
	ImportMode = "import"
	// NormalMode defines mode of normal for tikv.
	NormalMode = "normal"
)

var defaultConfigPaths = []string{"tidb-lightning.toml", "conf/tidb-lightning.toml"}

type DBStore struct {
	Host       string `toml:"host" json:"host"`
	Port       int    `toml:"port" json:"port"`
	User       string `toml:"user" json:"user"`
	Psw        string `toml:"password" json:"-"`
	StatusPort int    `toml:"status-port" json:"status-port"`
	PdAddr     string `toml:"pd-addr" json:"pd-addr"`
	StrSQLMode string `toml:"sql-mode" json:"sql-mode"`

	SQLMode mysql.SQLMode `toml:"-" json:"-"`

	DistSQLScanConcurrency     int `toml:"distsql-scan-concurrency" json:"distsql-scan-concurrency"`
	BuildStatsConcurrency      int `toml:"build-stats-concurrency" json:"build-stats-concurrency"`
	IndexSerialScanConcurrency int `toml:"index-serial-scan-concurrency" json:"index-serial-scan-concurrency"`
	ChecksumTableConcurrency   int `toml:"checksum-table-concurrency" json:"checksum-table-concurrency"`
}

type Config struct {
	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	Checkpoint   Checkpoint          `toml:"checkpoint" json:"checkpoint"`
	Mydumper     MydumperRuntime     `toml:"mydumper" json:"mydumper"`
	BWList       *filter.Rules       `toml:"black-white-list" json:"black-white-list"`
	TikvImporter TikvImporter        `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore         `toml:"post-restore" json:"post-restore"`
	Cron         Cron                `toml:"cron" json:"cron"`
	Routes       []*router.TableRule `toml:"routes" json:"routes"`
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal config to json error", log.ShortError(err))
	}
	return string(bytes)
}

type Lightning struct {
	TableConcurrency  int  `toml:"table-concurrency" json:"table-concurrency"`
	IndexConcurrency  int  `toml:"index-concurrency" json:"index-concurrency"`
	RegionConcurrency int  `toml:"region-concurrency" json:"region-concurrency"`
	IOConcurrency     int  `toml:"io-concurrency" json:"io-concurrency"`
	CheckRequirements bool `toml:"check-requirements" json:"check-requirements"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Level1Compact bool `toml:"level-1-compact" json:"level-1-compact"`
	Compact       bool `toml:"compact" json:"compact"`
	Checksum      bool `toml:"checksum" json:"checksum"`
	Analyze       bool `toml:"analyze" json:"analyze"`
}

type CSVConfig struct {
	Separator       string `toml:"separator" json:"separator"`
	Delimiter       string `toml:"delimiter" json:"delimiter"`
	Header          bool   `toml:"header" json:"header"`
	TrimLastSep     bool   `toml:"trim-last-separator" json:"trim-last-separator"`
	NotNull         bool   `toml:"not-null" json:"not-null"`
	Null            string `toml:"null" json:"null"`
	BackslashEscape bool   `toml:"backslash-escape" json:"backslash-escape"`
}

type MydumperRuntime struct {
	ReadBlockSize    int64     `toml:"read-block-size" json:"read-block-size"`
	BatchSize        int64     `toml:"batch-size" json:"batch-size"`
	BatchImportRatio float64   `toml:"batch-import-ratio" json:"batch-import-ratio"`
	SourceDir        string    `toml:"data-source-dir" json:"data-source-dir"`
	NoSchema         bool      `toml:"no-schema" json:"no-schema"`
	CharacterSet     string    `toml:"character-set" json:"character-set"`
	CSV              CSVConfig `toml:"csv" json:"csv"`
	CaseSensitive    bool      `toml:"case-sensitive" json:"case-sensitive"`
}

type TikvImporter struct {
	Addr string `toml:"addr" json:"addr"`
}

type Checkpoint struct {
	Enable           bool   `toml:"enable" json:"enable"`
	Schema           string `toml:"schema" json:"schema"`
	DSN              string `toml:"dsn" json:"-"` // DSN may contain password, don't expose this to JSON.
	Driver           string `toml:"driver" json:"driver"`
	KeepAfterSuccess bool   `toml:"keep-after-success" json:"keep-after-success"`
}

type Cron struct {
	SwitchMode  Duration `toml:"switch-mode" json:"switch-mode"`
	LogProgress Duration `toml:"log-progress" json:"log-progress"`
}

// A duration which can be deserialized from a TOML string.
// Implemented as https://github.com/BurntSushi/toml#using-the-encodingtextunmarshaler-interface
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.Duration)), nil
}

func NewConfig() *Config {
	return &Config{
		App: Lightning{
			RegionConcurrency: runtime.NumCPU(),
			TableConcurrency:  6,
			IndexConcurrency:  2,
			IOConcurrency:     5,
			CheckRequirements: true,
		},
		TiDB: DBStore{
			Host:                       "127.0.0.1",
			User:                       "root",
			StatusPort:                 10080,
			StrSQLMode:                 mysql.DefaultSQLMode,
			BuildStatsConcurrency:      20,
			DistSQLScanConcurrency:     100,
			IndexSerialScanConcurrency: 20,
			ChecksumTableConcurrency:   16,
		},
		Cron: Cron{
			SwitchMode:  Duration{Duration: 5 * time.Minute},
			LogProgress: Duration{Duration: 5 * time.Minute},
		},
		Mydumper: MydumperRuntime{
			ReadBlockSize: ReadBlockSize,
			CSV: CSVConfig{
				Separator: ",",
				Delimiter: `"`,
			},
		},
		PostRestore: PostRestore{
			Checksum: true,
		},
	}
}

// LoadFromGlobal resets the current configuration to the global settings.
func (cfg *Config) LoadFromGlobal(global *GlobalConfig) error {
	if err := cfg.LoadFromTOML(global.ConfigFileContent); err != nil {
		return err
	}

	cfg.TiDB.Host = global.TiDB.Host
	cfg.TiDB.Port = global.TiDB.Port
	cfg.TiDB.User = global.TiDB.User
	cfg.TiDB.StatusPort = global.TiDB.StatusPort
	cfg.TiDB.PdAddr = global.TiDB.PdAddr
	cfg.Mydumper.SourceDir = global.Mydumper.SourceDir
	cfg.TikvImporter.Addr = global.TikvImporter.Addr

	return nil
}

// LoadFromTOML overwrites the current configuration by the TOML data
func (cfg *Config) LoadFromTOML(data []byte) error {
	return errors.Trace(toml.Unmarshal(data, cfg))
}

// Adjust fixes the invalid or unspecified settings to reasonable valid values.
func (cfg *Config) Adjust() error {
	// Reject problematic CSV configurations.
	csv := &cfg.Mydumper.CSV
	if len(csv.Separator) != 1 {
		return errors.New("invalid config: `mydumper.csv.separator` must be exactly one byte long")
	}

	if len(csv.Delimiter) > 1 {
		return errors.New("invalid config: `mydumper.csv.delimiter` must be one byte long or empty")
	}

	if csv.Separator == csv.Delimiter {
		return errors.New("invalid config: cannot use the same character for both CSV delimiter and separator")
	}

	if csv.BackslashEscape {
		if csv.Separator == `\` {
			return errors.New("invalid config: cannot use '\\' as CSV separator when `mydumper.csv.backslash-escape` is true")
		}
		if csv.Delimiter == `\` {
			return errors.New("invalid config: cannot use '\\' as CSV delimiter when `mydumper.csv.backslash-escape` is true")
		}
	}

	var err error
	cfg.TiDB.SQLMode, err = mysql.GetSQLMode(cfg.TiDB.StrSQLMode)
	if err != nil {
		return errors.Annotate(err, "invalid config: `mydumper.tidb.sql_mode` must be a valid SQL_MODE")
	}

	for _, rule := range cfg.Routes {
		if !cfg.Mydumper.CaseSensitive {
			rule.ToLower()
		}
		if err := rule.Valid(); err != nil {
			return errors.Trace(err)
		}
	}

	// automatically determine the TiDB port & PD address from TiDB settings
	if cfg.TiDB.Port <= 0 || len(cfg.TiDB.PdAddr) == 0 {
		resp, err := http.Get(fmt.Sprintf("http://%s:%d/settings", cfg.TiDB.Host, cfg.TiDB.StatusPort))
		if err != nil {
			return errors.Annotate(err, "cannot fetch settings from TiDB, please manually fill in `tidb.port` and `tidb.pd-addr`")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("TiDB settings returned %s, please manually fill in `tidb.port` and `tidb.pd-addr`", resp.Status)
		}
		var settings tidbcfg.Config
		err = json.NewDecoder(resp.Body).Decode(&settings)
		if err != nil {
			return errors.Annotate(err, "cannot decode settings from TiDB, please manually fill in `tidb.port` and `tidb.pd-addr`")
		}
		if cfg.TiDB.Port <= 0 {
			cfg.TiDB.Port = int(settings.Port)
			if cfg.TiDB.Port <= 0 {
				return errors.New("invalid `tidb.port` setting")
			}
		}
		if len(cfg.TiDB.PdAddr) == 0 {
			pdAddrs := strings.Split(settings.Path, ",")
			cfg.TiDB.PdAddr = pdAddrs[0] // FIXME support multiple PDs once importer can.
			if len(cfg.TiDB.PdAddr) == 0 {
				return errors.New("invalid `tidb.pd-addr` setting")
			}
		}
	}

	// handle mydumper
	if cfg.Mydumper.BatchSize <= 0 {
		cfg.Mydumper.BatchSize = 100 * _G
	}
	if cfg.Mydumper.BatchImportRatio < 0.0 || cfg.Mydumper.BatchImportRatio >= 1.0 {
		cfg.Mydumper.BatchImportRatio = 0.75
	}
	if cfg.Mydumper.ReadBlockSize <= 0 {
		cfg.Mydumper.ReadBlockSize = ReadBlockSize
	}
	if len(cfg.Mydumper.CharacterSet) == 0 {
		cfg.Mydumper.CharacterSet = "auto"
	}

	if len(cfg.Checkpoint.Schema) == 0 {
		cfg.Checkpoint.Schema = "tidb_lightning_checkpoint"
	}
	if len(cfg.Checkpoint.Driver) == 0 {
		cfg.Checkpoint.Driver = "file"
	}
	if len(cfg.Checkpoint.DSN) == 0 {
		switch cfg.Checkpoint.Driver {
		case "mysql":
			cfg.Checkpoint.DSN = common.ToDSN(cfg.TiDB.Host, cfg.TiDB.Port, cfg.TiDB.User, cfg.TiDB.Psw)
		case "file":
			cfg.Checkpoint.DSN = "/tmp/" + cfg.Checkpoint.Schema + ".pb"
		}
	}

	return nil
}
