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
	"flag"
	"fmt"
	"io/ioutil"
	"runtime"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

const (
	// ImportMode defines mode of import for tikv.
	ImportMode = "import"
	// NormalMode defines mode of normal for tikv.
	NormalMode = "normal"
)

type DBStore struct {
	Host       string `toml:"host" json:"host"`
	Port       int    `toml:"port" json:"port"`
	User       string `toml:"user" json:"user"`
	Psw        string `toml:"password" json:"-"`
	StatusPort int    `toml:"status-port" json:"status-port"`
	PdAddr     string `toml:"pd-addr" json:"pd-addr"`
	SQLMode    string `toml:"sql-mode" json:"sql-mode"`
	LogLevel   string `toml:"log-level" json:"log-level"`

	DistSQLScanConcurrency     int `toml:"distsql-scan-concurrency" json:"distsql-scan-concurrency"`
	BuildStatsConcurrency      int `toml:"build-stats-concurrency" json:"build-stats-concurrency"`
	IndexSerialScanConcurrency int `toml:"index-serial-scan-concurrency" json:"index-serial-scan-concurrency"`
	ChecksumTableConcurrency   int `toml:"checksum-table-concurrency" json:"checksum-table-concurrency"`
}

type Config struct {
	*flag.FlagSet `json:"-"`

	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	// not implemented yet.
	// ProgressStore DBStore `toml:"progress-store" json:"progress-store"`
	Checkpoint   Checkpoint      `toml:"checkpoint" json:"checkpoint"`
	Mydumper     MydumperRuntime `toml:"mydumper" json:"mydumper"`
	BWList       *filter.Rules   `toml:"black-white-list" json:"black-white-list"`
	TikvImporter TikvImporter    `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore     `toml:"post-restore" json:"post-restore"`
	Cron         Cron            `toml:"cron" json:"cron"`

	// command line flags
	ConfigFile   string `json:"config-file"`
	DoCompact    bool   `json:"-"`
	SwitchMode   string `json:"-"`
	printVersion bool
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		common.AppLogger.Errorf("marshal config to json error %v", err)
	}
	return string(bytes)
}

type Lightning struct {
	common.LogConfig
	TableConcurrency  int  `toml:"table-concurrency" json:"table-concurrency"`
	RegionConcurrency int  `toml:"region-concurrency" json:"region-concurrency"`
	IOConcurrency     int  `toml:"io-concurrency" json:"io-concurrency"`
	ProfilePort       int  `toml:"pprof-port" json:"pprof-port"`
	CheckRequirements bool `toml:"check-requirements" json:"check-requirements"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Level1Compact *bool `toml:"level-1-compact" json:"level-1-compact"`
	Compact       bool  `toml:"compact" json:"compact"`
	Checksum      bool  `toml:"checksum" json:"checksum"`
	Analyze       bool  `toml:"analyze" json:"analyze"`
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
			TableConcurrency:  8,
			IOConcurrency:     5,
			CheckRequirements: true,
		},
		TiDB: DBStore{
			SQLMode:                    "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
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
			CSV: CSVConfig{
				Separator: ",",
			},
		},
	}
}

func LoadConfig(args []string) (*Config, error) {
	cfg := NewConfig()

	cfg.FlagSet = flag.NewFlagSet("lightning", flag.ContinueOnError)
	fs := cfg.FlagSet

	// if both `-c` and `-config` are specified, the last one in the command line will take effect.
	// the default value is assigned immediately after the StringVar() call,
	// so it is fine to not give any default value for `-c`, to keep the `-h` page clean.
	fs.StringVar(&cfg.ConfigFile, "c", "", "(deprecated alias of -config)")
	fs.StringVar(&cfg.ConfigFile, "config", "tidb-lightning.toml", "tidb-lightning configuration file")
	fs.BoolVar(&cfg.DoCompact, "compact", false, "do manual compaction on the target cluster, run then exit")
	fs.StringVar(&cfg.SwitchMode, "switch-mode", "", "switch tikv into import mode or normal mode, values can be ['import', 'normal'], run then exit")
	fs.BoolVar(&cfg.printVersion, "V", false, "print version of lightning")

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	if err := cfg.Load(); err != nil {
		return nil, errors.Trace(err)
	}
	return cfg, nil
}

func (cfg *Config) Load() error {
	if cfg.printVersion {
		fmt.Println(common.GetRawInfo())
		return flag.ErrHelp
	}

	data, err := ioutil.ReadFile(cfg.ConfigFile)
	if err != nil {
		return errors.Trace(err)
	}
	if err = toml.Unmarshal(data, cfg); err != nil {
		return errors.Trace(err)
	}

	if len(cfg.Mydumper.CSV.Separator) != 1 {
		return errors.New("invalid config: `mydumper.csv.separator` must be exactly one byte long")
	}

	if len(cfg.Mydumper.CSV.Delimiter) > 1 {
		return errors.New("invalid config: `mydumper.csv.delimiter` must be one byte long or empty")
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

	// If the level 1 compact configuration not found, default to true
	if cfg.PostRestore.Level1Compact == nil {
		cfg.PostRestore.Level1Compact = new(bool)
		*cfg.PostRestore.Level1Compact = true
	}

	return nil
}
