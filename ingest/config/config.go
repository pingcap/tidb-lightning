package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb-lightning/ingest/log"
)

type DataSource struct {
	Type string `toml:"type"`
	URL  string `toml:"url"`
}

type DBStore struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Psw      string `toml:"password"`
	Database string `toml:"database"`
	SQLMode  string `toml:"sql-mode"`
}

type Config struct {
	Dir       string `toml:"dir"`
	SourceDir string `toml:"data_source_dir"`

	PdAddr string  `toml:"pd_backend"`
	TiDB   DBStore `toml:"tidb"`

	Log log.LogConfig `toml:"log"`

	ProfilePort   int     `toml:"pprof_port"`
	ProgressStore DBStore `toml:"progress_store"`

	Mydumper MydumperRuntime `toml:"mydumper"`
	KvIngest KVIngest        `toml:"kv-ingest"`

	Verify Verification `toml:"verify"`
}

type MydumperRuntime struct {
	ReadBlockSize int64 `toml:"read-block-size"`
	MinRegionSize int64 `toml:"region-min-size"`
}

type KVIngest struct {
	Backend   string `toml:"backend"`
	BatchSize int64  `toml:"batch_size"`
	Compact   bool   `toml:"compact"`
}

type Verification struct {
	RunChecksumTable bool `toml:"run_checksum_table"`
	CheckRowsCount   bool `toml:"check_rows_count"`
}

func LoadConfig(file string) (*Config, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	// set default sql_mode
	cfg.TiDB = DBStore{SQLMode: "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"}
	if err = toml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// handle mydumper
	if cfg.Mydumper.MinRegionSize <= 0 {
		cfg.Mydumper.MinRegionSize = MinRegionSize
	}
	if cfg.Mydumper.ReadBlockSize <= 0 {
		cfg.Mydumper.ReadBlockSize = ReadBlockSize
	}

	// hendle kv ingest
	if cfg.KvIngest.BatchSize <= 0 {
		cfg.KvIngest.BatchSize = KVMaxBatchSize
	}

	return cfg, nil
}
