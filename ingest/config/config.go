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
	Pwd      string `toml:"password"`
	Database string `toml:"database"`
}

type Config struct {
	Dir       string `toml:"dir"`
	SourceDir string `toml:"data_source_dir"`

	PdAddr string  `toml:"pd_backend"`
	TiDB   DBStore `toml:"tidb"`

	Log log.LogConfig `toml:"log"`

	ProfilePort   string  `toml:"pprof_port"`
	ProgressStore DBStore `toml:"progress_store"`

	Mydump   MydumperRuntime `toml:"mydumper"`
	KvIngest KVIngest        `toml:"kv-ingest"`

	Verfiy Verification `toml:"verify"`
}

type MydumperRuntime struct {
	ReadBlockSize int64 `toml:"read-block-size"`
	MinRegionSize int64 `toml:"region-min-size"`
}

type KVIngest struct {
	// KvDeliverAddr string  `toml:"kv_import_backend"`
	Backend   string `toml:"backend"`
	BatchSize int64  `toml:"batch_size"`
}

type Verification struct {
	RunCheckTable bool `toml:"run_check_table"`
}

func LoadConfig(file string) (*Config, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	if err = toml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// handle mydumper
	if cfg.Mydump.MinRegionSize <= 0 {
		cfg.Mydump.MinRegionSize = MinRegionSize
	}
	if cfg.Mydump.ReadBlockSize <= 0 {
		cfg.Mydump.ReadBlockSize = ReadBlockSize
	}

	// hendle kv ingest
	if cfg.KvIngest.BatchSize <= 0 {
		cfg.KvIngest.BatchSize = KVMaxBatchSize
	}

	return cfg, nil
}
