package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
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

	PdAddr        string  `toml:"pd_backend"`
	KvDeliverAddr string  `toml:"kv_import_backend"`
	TiDB          DBStore `toml:"tidb"`

	ProfilePort   string  `toml:"pprof_port"`
	ProgressStore DBStore `toml:"progress_store"`

	Mydump MydumperRuntime  `toml:"mydumper"`
	KvDev  KVDeliverRuntime `toml:"kv-ingest"`
}

type MydumperRuntime struct {
	ReadBlockSize int64 `toml:"read-block-size"`
	MinRegionSize int64 `toml:"region-min-size"`
}

type KVDeliverRuntime struct {
	MaxFlushSize int64 `toml:"max-flush-size"`
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

	// TODO ... adjust
	// cfg.Mydump.MinRegionSize = MinRegionSize
	// cfg.Mydump.ReadBlockSize = ReadBlockSize
	cfg.KvDev.MaxFlushSize = MaxFlushSize

	return cfg, nil
}
