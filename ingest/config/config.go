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
	Psw      string `toml:"password"`
	Database string `toml:"database"`
}

type Config struct {
	Dir           string  `toml:"dir"`
	SourceDir     string  `toml:"data_source_dir"`
	PdAddr        string  `toml:"pd_backend"`
	KvDeliverAddr string  `toml:"kv_import_backend"`
	TiDB          DBStore `toml:"tidb"`
	ProgressStore DBStore `toml:"progress_store"`
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

	return cfg, nil
}
