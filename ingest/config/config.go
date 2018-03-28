package config

import (
	"flag"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
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
}

type Config struct {
	*flag.FlagSet `json:"-"`

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

	DoCompact  bool
	DoChecksum string
	file       string
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

func LoadConfig(args []string) (*Config, error) {
	cfg := new(Config)
	cfg.FlagSet = flag.NewFlagSet("lightning", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.file, "c", "tidb-lightning.toml", "tidb-lightning configuration file")
	fs.BoolVar(&cfg.DoCompact, "compact", false, "do manual compact")
	fs.StringVar(&cfg.DoChecksum, "checksum", "", "do manual checksum for tables which in comma separated format, like foo.bar1,foo.bar2")

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	data, err := ioutil.ReadFile(cfg.file)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = toml.Unmarshal(data, cfg); err != nil {
		return nil, errors.Trace(err)
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
