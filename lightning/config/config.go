package config

import (
	"flag"
	"io/ioutil"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

// DBStore contains tidb informations.
type DBStore struct {
	Host                   string `toml:"host"`
	Port                   int    `toml:"port"`
	User                   string `toml:"user"`
	Psw                    string `toml:"password"`
	StatusPort             int    `toml:"status-port"`
	PdAddr                 string `toml:"pd-addr"`
	SQLMode                string `toml:"sql-mode"`
	LogLevel               string `toml:"log-level"`
	DistSQLScanConcurrency int    `toml:"distsql-scan-concurrency"`
}

// Config represents configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	App  Lightning `toml:"lightning"`
	TiDB DBStore   `toml:"tidb"`

	// not implemented yet.
	ProgressStore DBStore `toml:"progress-store"`

	DataSource   DataSource   `toml:"data-source"`
	TikvImporter TikvImporter `toml:"tikv-importer"`
	PostRestore  PostRestore  `toml:"post-restore"`

	// command line flags
	ConfigFile string
	DoCompact  bool
}

// Lightning contains the configuration about the application itself.
type Lightning struct {
	log.LogConfig
	ProfilePort    int `toml:"pprof-port"`
	WorkerPoolSize int `toml:"worker-pool-size"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Compact  bool `toml:"compact"`
	Checksum bool `toml:"checksum"`
	Analyze  bool `toml:"analyze"`
}

// DataSource represents a datasource.
type DataSource struct {
	SourceType      string `toml:"data-source-type"`
	SourceDir       string `toml:"data-source-dir"`
	ReadBlockSize   int64  `toml:"read-block-size"`
	MinRegionSize   int64  `toml:"region-min-size"`
	NoSchema        bool   `toml:"no-schema"`
	Batch           int64  `toml:"batch"`
	IgnoreFirstLine bool   `toml:"ignore-first-line"`
}

// TikvImporter contains configuration for tikv-importer.
type TikvImporter struct {
	Addr      string `toml:"addr"`
	BatchSize int64  `toml:"batch-size"`
}

// NewConfig returns a Config instance.
func NewConfig() *Config {
	return &Config{
		App: Lightning{
			WorkerPoolSize: runtime.NumCPU(),
		},
		TiDB: DBStore{
			SQLMode:                "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
			DistSQLScanConcurrency: 16,
		},
	}
}

// LoadConfig loads configuration from command line flags and configurations file.
func LoadConfig(args []string) (*Config, error) {
	cfg := NewConfig()

	cfg.FlagSet = flag.NewFlagSet("lightning", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "c", "tidb-lightning.toml", "tidb-lightning configuration file")
	fs.BoolVar(&cfg.DoCompact, "compact", false, "do manual compaction on the target cluster")

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	data, err := ioutil.ReadFile(cfg.ConfigFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = toml.Unmarshal(data, cfg); err != nil {
		return nil, errors.Trace(err)
	}

	// handle mydumper
	if cfg.DataSource.MinRegionSize <= 0 {
		cfg.DataSource.MinRegionSize = defaultMinRegionSize
	}
	if cfg.DataSource.ReadBlockSize <= 0 {
		cfg.DataSource.ReadBlockSize = defaultReadBlockSize
	}

	// handle kv import
	if cfg.TikvImporter.BatchSize <= 0 {
		cfg.TikvImporter.BatchSize = kvMaxBatchSize
	}

	return cfg, nil
}
