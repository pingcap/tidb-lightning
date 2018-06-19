package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
)

// DBStore contains tidb informations.
type DBStore struct {
	Host                   string `toml:"host" json:"host"`
	Port                   int    `toml:"port" json:"port"`
	User                   string `toml:"user" json:"user"`
	Psw                    string `toml:"password" json:"-"`
	StatusPort             int    `toml:"status-port" json:"status-port"`
	PdAddr                 string `toml:"pd-addr" json:"pd-addr"`
	SQLMode                string `toml:"sql-mode" json:"sql-mode"`
	LogLevel               string `toml:"log-level" json:"log-level"`
	DistSQLScanConcurrency int    `toml:"distsql-scan-concurrency" json:"distsql-scan-concurrency"`
}

// Config represents configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	// not implemented yet.
	ProgressStore DBStore `toml:"progress-store" json:"progress-store"`

	DataSource   DataSource   `toml:"data-source" json:"data-source"`
	TikvImporter TikvImporter `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore  `toml:"post-restore" json:"post-restore"`

	// command line flags
	ConfigFile   string `json:"config-file"`
	DoCompact    bool   `json:"-"`
	printVersion bool
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		common.AppLogger.Errorf("marshal config to json error %v", err)
	}
	return string(bytes)
}

// Lightning contains the configuration about the application itself.
type Lightning struct {
	common.LogConfig
	ProfilePort    int `toml:"pprof-port" json:"json"`
	WorkerPoolSize int `toml:"worker-pool-size" json:"worker-pool-size"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Compact  bool `toml:"compact" json:"compact"`
	Checksum bool `toml:"checksum" json:"checksum"`
	Analyze  bool `toml:"analyze" json:"analyze"`
}

// DataSource represents a datasource.
type DataSource struct {
	SourceType      string `toml:"data-source-type" json:"data-source-type"`
	SourceDir       string `toml:"data-source-dir" json:"data-source-dir"`
	ReadBlockSize   int64  `toml:"read-block-size" json:"read-block-size"`
	MinRegionSize   int64  `toml:"region-min-size" json:"region-min-size"`
	NoSchema        bool   `toml:"no-schema" json:"no-schema"`
	Batch           int64  `toml:"batch" json:"batch"`
	IgnoreFirstLine bool   `toml:"ignore-first-line" json:"ignore-first-line"`
}

// TikvImporter contains configuration for tikv-importer.
type TikvImporter struct {
	Addr      string `toml:"addr" json:"addr"`
	BatchSize int64  `toml:"batch-size" json:"batch-size"`
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
	fs.BoolVar(&cfg.printVersion, "V", false, "print version of lightning")

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}
	if cfg.printVersion {
		fmt.Println(common.GetRawInfo())
		return nil, flag.ErrHelp
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
