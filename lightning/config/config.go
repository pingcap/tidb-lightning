package config

import (
	"flag"
	"io/ioutil"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

type DBStore struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Psw      string `toml:"password"`
	PdAddr   string `toml:"pd-addr"`
	SQLMode  string `toml:"sql-mode"`
	LogLevel string `toml:"log-level"`
}

type Config struct {
	*flag.FlagSet `json:"-"`

	App  Lightning `toml:"lightning"`
	TiDB DBStore   `toml:"tidb"`

	// not implemented yet.
	ProgressStore DBStore `toml:"progress-store"`

	Mydumper     MydumperRuntime `toml:"mydumper"`
	ImportServer ImportServer    `toml:"import-server"`
	PostRestore  PostRestore     `toml:"post-restore"`

	// command line flags
	ConfigFile string
	DoCompact  bool
}

type Lightning struct {
	log.LogConfig
	NumCPU      int `toml:"num-cpu"`
	ProfilePort int `toml:"pprof-port"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Compact  bool `toml:"compact"`
	Checksum bool `toml:"checksum"`
	Analyze  bool `toml:"analyze"`
}

type MydumperRuntime struct {
	ReadBlockSize int64  `toml:"read-block-size"`
	MinRegionSize int64  `toml:"region-min-size"`
	SourceDir     string `toml:"data-source-dir"`
}

type ImportServer struct {
	Addr      string `toml:"addr"`
	BatchSize int64  `toml:"batch-size"`
}

func LoadConfig(args []string) (*Config, error) {
	cfg := new(Config)

	// set default num-cpu
	cfg.App = Lightning{NumCPU: runtime.NumCPU()}

	// set default sql-mode
	cfg.TiDB = DBStore{SQLMode: "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"}

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
	if cfg.Mydumper.MinRegionSize <= 0 {
		cfg.Mydumper.MinRegionSize = MinRegionSize
	}
	if cfg.Mydumper.ReadBlockSize <= 0 {
		cfg.Mydumper.ReadBlockSize = ReadBlockSize
	}

	// hendle kv import
	if cfg.ImportServer.BatchSize <= 0 {
		cfg.ImportServer.BatchSize = KVMaxBatchSize
	}

	return cfg, nil
}
