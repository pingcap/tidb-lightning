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

const (
	// ImportMode defines mode of import for tikv.
	ImportMode = "import"
	// NormalMode defines mode of normal for tikv.
	NormalMode = "normal"
)

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

type Config struct {
	*flag.FlagSet `json:"-"`

	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	// not implemented yet.
	// ProgressStore DBStore `toml:"progress-store" json:"progress-store"`

	Mydumper     MydumperRuntime `toml:"mydumper" json:"mydumper"`
	TikvImporter TikvImporter    `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore     `toml:"post-restore" json:"post-restore"`

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
	ProfilePort       int  `toml:"pprof-port" json:"pprof-port"`
	CheckRequirements bool `toml:"check-requirements" json:"check-requirements"`
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Compact  bool `toml:"compact" json:"compact"`
	Checksum bool `toml:"checksum" json:"checksum"`
	Analyze  bool `toml:"analyze" json:"analyze"`
}

type MydumperRuntime struct {
	ReadBlockSize int64  `toml:"read-block-size" json:"read-block-size"`
	MinRegionSize int64  `toml:"region-min-size" json:"region-min-size"`
	SourceDir     string `toml:"data-source-dir" json:"data-source-dir"`
	NoSchema      bool   `toml:"no-schema" json:"no-schema"`
}

type TikvImporter struct {
	Addr      string `toml:"addr" json:"addr"`
	BatchSize int64  `toml:"batch-size" json:"batch-size"`
}

func NewConfig() *Config {
	return &Config{
		App: Lightning{
			RegionConcurrency: runtime.NumCPU(),
			TableConcurrency:  8,
			CheckRequirements: true,
		},
		TiDB: DBStore{
			SQLMode:                "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
			DistSQLScanConcurrency: 16,
		},
	}
}

func LoadConfig(args []string) (*Config, error) {
	cfg := NewConfig()

	cfg.FlagSet = flag.NewFlagSet("lightning", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "c", "tidb-lightning.toml", "tidb-lightning configuration file")
	fs.BoolVar(&cfg.DoCompact, "compact", false, "do manual compaction on the target cluster, run then exit")
	fs.StringVar(&cfg.SwitchMode, "switch-mode", "", "switch tikv into import mode or normal mode, values can be ['import', 'normal'], run then exit")
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
	if cfg.Mydumper.MinRegionSize <= 0 {
		cfg.Mydumper.MinRegionSize = MinRegionSize
	}
	if cfg.Mydumper.ReadBlockSize <= 0 {
		cfg.Mydumper.ReadBlockSize = ReadBlockSize
	}

	// hendle kv import
	if cfg.TikvImporter.BatchSize <= 0 {
		cfg.TikvImporter.BatchSize = KVMaxBatchSize
	}

	return cfg, nil
}
