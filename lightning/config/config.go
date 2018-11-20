package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pkg/errors"
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
	Checkpoint   Checkpoint      `toml:"checkpoint" json:"checkpoint"`
	Mydumper     MydumperRuntime `toml:"mydumper" json:"mydumper"`
	BWList       *filter.Rules   `toml:"black-white-list" json:"black-white-list"`
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
	ProfileCPU        bool `toml:"profile-cpu" json:"profile-cpu"`
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

type Checkpoint struct {
	Enable           bool   `toml:"enable" json:"enable"`
	Schema           string `toml:"schema" json:"schema"`
	DSN              string `toml:"dsn" json:"-"` // DSN may contain password, don't expose this to JSON.
	KeepAfterSuccess bool   `toml:"keep-after-success" json:"keep-after-success"`
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

	if len(cfg.Checkpoint.Schema) == 0 {
		cfg.Checkpoint.Schema = "tidb_lightning_checkpoint"
	}
	if len(cfg.Checkpoint.DSN) == 0 {
		cfg.Checkpoint.DSN = common.ToDSN(cfg.TiDB.Host, cfg.TiDB.Port, cfg.TiDB.User, cfg.TiDB.Psw)
	}

	return nil
}
