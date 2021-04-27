// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/docker/go-units"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	tidbcfg "github.com/pingcap/tidb/config"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/log"
)

const (
	// ImportMode defines mode of import for tikv.
	ImportMode = "import"
	// NormalMode defines mode of normal for tikv.
	NormalMode = "normal"

	// BackendTiDB is a constant for choosing the "TiDB" backend in the configuration.
	BackendTiDB = "tidb"
	// BackendImporter is a constant for choosing the "Importer" backend in the configuration.
	BackendImporter = "importer"
	// BackendLocal is a constant for choosing the "Local" backup in the configuration.
	// In this mode, we write & sort kv pairs with local storage and directly write them to tikv.
	BackendLocal = "local"

	// CheckpointDriverMySQL is a constant for choosing the "MySQL" checkpoint driver in the configuration.
	CheckpointDriverMySQL = "mysql"
	// CheckpointDriverFile is a constant for choosing the "File" checkpoint driver in the configuration.
	CheckpointDriverFile = "file"

	// ReplaceOnDup indicates using REPLACE INTO to insert data
	ReplaceOnDup = "replace"
	// IgnoreOnDup indicates using INSERT IGNORE INTO to insert data
	IgnoreOnDup = "ignore"
	// ErrorOnDup indicates using INSERT INTO to insert data, which would violate PK or UNIQUE constraint
	ErrorOnDup = "error"

	defaultDistSQLScanConcurrency     = 15
	defaultBuildStatsConcurrency      = 20
	defaultIndexSerialScanConcurrency = 20
	defaultChecksumTableConcurrency   = 2
)

const (
	LocalMemoryTableSize = 512 * units.MiB

	// autoDiskQuotaLocalReservedSize is the estimated size a local-backend
	// engine may gain after calling Flush(). This is currently defined by its
	// max MemTable size (512 MiB). It is used to compensate for the soft limit
	// of the disk quota against the hard limit of the disk free space.
	//
	// With a maximum of 8 engines, this should contribute 4.0 GiB to the
	// reserved size.
	autoDiskQuotaLocalReservedSize uint64 = LocalMemoryTableSize

	// autoDiskQuotaLocalReservedSpeed is the estimated size increase per
	// millisecond per write thread the local backend may gain on all engines.
	// This is used to compute the maximum size overshoot between two disk quota
	// checks, if the first one has barely passed.
	//
	// With cron.check-disk-quota = 1m, region-concurrency = 40, this should
	// contribute 2.3 GiB to the reserved size.
	autoDiskQuotaLocalReservedSpeed uint64 = 1 * units.KiB
)

var (
	defaultConfigPaths    = []string{"tidb-lightning.toml", "conf/tidb-lightning.toml"}
	supportedStorageTypes = []string{"file", "local", "s3", "noop"}

	DefaultFilter = []string{
		"*.*",
		"!mysql.*",
		"!sys.*",
		"!INFORMATION_SCHEMA.*",
		"!PERFORMANCE_SCHEMA.*",
		"!METRICS_SCHEMA.*",
		"!INSPECTION_SCHEMA.*",
	}
)

type DBStore struct {
	Host       string    `toml:"host" json:"host"`
	Port       int       `toml:"port" json:"port"`
	User       string    `toml:"user" json:"user"`
	Psw        string    `toml:"password" json:"-"`
	StatusPort int       `toml:"status-port" json:"status-port"`
	PdAddr     string    `toml:"pd-addr" json:"pd-addr"`
	StrSQLMode string    `toml:"sql-mode" json:"sql-mode"`
	TLS        string    `toml:"tls" json:"tls"`
	Security   *Security `toml:"security" json:"security"`

	SQLMode          mysql.SQLMode `toml:"-" json:"-"`
	MaxAllowedPacket uint64        `toml:"max-allowed-packet" json:"max-allowed-packet"`

	DistSQLScanConcurrency     int `toml:"distsql-scan-concurrency" json:"distsql-scan-concurrency"`
	BuildStatsConcurrency      int `toml:"build-stats-concurrency" json:"build-stats-concurrency"`
	IndexSerialScanConcurrency int `toml:"index-serial-scan-concurrency" json:"index-serial-scan-concurrency"`
	ChecksumTableConcurrency   int `toml:"checksum-table-concurrency" json:"checksum-table-concurrency"`
}

type Config struct {
	TaskID int64 `toml:"-" json:"id"`

	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	Checkpoint   Checkpoint          `toml:"checkpoint" json:"checkpoint"`
	Mydumper     MydumperRuntime     `toml:"mydumper" json:"mydumper"`
	TikvImporter TikvImporter        `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore         `toml:"post-restore" json:"post-restore"`
	Cron         Cron                `toml:"cron" json:"cron"`
	Routes       []*router.TableRule `toml:"routes" json:"routes"`
	Security     Security            `toml:"security" json:"security"`

	BWList filter.MySQLReplicationRules `toml:"black-white-list" json:"black-white-list"`
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal config to json error", log.ShortError(err))
	}
	return string(bytes)
}

func (c *Config) ToTLS() (*common.TLS, error) {
	hostPort := net.JoinHostPort(c.TiDB.Host, strconv.Itoa(c.TiDB.StatusPort))
	return common.NewTLS(c.Security.CAPath, c.Security.CertPath, c.Security.KeyPath, hostPort)
}

type Lightning struct {
	TableConcurrency  int  `toml:"table-concurrency" json:"table-concurrency"`
	IndexConcurrency  int  `toml:"index-concurrency" json:"index-concurrency"`
	RegionConcurrency int  `toml:"region-concurrency" json:"region-concurrency"`
	IOConcurrency     int  `toml:"io-concurrency" json:"io-concurrency"`
	CheckRequirements bool `toml:"check-requirements" json:"check-requirements"`
}

type PostOpLevel int

const (
	OpLevelOff PostOpLevel = iota
	OpLevelOptional
	OpLevelRequired
)

func (t *PostOpLevel) UnmarshalTOML(v interface{}) error {
	switch val := v.(type) {
	case bool:
		if val {
			*t = OpLevelRequired
		} else {
			*t = OpLevelOff
		}
	case string:
		return t.FromStringValue(val)
	default:
		return errors.Errorf("invalid op level '%v', please choose valid option between ['off', 'optional', 'required']", v)
	}
	return nil
}

func (t PostOpLevel) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parses command line parameters.
func (t *PostOpLevel) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	case "off", "false":
		*t = OpLevelOff
	case "required", "true":
		*t = OpLevelRequired
	case "optional":
		*t = OpLevelOptional
	default:
		return errors.Errorf("invalid op level '%s', please choose valid option between ['off', 'optional', 'required']", s)
	}
	return nil
}

func (t *PostOpLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

func (t *PostOpLevel) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

func (t PostOpLevel) String() string {
	switch t {
	case OpLevelOff:
		return "off"
	case OpLevelOptional:
		return "optional"
	case OpLevelRequired:
		return "required"
	default:
		panic(fmt.Sprintf("invalid post process type '%d'", t))
	}
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Level1Compact     bool        `toml:"level-1-compact" json:"level-1-compact"`
	Compact           bool        `toml:"compact" json:"compact"`
	Checksum          PostOpLevel `toml:"checksum" json:"checksum"`
	Analyze           PostOpLevel `toml:"analyze" json:"analyze"`
	PostProcessAtLast bool        `toml:"post-process-at-last" json:"post-process-at-last"`
}

type CSVConfig struct {
	Separator       string `toml:"separator" json:"separator"`
	Delimiter       string `toml:"delimiter" json:"delimiter"`
	Header          bool   `toml:"header" json:"header"`
	TrimLastSep     bool   `toml:"trim-last-separator" json:"trim-last-separator"`
	NotNull         bool   `toml:"not-null" json:"not-null"`
	Null            string `toml:"null" json:"null"`
	BackslashEscape bool   `toml:"backslash-escape" json:"backslash-escape"`
}

type MydumperRuntime struct {
	ReadBlockSize    ByteSize         `toml:"read-block-size" json:"read-block-size"`
	BatchSize        ByteSize         `toml:"batch-size" json:"batch-size"`
	BatchImportRatio float64          `toml:"batch-import-ratio" json:"batch-import-ratio"`
	SourceDir        string           `toml:"data-source-dir" json:"data-source-dir"`
	NoSchema         bool             `toml:"no-schema" json:"no-schema"`
	CharacterSet     string           `toml:"character-set" json:"character-set"`
	CSV              CSVConfig        `toml:"csv" json:"csv"`
	CaseSensitive    bool             `toml:"case-sensitive" json:"case-sensitive"`
	StrictFormat     bool             `toml:"strict-format" json:"strict-format"`
	MaxRegionSize    ByteSize         `toml:"max-region-size" json:"max-region-size"`
	Filter           []string         `toml:"filter" json:"filter"`
	FileRouters      []*FileRouteRule `toml:"files" json:"files"`
	DefaultFileRules bool             `toml:"default-file-rules" json:"default-file-rules"`
}

type FileRouteRule struct {
	Pattern     string `json:"pattern" toml:"pattern" yaml:"pattern"`
	Path        string `json:"path" toml:"path" yaml:"path"`
	Schema      string `json:"schema" toml:"schema" yaml:"schema"`
	Table       string `json:"table" toml:"table" yaml:"table"`
	Type        string `json:"type" toml:"type" yaml:"type"`
	Key         string `json:"key" toml:"key" yaml:"key"`
	Compression string `json:"compression" toml:"compression" yaml:"compression"`
}

type TikvImporter struct {
	Addr             string   `toml:"addr" json:"addr"`
	Backend          string   `toml:"backend" json:"backend"`
	OnDuplicate      string   `toml:"on-duplicate" json:"on-duplicate"`
	MaxKVPairs       int      `toml:"max-kv-pairs" json:"max-kv-pairs"`
	SendKVPairs      int      `toml:"send-kv-pairs" json:"send-kv-pairs"`
	RegionSplitSize  ByteSize `toml:"region-split-size" json:"region-split-size"`
	SortedKVDir      string   `toml:"sorted-kv-dir" json:"sorted-kv-dir"`
	DiskQuota        ByteSize `toml:"disk-quota" json:"disk-quota"`
	RangeConcurrency int      `toml:"range-concurrency" json:"range-concurrency"`
}

type Checkpoint struct {
	Enable           bool   `toml:"enable" json:"enable"`
	Schema           string `toml:"schema" json:"schema"`
	DSN              string `toml:"dsn" json:"-"` // DSN may contain password, don't expose this to JSON.
	Driver           string `toml:"driver" json:"driver"`
	KeepAfterSuccess bool   `toml:"keep-after-success" json:"keep-after-success"`
}

type Cron struct {
	SwitchMode     Duration `toml:"switch-mode" json:"switch-mode"`
	LogProgress    Duration `toml:"log-progress" json:"log-progress"`
	CheckDiskQuota Duration `toml:"check-disk-quota" json:"check-disk-quota"`
}

type Security struct {
	CAPath   string `toml:"ca-path" json:"ca-path"`
	CertPath string `toml:"cert-path" json:"cert-path"`
	KeyPath  string `toml:"key-path" json:"key-path"`
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool `toml:"redact-info-log" json:"redact-info-log"`
}

// RegisterMySQL registers (or deregisters) the TLS config with name "cluster"
// for use in `sql.Open()`. This method is goroutine-safe.
func (sec *Security) RegisterMySQL() error {
	if sec == nil {
		return nil
	}
	tlsConfig, err := common.ToTLSConfig(sec.CAPath, sec.CertPath, sec.KeyPath)
	switch {
	case err != nil:
		return err
	case tlsConfig != nil:
		// error happens only when the key coincides with the built-in names.
		_ = gomysql.RegisterTLSConfig("cluster", tlsConfig)
	default:
		gomysql.DeregisterTLSConfig("cluster")
	}
	return nil
}

// Duration which can be deserialized from a TOML string.
// Implemented as https://github.com/BurntSushi/toml#using-the-encodingtextunmarshaler-interface
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.Duration)), nil
}

func NewConfig() *Config {
	return &Config{
		App: Lightning{
			RegionConcurrency: runtime.NumCPU(),
			TableConcurrency:  0,
			IndexConcurrency:  0,
			IOConcurrency:     5,
			CheckRequirements: true,
		},
		Checkpoint: Checkpoint{
			Enable: true,
		},
		TiDB: DBStore{
			Host:                       "127.0.0.1",
			User:                       "root",
			StatusPort:                 10080,
			StrSQLMode:                 "ONLY_FULL_GROUP_BY,NO_AUTO_CREATE_USER",
			MaxAllowedPacket:           defaultMaxAllowedPacket,
			BuildStatsConcurrency:      defaultBuildStatsConcurrency,
			DistSQLScanConcurrency:     defaultDistSQLScanConcurrency,
			IndexSerialScanConcurrency: defaultIndexSerialScanConcurrency,
			ChecksumTableConcurrency:   defaultChecksumTableConcurrency,
		},
		Cron: Cron{
			SwitchMode:     Duration{Duration: 5 * time.Minute},
			LogProgress:    Duration{Duration: 5 * time.Minute},
			CheckDiskQuota: Duration{Duration: 1 * time.Minute},
		},
		Mydumper: MydumperRuntime{
			ReadBlockSize: ReadBlockSize,
			CSV: CSVConfig{
				Separator:       ",",
				Delimiter:       `"`,
				Header:          true,
				NotNull:         false,
				Null:            `\N`,
				BackslashEscape: true,
				TrimLastSep:     false,
			},
			StrictFormat:  false,
			MaxRegionSize: MaxRegionSize,
			Filter:        DefaultFilter,
		},
		TikvImporter: TikvImporter{
			Backend:         BackendImporter,
			OnDuplicate:     ReplaceOnDup,
			MaxKVPairs:      4096,
			SendKVPairs:     32768,
			RegionSplitSize: SplitRegionSize,
		},
		PostRestore: PostRestore{
			Checksum:          OpLevelRequired,
			Analyze:           OpLevelOptional,
			PostProcessAtLast: true,
		},
	}
}

// LoadFromGlobal resets the current configuration to the global settings.
func (c *Config) LoadFromGlobal(global *GlobalConfig) error {
	if err := c.LoadFromTOML(global.ConfigFileContent); err != nil {
		return err
	}

	c.TiDB.Host = global.TiDB.Host
	c.TiDB.Port = global.TiDB.Port
	c.TiDB.User = global.TiDB.User
	c.TiDB.Psw = global.TiDB.Psw
	c.TiDB.StatusPort = global.TiDB.StatusPort
	c.TiDB.PdAddr = global.TiDB.PdAddr
	c.Mydumper.SourceDir = global.Mydumper.SourceDir
	c.Mydumper.NoSchema = global.Mydumper.NoSchema
	c.Mydumper.Filter = global.Mydumper.Filter
	c.TikvImporter.Addr = global.TikvImporter.Addr
	c.TikvImporter.Backend = global.TikvImporter.Backend
	c.TikvImporter.SortedKVDir = global.TikvImporter.SortedKVDir
	c.Checkpoint.Enable = global.Checkpoint.Enable
	c.PostRestore.Checksum = global.PostRestore.Checksum
	c.PostRestore.Analyze = global.PostRestore.Analyze
	c.App.CheckRequirements = global.App.CheckRequirements
	c.Security = global.Security

	return nil
}

// LoadFromTOML overwrites the current configuration by the TOML data
// If data contains toml items not in Config and GlobalConfig, return an error
// If data contains toml items not in Config, thus won't take effect, warn user
func (c *Config) LoadFromTOML(data []byte) error {
	// bothUnused saves toml items not belong to Config nor GlobalConfig
	var bothUnused []string
	// warnItems saves legal toml items but won't effect
	var warnItems []string

	dataStr := string(data)

	// Here we load toml into cfg, and rest logic is check unused keys
	metaData, err := toml.Decode(dataStr, c)

	if err != nil {
		return errors.Trace(err)
	}

	unusedConfigKeys := metaData.Undecoded()
	if len(unusedConfigKeys) == 0 {
		return nil
	}

	// Now we deal with potential both-unused keys of Config and GlobalConfig struct

	metaDataGlobal, err := toml.Decode(dataStr, &GlobalConfig{})
	if err != nil {
		return errors.Trace(err)
	}

	// Key type returned by metadata.Undecoded doesn't have a equality comparison,
	// we convert them to string type instead, and this conversion is identical
	unusedGlobalKeys := metaDataGlobal.Undecoded()
	unusedGlobalKeyStrs := make(map[string]struct{})
	for _, key := range unusedGlobalKeys {
		unusedGlobalKeyStrs[key.String()] = struct{}{}
	}

	for _, key := range unusedConfigKeys {
		keyStr := key.String()
		if _, found := unusedGlobalKeyStrs[keyStr]; found {
			bothUnused = append(bothUnused, keyStr)
		} else {
			warnItems = append(warnItems, keyStr)
		}
	}

	if len(bothUnused) > 0 {
		return errors.Errorf("config file contained unknown configuration options: %s",
			strings.Join(bothUnused, ", "))
	}

	// Warn that some legal field of config file won't be overwritten, such as lightning.file
	if len(warnItems) > 0 {
		log.L().Warn("currently only per-task configuration can be applied, global configuration changes can only be made on startup",
			zap.Strings("global config changes", warnItems))
	}

	return nil
}

// Adjust fixes the invalid or unspecified settings to reasonable valid values.
func (c *Config) Adjust(ctx context.Context) error {
	// Reject problematic CSV configurations.
	csv := &c.Mydumper.CSV
	if len(csv.Separator) == 0 {
		return errors.New("invalid config: `mydumper.csv.separator` must not be empty")
	}

	if len(csv.Delimiter) > 0 && (strings.HasPrefix(csv.Separator, csv.Delimiter) || strings.HasPrefix(csv.Delimiter, csv.Separator)) {
		return errors.New("invalid config: `mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other")
	}

	if csv.BackslashEscape {
		if csv.Separator == `\` {
			return errors.New("invalid config: cannot use '\\' as CSV separator when `mydumper.csv.backslash-escape` is true")
		}
		if csv.Delimiter == `\` {
			return errors.New("invalid config: cannot use '\\' as CSV delimiter when `mydumper.csv.backslash-escape` is true")
		}
	}

	// adjust file routing
	for _, rule := range c.Mydumper.FileRouters {
		if filepath.IsAbs(rule.Path) {
			relPath, err := filepath.Rel(c.Mydumper.SourceDir, rule.Path)
			if err != nil {
				return errors.Trace(err)
			}
			// ".." means that this path is not in source dir, so we should return an error
			if strings.HasPrefix(relPath, "..") {
				return errors.Errorf("file route path '%s' is not in source dir '%s'", rule.Path, c.Mydumper.SourceDir)
			}
			rule.Path = relPath
		}
	}

	// enable default file route rule if no rules are set
	if len(c.Mydumper.FileRouters) == 0 {
		c.Mydumper.DefaultFileRules = true
	}

	c.TikvImporter.Backend = strings.ToLower(c.TikvImporter.Backend)
	mustHaveInternalConnections := true
	switch c.TikvImporter.Backend {
	case BackendTiDB:
		if c.App.IndexConcurrency == 0 {
			c.App.IndexConcurrency = c.App.RegionConcurrency
		}
		if c.App.TableConcurrency == 0 {
			c.App.TableConcurrency = c.App.RegionConcurrency
		}
		mustHaveInternalConnections = false
	case BackendImporter, BackendLocal:
		if c.App.IndexConcurrency == 0 {
			c.App.IndexConcurrency = 2
		}
		if c.App.TableConcurrency == 0 {
			c.App.TableConcurrency = 6
		}
		if c.TikvImporter.RangeConcurrency == 0 {
			c.TikvImporter.RangeConcurrency = 16
		}
		if c.TikvImporter.RegionSplitSize == 0 {
			c.TikvImporter.RegionSplitSize = SplitRegionSize
		}
		if c.TiDB.DistSQLScanConcurrency == 0 {
			c.TiDB.DistSQLScanConcurrency = defaultDistSQLScanConcurrency
		}
		if c.TiDB.BuildStatsConcurrency == 0 {
			c.TiDB.BuildStatsConcurrency = defaultBuildStatsConcurrency
		}
		if c.TiDB.IndexSerialScanConcurrency == 0 {
			c.TiDB.IndexSerialScanConcurrency = defaultIndexSerialScanConcurrency
		}
		if c.TiDB.ChecksumTableConcurrency == 0 {
			c.TiDB.ChecksumTableConcurrency = defaultChecksumTableConcurrency
		}
	default:
		return errors.Errorf("invalid config: unsupported `tikv-importer.backend` (%s)", c.TikvImporter.Backend)
	}

	if c.TikvImporter.Backend == BackendLocal {
		if len(c.TikvImporter.SortedKVDir) == 0 {
			return errors.Errorf("tikv-importer.sorted-kv-dir must not be empty!")
		}

		storageSizeDir := filepath.Clean(c.TikvImporter.SortedKVDir)
		sortedKVDirInfo, err := os.Stat(storageSizeDir)
		switch {
		case os.IsNotExist(err):
			// the sorted-kv-dir does not exist, meaning we will create it automatically.
			// so we extract the storage size from its parent directory.
			storageSizeDir = filepath.Dir(storageSizeDir)
		case err == nil:
			if !sortedKVDirInfo.IsDir() {
				return errors.Errorf("tikv-importer.sorted-kv-dir ('%s') is not a directory", storageSizeDir)
			}
		default:
			return errors.Annotate(err, "invalid tikv-importer.sorted-kv-dir")
		}

		if c.TikvImporter.DiskQuota == 0 {
			enginesCount := uint64(c.App.IndexConcurrency + c.App.TableConcurrency)
			writeAmount := uint64(c.App.RegionConcurrency) * uint64(c.Cron.CheckDiskQuota.Milliseconds())
			reservedSize := enginesCount*autoDiskQuotaLocalReservedSize + writeAmount*autoDiskQuotaLocalReservedSpeed

			storageSize, err := common.GetStorageSize(storageSizeDir)
			if err != nil {
				return err
			}
			if storageSize.Available <= reservedSize {
				return errors.Errorf(
					"insufficient disk free space on `%s` (only %s, expecting >%s), please use a storage with enough free space, or specify `tikv-importer.disk-quota`",
					c.TikvImporter.SortedKVDir,
					units.BytesSize(float64(storageSize.Available)),
					units.BytesSize(float64(reservedSize)))
			}
			c.TikvImporter.DiskQuota = ByteSize(storageSize.Available - reservedSize)
		}
	}

	if c.TikvImporter.Backend == BackendTiDB {
		c.TikvImporter.OnDuplicate = strings.ToLower(c.TikvImporter.OnDuplicate)
		switch c.TikvImporter.OnDuplicate {
		case ReplaceOnDup, IgnoreOnDup, ErrorOnDup:
		default:
			return errors.Errorf("invalid config: unsupported `tikv-importer.on-duplicate` (%s)", c.TikvImporter.OnDuplicate)
		}
	}

	var err error
	c.TiDB.SQLMode, err = mysql.GetSQLMode(c.TiDB.StrSQLMode)
	if err != nil {
		return errors.Annotate(err, "invalid config: `mydumper.tidb.sql_mode` must be a valid SQL_MODE")
	}

	if c.TiDB.Security == nil {
		c.TiDB.Security = &c.Security
	}

	switch c.TiDB.TLS {
	case "":
		if len(c.TiDB.Security.CAPath) > 0 {
			c.TiDB.TLS = "cluster"
		} else {
			c.TiDB.TLS = "false"
		}
	case "cluster":
		if len(c.Security.CAPath) == 0 {
			return errors.New("invalid config: cannot set `tidb.tls` to 'cluster' without a [security] section")
		}
	case "false", "skip-verify", "preferred":
		break
	default:
		return errors.Errorf("invalid config: unsupported `tidb.tls` config %s", c.TiDB.TLS)
	}

	// mydumper.filter and black-white-list cannot co-exist.
	if c.HasLegacyBlackWhiteList() {
		log.L().Warn("the config `black-white-list` has been deprecated, please replace with `mydumper.filter`")
		if !common.StringSliceEqual(c.Mydumper.Filter, DefaultFilter) {
			return errors.New("invalid config: `mydumper.filter` and `black-white-list` cannot be simultaneously defined")
		}
	}

	for _, rule := range c.Routes {
		if !c.Mydumper.CaseSensitive {
			rule.ToLower()
		}
		if err := rule.Valid(); err != nil {
			return errors.Trace(err)
		}
	}

	// automatically determine the TiDB port & PD address from TiDB settings
	if mustHaveInternalConnections && (c.TiDB.Port <= 0 || len(c.TiDB.PdAddr) == 0) {
		tls, err := c.ToTLS()
		if err != nil {
			return err
		}

		var settings tidbcfg.Config
		err = tls.GetJSON(ctx, "/settings", &settings)
		if err != nil {
			return errors.Annotate(err, "cannot fetch settings from TiDB, please manually fill in `tidb.port` and `tidb.pd-addr`")
		}
		if c.TiDB.Port <= 0 {
			c.TiDB.Port = int(settings.Port)
		}
		if len(c.TiDB.PdAddr) == 0 {
			pdAddrs := strings.Split(settings.Path, ",")
			c.TiDB.PdAddr = pdAddrs[0] // FIXME support multiple PDs once importer can.
		}
	}

	if c.TiDB.Port <= 0 {
		return errors.New("invalid `tidb.port` setting")
	}
	if mustHaveInternalConnections && len(c.TiDB.PdAddr) == 0 {
		return errors.New("invalid `tidb.pd-addr` setting")
	}

	// handle mydumper
	if c.Mydumper.BatchSize <= 0 {
		// if rows in source files are not sorted by primary key(if primary is number or cluster index enabled),
		// the key range in each data engine may have overlap, thus a bigger engine size can somewhat alleviate it.
		c.Mydumper.BatchSize = defaultBatchSize

	}
	if c.Mydumper.BatchImportRatio < 0.0 || c.Mydumper.BatchImportRatio >= 1.0 {
		c.Mydumper.BatchImportRatio = 0.75
	}
	if c.Mydumper.ReadBlockSize <= 0 {
		c.Mydumper.ReadBlockSize = ReadBlockSize
	}
	if len(c.Mydumper.CharacterSet) == 0 {
		c.Mydumper.CharacterSet = "auto"
	}

	if len(c.Checkpoint.Schema) == 0 {
		c.Checkpoint.Schema = "tidb_lightning_checkpoint"
	}
	if len(c.Checkpoint.Driver) == 0 {
		c.Checkpoint.Driver = CheckpointDriverFile
	}
	if len(c.Checkpoint.DSN) == 0 {
		switch c.Checkpoint.Driver {
		case CheckpointDriverMySQL:
			param := common.MySQLConnectParam{
				Host:             c.TiDB.Host,
				Port:             c.TiDB.Port,
				User:             c.TiDB.User,
				Password:         c.TiDB.Psw,
				SQLMode:          mysql.DefaultSQLMode,
				MaxAllowedPacket: defaultMaxAllowedPacket,
				TLS:              c.TiDB.TLS,
			}
			c.Checkpoint.DSN = param.ToDSN()
		case CheckpointDriverFile:
			c.Checkpoint.DSN = "/tmp/" + c.Checkpoint.Schema + ".pb"
		}
	}

	var u *url.URL

	// An absolute Windows path like "C:\Users\XYZ" would be interpreted as
	// an URL with scheme "C" and opaque data "\Users\XYZ".
	// Therefore, we only perform URL parsing if we are sure the path is not
	// an absolute Windows path.
	// Here we use the `filepath.VolumeName` which can identify the "C:" part
	// out of the path. On Linux this method always return an empty string.
	// On Windows, the drive letter can only be single letters from "A:" to "Z:",
	// so this won't mistake "S3:" as a Windows path.
	if len(filepath.VolumeName(c.Mydumper.SourceDir)) == 0 {
		u, err = url.Parse(c.Mydumper.SourceDir)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		u = &url.URL{}
	}

	// convert path and relative path to a valid file url
	if u.Scheme == "" {
		if !common.IsDirExists(c.Mydumper.SourceDir) {
			return errors.Errorf("%s: mydumper dir does not exist", c.Mydumper.SourceDir)
		}
		absPath, err := filepath.Abs(c.Mydumper.SourceDir)
		if err != nil {
			return errors.Annotatef(err, "covert data-source-dir '%s' to absolute path failed", c.Mydumper.SourceDir)
		}
		c.Mydumper.SourceDir = "file://" + filepath.ToSlash(absPath)
		u.Path = absPath
		u.Scheme = "file"
	}

	found := false
	for _, t := range supportedStorageTypes {
		if u.Scheme == t {
			found = true
			break
		}
	}
	if !found {
		return errors.Errorf("Unsupported data-source-dir url '%s'", c.Mydumper.SourceDir)
	}

	return nil
}

// HasLegacyBlackWhiteList checks whether the deprecated [black-white-list] section
// was defined.
func (c *Config) HasLegacyBlackWhiteList() bool {
	return len(c.BWList.DoTables) != 0 || len(c.BWList.DoDBs) != 0 || len(c.BWList.IgnoreTables) != 0 || len(c.BWList.IgnoreDBs) != 0
}
