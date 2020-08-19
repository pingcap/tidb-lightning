package mydump

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/pd/v4/pkg/slice"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type SourceType int

const (
	SourceTypeIgnore SourceType = iota
	SourceTypeSchemaSchema
	SourceTypeTableSchema
	SourceTypeSQL
	SourceTypeCSV
	SourceTypeParquet
)

const (
	SchemaSchema = "schema-schema"
	TableSchema  = "table-schema"
	TypeSQL      = "sql"
	TypeCSV      = "csv"
	TypeParquet  = "parquet"
	TypeIgnore   = "ignore"
)

type Compression int

const (
	CompressionNone Compression = iota
	CompressionGZ
	CompressionLZ4
	CompressionZStd
	CompressionXZ
)

func parseSourceType(t string) (SourceType, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case SchemaSchema:
		return SourceTypeSchemaSchema, nil
	case TableSchema:
		return SourceTypeTableSchema, nil
	case TypeSQL:
		return SourceTypeSQL, nil
	case TypeCSV:
		return SourceTypeCSV, nil
	case TypeParquet:
		return SourceTypeParquet, nil
	case TypeIgnore:
		return SourceTypeIgnore, nil
	default:
		return SourceTypeIgnore, errors.Errorf("unknown source type '%s'", t)
	}
}

func (s SourceType) String() string {
	switch s {
	case SourceTypeSchemaSchema:
		return SchemaSchema
	case SourceTypeTableSchema:
		return TableSchema
	case SourceTypeCSV:
		return TypeCSV
	case SourceTypeSQL:
		return TypeSQL
	case SourceTypeParquet:
		return TypeParquet
	default:
		return TypeIgnore
	}
}

func parseCompressionType(t string) (Compression, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "gz":
		return CompressionGZ, nil
	case "lz4":
		return CompressionLZ4, nil
	case "zstd":
		return CompressionZStd, nil
	case "xz":
		return CompressionXZ, nil
	case "":
		return CompressionNone, nil
	default:
		return CompressionNone, errors.Errorf("invalid compression type '%s'", t)
	}
}

var (
	expandVariablePattern = regexp.MustCompile(`\$(?:\$|[\pL\p{Nd}_]+|\{[\pL\p{Nd}_]+\})`)
)

var (
	defaultFileRouteRules = []*config.FileRouteRule{
		// ignore *-schema-view.sql,-schema-trigger.sql,-schema-post.sql files
		{Pattern: `(?i).*(-schema-view|-schema-trigger|-schema-post)\.sql`, Type: "ignore"},
		// db schema create file pattern, matches files like '{schema}-schema-create.sql'
		{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)-schema-create\.sql`, Schema: "$1", Table: "", Type: SchemaSchema},
		// table schema create file pattern, matches files like '{schema}.{table}-schema.sql'
		{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)-schema\.sql`, Schema: "$1", Table: "$2", Type: TableSchema},
		// source file pattern, matches files like '{schema}.{table}.0001.{sql|csv}'
		{Pattern: `(?i)^(?:[^/]*/)*([^/.]+)\.(.*?)(?:\.([0-9]+))?\.(sql|csv)$`, Schema: "$1", Table: "$2", Type: "$4", Key: "$3"},
	}
)

// // RouteRule is a rule to route file path to target schema/table
type FileRouter interface {
	// Route apply rule to path. Return nil if path doesn't math route rule
	Route(path string) *RouteResult
}

// chainRouters aggregates multi `FileRouter` as a router
type chainRouters []FileRouter

func (c chainRouters) Route(path string) *RouteResult {
	for _, r := range c {
		res := r.Route(path)
		if res != nil {
			return res
		}
	}
	return nil
}

func NewFileRouter(cfg []*config.FileRouteRule) (FileRouter, error) {
	res := make([]FileRouter, 0, len(cfg))
	p := regexRouterParser{}
	for _, c := range cfg {
		rule, err := p.Parse(c)
		if err != nil {
			return nil, err
		}
		res = append(res, rule)
	}
	return chainRouters(res), nil
}

// `RegexRouter` is a `FileRouter` implement that apply specific regex pattern to filepath.
// if regex pattern match, then each extractors with capture the matched regexp pattern and
// set value to target field in `RouteResult`
type RegexRouter struct {
	pattern    *regexp.Regexp
	extractors []patExpander
}

func (r *RegexRouter) Route(path string) *RouteResult {
	// the regexp pattern maybe not contains any sub matches
	if !r.pattern.MatchString(path) {
		return nil
	}

	indexes := r.pattern.FindStringSubmatchIndex(path)
	result := &RouteResult{}
	for _, e := range r.extractors {
		if !e.Expand(r.pattern, path, indexes, result) {
			return nil
		}
	}
	return result
}

type regexRouterParser struct{}

func (p regexRouterParser) Parse(r *config.FileRouteRule) (*RegexRouter, error) {
	rule := &RegexRouter{}
	if r.Path != "" && r.Pattern != "" {
		return nil, errors.New("can't set both `path` and `pattern` field")
	}
	if r.Path != "" {
		// convert constant string as a regexp pattern
		r.Pattern = regexp.QuoteMeta(r.Path)
		// escape all '$' by '$$' in match templates
		quoteTmplFn := func(t string) string { return strings.ReplaceAll(t, "$", "$$") }
		r.Table = quoteTmplFn(r.Table)
		r.Schema = quoteTmplFn(r.Schema)
		r.Type = quoteTmplFn(r.Type)
		r.Compression = quoteTmplFn(r.Compression)
		r.Key = quoteTmplFn(r.Key)

	}
	pattern, err := regexp.Compile(r.Pattern)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rule.pattern = pattern

	err = p.parseFieldExtractor(rule, "type", r.Type, func(s string) error {
		_, e := parseSourceType(s)
		return e
	}, func(result *RouteResult, value string) bool {
		ty, err := parseSourceType(value)
		if err != nil {
			return false
		}
		result.Type = ty
		return true
	})
	if err != nil {
		return nil, err
	}
	// ignore pattern needn't parse other fields
	if r.Type == TypeIgnore {
		return rule, nil
	}

	err = p.parseFieldExtractor(rule, "schema", r.Schema, alwaysValid, func(result *RouteResult, value string) bool {
		result.Schema = value
		return true
	})
	if err != nil {
		return nil, err
	}

	// special case: when the pattern is for db schema, should not parse table name
	if r.Type != SchemaSchema {
		err = p.parseFieldExtractor(rule, "table", r.Table, alwaysValid, func(result *RouteResult, value string) bool {
			result.Name = value
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	if len(r.Key) > 0 {
		err = p.parseFieldExtractor(rule, "key", r.Key, alwaysValid, func(result *RouteResult, value string) bool {
			result.Key = value
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	if len(r.Compression) > 0 {
		err = p.parseFieldExtractor(rule, "compression", r.Compression, func(s string) error {
			_, e := parseCompressionType(s)
			return e
		}, func(result *RouteResult, value string) bool {
			// TODO: should support restore compressed source files
			compression, err := parseCompressionType(value)
			if err != nil {
				return false
			}
			if compression != CompressionNone {
				log.L().Warn("Currently we don't support restore compressed source file yet, source file will be ignored")
				return false
			}
			result.Compression = compression
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	return rule, nil
}

func alwaysValid(_ string) error {
	return nil
}

// parse each field extractor in `p.r` and set them to p.rule
func (p regexRouterParser) parseFieldExtractor(
	rule *RegexRouter,
	field,
	fieldPattern string,
	validateFn func(string) error,
	applyFn func(result *RouteResult, value string) bool,
) error {
	// pattern is empty, return default rule
	if len(fieldPattern) == 0 {
		return errors.Errorf("field '%s' match pattern can't be empty", field)
	}

	// check and parse regexp template
	if err := p.checkSubPatterns(rule.pattern, fieldPattern, validateFn); err != nil {
		return errors.Trace(err)
	}
	rule.extractors = append(rule.extractors, patExpander{
		template: fieldPattern,
		applyFn:  applyFn,
	})
	return nil
}

func (p regexRouterParser) checkSubPatterns(pat *regexp.Regexp, t string, validateFn func(string) error) error {
	subPats := expandVariablePattern.FindAllString(t, -1)
	if len(subPats) == 0 {
		// validate const patterns
		if err := validateFn(t); err != nil {
			return err
		}
	}
	for _, subVar := range subPats {
		var tmplName string
		switch {
		case subVar == "$$":
			continue
		case strings.HasPrefix(subVar, "${"):
			tmplName = subVar[2 : len(subVar)-1]
		default:
			tmplName = subVar[1:]
		}
		if number, err := strconv.Atoi(tmplName); err == nil {
			if number > pat.NumSubexp() {
				return errors.Errorf("sub pattern capture '%s' out of range", subVar)
			}
		} else if !slice.AnyOf(pat.SubexpNames(), func(i int) bool {
			// FIXME: we should use re.SubexpIndex here, but not supported in go1.13 yet
			return pat.SubexpNames()[i] == tmplName
		}) {
			return errors.Errorf("invalid named capture '%s'", subVar)
		}
	}

	return nil
}

// patExpander extract string by expanding template with the regexp pattern
type patExpander struct {
	template string
	applyFn  func(result *RouteResult, value string) bool
}

func (p *patExpander) Expand(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool {
	value := pattern.ExpandString([]byte{}, p.template, path, matchIndex)
	return p.applyFn(result, string(value))
}

type RouteResult struct {
	filter.Table
	Key         string
	Compression Compression
	Type        SourceType
}
