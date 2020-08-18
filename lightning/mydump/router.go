package mydump

import (
	"regexp"
	"strconv"
	"strings"

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
)

const (
	SchemaSchema = "schema-schema"
	TableSchema  = "table-schema"
	TypeSQL      = "sql"
	TypeCSV      = "csv"
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
	isAlphaNumOrUnderline = regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString
	isNumber              = regexp.MustCompile(`^[0-9]+$`).MatchString
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
	p := &regexRouterParser{}
	for _, c := range cfg {
		p.rule = &RegexRouter{}
		if err := p.Parse(c); err != nil {
			return nil, err
		}
		res = append(res, p.rule)
	}
	return chainRouters(res), nil
}

// `RegexRouter` is a `FileRouter` implement that apply specific regex pattern to filepath.
// if regex pattern match, then each extractors with capture the matched regexp pattern and
// set value to target field in `RouteResult`
type RegexRouter struct {
	pattern    *regexp.Regexp
	extractors []regexExpander
}

func (r *RegexRouter) Route(path string) *RouteResult {
	indexes := r.pattern.FindStringSubmatchIndex(path)
	if len(indexes) == 0 {
		return nil
	}
	result := &RouteResult{}
	for _, e := range r.extractors {
		if !e.Expand(r.pattern, path, indexes, result) {
			return nil
		}
	}
	return result
}

type regexRouterParser struct {
	rule *RegexRouter
	r    *config.FileRouteRule
}

func (p *regexRouterParser) Parse(r *config.FileRouteRule) error {
	p.r = r
	pattern, err := regexp.Compile(p.r.Pattern)
	if err != nil {
		return errors.Trace(err)
	}
	p.rule.pattern = pattern

	err = p.parseFieldExtractor("type", p.r.Type, func(s string) error {
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
		return err
	}
	// ignore pattern needn't parse other fields
	if p.r.Type == TypeIgnore {
		return nil
	}

	err = p.parseFieldExtractor("schema", p.r.Schema, alwaysValid, func(result *RouteResult, value string) bool {
		result.Schema = value
		return true
	})
	if err != nil {
		return err
	}

	// special case: when the pattern is for db schema, should not parse table name
	if p.r.Type != SchemaSchema {
		err = p.parseFieldExtractor("table", p.r.Table, alwaysValid, func(result *RouteResult, value string) bool {
			result.Name = value
			return true
		})
		if err != nil {
			return err
		}
	}

	if len(p.r.Key) > 0 {
		err = p.parseFieldExtractor("key", p.r.Key, alwaysValid, func(result *RouteResult, value string) bool {
			result.Key = value
			return true
		})
		if err != nil {
			return err
		}
	}

	if len(p.r.Compression) > 0 {
		err = p.parseFieldExtractor("compression", p.r.Compression, func(s string) error {
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
			return err
		}
	}

	return nil
}

func alwaysValid(_ string) error {
	return nil
}

// parse each field extractor in `p.r` and set them to p.rule
func (p *regexRouterParser) parseFieldExtractor(
	field,
	fieldPattern string,
	validateFn func(string) error,
	applyFn func(result *RouteResult, value string) bool,
) error {
	// pattern is empty, return default rule
	if len(fieldPattern) == 0 {
		return errors.Errorf("field '%s' match pattern can't be empty", field)
	}

	// template is const string, return directly
	if strings.IndexByte(fieldPattern, '$') < 0 {
		// check if the const pattern is valid
		if err := validateFn(fieldPattern); err != nil {
			return err
		}
		p.rule.extractors = append(p.rule.extractors, &constExpander{
			value:   fieldPattern,
			applyFn: applyFn,
		})
		return nil
	}

	// check and parse regexp template
	if err := p.checkSubPatterns(fieldPattern); err != nil {
		return errors.Trace(err)
	}
	p.rule.extractors = append(p.rule.extractors, &patExpander{
		template: fieldPattern,
		applyFn:  applyFn,
	})
	return nil
}

func (p *regexRouterParser) checkSubPatterns(t string) error {
	captures, err := p.extractSubPatterns(t)
	if err != nil {
		return errors.Trace(err)
	}

outer:
	for _, c := range captures {
		if isNumber(c) {
			idx, _ := strconv.Atoi(c)
			if idx > p.rule.pattern.NumSubexp() {
				return errors.Errorf("sub pattern capture index '%d' out of range", idx)
			}
		} else {
			for _, n := range p.rule.pattern.SubexpNames() {
				if c == n {
					continue outer
				}
			}
			return errors.Errorf("invalid named capture '%s', available names are: %s", c, p.rule.pattern.SubexpNames())
		}
	}

	return nil
}

func (p *regexRouterParser) extractSubPatterns(t string) ([]string, error) {
	var res []string
	remain := t
	for {
		idx := strings.IndexByte(remain, '$')
		if idx < 0 {
			return res, nil
		}
		// $$ is expanding to '$', so just continue
		if idx < len(remain)-1 && remain[idx+1] == '$' {
			remain = remain[idx+2:]
			continue
		}
		remain = remain[idx+1:]
		if len(remain) == 0 {
			return nil, errors.Errorf("invalid template '%s', cannot end with '$'", t)
		}

		// parse sub-pattern such as $(pattern) or ${pattern}
		if remain[0] == '{' {
			// parenthesis pattern
			end := strings.IndexByte(remain, '}')
			if end < 0 {
				return nil, errors.Errorf("invalid template '%s', named capture starts with '${' should end with '}'", t)
			}
			if end == 1 {
				return nil, errors.Errorf("invalid template '%s', named capture must not be empty", t)
			}
			if !isAlphaNumOrUnderline(remain[1:end]) {
				return nil, errors.Errorf("invalid template '%s', invalid named capture '%s'", t, remain[1:end-1])
			}

			res = append(res, remain[1:end])
			remain = remain[end+1:]
			continue
		}

		// find the end Index of
		endIdx := strings.IndexFunc(remain, func(r rune) bool {
			return r < 48 || (r > 57 && r < 65) || (r > 90 && r != 95 && r < 97) || r > 122
		})
		switch {
		case endIdx == 0:
			return nil, errors.Errorf("invalid template '%s', named capture can only contain alphabets or numbers", t)
		case endIdx < 0:
			res = append(res, remain)
			remain = remain[:0]
		default:
			res = append(res, remain[:endIdx])
			remain = remain[endIdx:]
		}
	}
}

// regexExpander expand one or more values from path base on pattern to result
// return true for success and false for failed
type regexExpander interface {
	// expand template with pattern and matchIndex, and set the result to `RouteResult`
	Expand(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool
}

// patExpander extract string by expanding template with the regexp pattern
type patExpander struct {
	template string
	applyFn  func(result *RouteResult, value string) bool
}

func (p *patExpander) Expand(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool {
	if len(matchIndex) == 0 {
		return false
	}
	value := pattern.ExpandString([]byte{}, p.template, path, matchIndex)
	return p.applyFn(result, string(value))
}

// constExpander is a extractor that always return a constant string
type constExpander struct {
	value   string
	applyFn func(result *RouteResult, value string) bool
}

func (p *constExpander) Expand(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool {
	return p.applyFn(result, p.value)
}

type RouteResult struct {
	filter.Table
	Key         string
	Compression Compression
	Type        SourceType
}
