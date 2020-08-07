package mydump

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/tidb-lightning/lightning/config"

	"github.com/pingcap/errors"

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
)

type Compression int

const (
	CompressionNone Compression = iota
	CompressionGZ
	CompressionLZ4
	CompressionZStd
	CompressionXZ
)

func parseSourceType(t string) SourceType {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case SchemaSchema:
		return SourceTypeSchemaSchema
	case TableSchema:
		return SourceTypeTableSchema
	case TypeSQL:
		return SourceTypeSQL
	case TypeCSV:
		return SourceTypeCSV
	default:
		return SourceTypeIgnore
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
		return "ignored"
	}
}

func parseCompressionType(t string) Compression {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "gz":
		return CompressionGZ
	case "lz4":
		return CompressionLZ4
	case "zstd":
		return CompressionZStd
	case "xz":
		return CompressionXZ
	default:
		return CompressionNone
	}
}

var (
	isAlphaNum = regexp.MustCompile(`^[A-Za-z0-9]+$`).MatchString
	isAlpha    = regexp.MustCompile(`^[0-9]+$`).MatchString
)

var (
	defaultFileRouteRules = []*config.FileRouteRule{
		// db schema create file pattern, matches files like '{schema}-schema-create.sql'
		{`(?i)^(?:[^/]*/)*([^/.]+)-schema-create\.sql`, "$1", "", SchemaSchema, "", ""},
		// table schema create file pattern, matches files like '{schema}.{table}-schema.sql'
		{`(?i)^(?:[^/]*/)*([^/.]+)\.([^/.]+)-schema\.sql`, "$1", "$2", TableSchema, "", ""},
		// source file pattern, matches files like '{schema}.{table}.0001.{sql|csv}'
		{`(?i)^(?:[^/]*/)*([^/.]+)\.([^/.]+)(?:\.([0-9]+))?\.(sql|csv)$`, "$1", "$2", "$4", "$3", ""},
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
	var result *RouteResult
	for _, r := range c {
		res := r.Route(path)
		if res != nil {
			// SourceTypeIgnore means not match, so try to find other matches.
			if res.Type != SourceTypeIgnore {
				return res
			}
			if result == nil {
				result = res
			}
		}
	}
	return result
}

func Parse(cfg []*config.FileRouteRule) (FileRouter, error) {
	res := make([]FileRouter, 0, len(cfg))
	p := &regexRuleParser{}
	for _, c := range cfg {
		p.rule = &RegexRouteRule{}
		if err := p.Parse(c); err != nil {
			return nil, err
		}
		res = append(res, p.rule)
	}
	return chainRouters(res), nil
}

type RegexRouteRule struct {
	pattern    *regexp.Regexp
	extractors []regexExtractor
}

func (r *RegexRouteRule) Route(path string) *RouteResult {
	if !r.pattern.MatchString(path) {
		return nil
	}

	indexes := r.pattern.FindStringSubmatchIndex(path)
	result := &RouteResult{}
	for _, e := range r.extractors {
		if !e.Extract(r.pattern, path, indexes, result) {
			return nil
		}
	}
	return result
}

type regexRuleParser struct {
	rule *RegexRouteRule
	r    *config.FileRouteRule
}

func (p *regexRuleParser) Parse(r *config.FileRouteRule) error {
	p.r = r
	pattern, err := regexp.Compile(p.r.Pattern)
	if err != nil {
		return errors.Trace(err)
	}
	p.rule.pattern = pattern

	err = p.parseFieldExtractor(p.r.Schema, func(result *RouteResult, value string) {
		result.Schema = value
	})
	if err != nil {
		return err
	}

	// special case: when the pattern is for db schema, should not parse table name
	if p.r.Type != SchemaSchema {
		err = p.parseFieldExtractor(p.r.Table, func(result *RouteResult, value string) {
			result.Name = value
		})
		if err != nil {
			return err
		}

	}
	err = p.parseFieldExtractor(p.r.Type, func(result *RouteResult, value string) {
		result.Type = parseSourceType(value)
	})
	if err != nil {
		return err
	}

	if len(p.r.Key) > 0 {
		err = p.parseFieldExtractor(p.r.Key, func(result *RouteResult, value string) {
			result.Key = value
		})
		if err != nil {
			return err
		}
	}

	if len(p.r.Compression) > 0 {
		err = p.parseFieldExtractor(p.r.Compression, func(result *RouteResult, value string) {
			result.Compression = parseCompressionType(value)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *regexRuleParser) parseFieldExtractor(
	fieldPattern string,
	applyFn func(result *RouteResult, value string),
) error {
	// pattern is empty, return default rule
	if len(fieldPattern) == 0 {
		return errors.New("match pattern can't be empty")
	}

	// template is const string, return directly
	if strings.IndexByte(fieldPattern, '$') < 0 {
		p.rule.extractors = append(p.rule.extractors, &constExtractor{
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

func (p *regexRuleParser) checkSubPatterns(t string) error {
	captures, err := p.extractSubPatterns(t)
	if err != nil {
		return errors.Trace(err)
	}

outer:
	for _, c := range captures {
		if isAlpha(c) {
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

func (p *regexRuleParser) extractSubPatterns(t string) ([]string, error) {
	var res []string
	remain := t
	for {
		idx := strings.IndexByte(remain, '$')
		if idx < 0 {
			return res, nil
		}
		remain = remain[idx+1:]
		if len(remain) == 0 {
			return nil, errors.Errorf("invalid template '%s', cannot end with '$'", t)
		}

		if remain[0] == '(' {
			// parenthesis pattern
			end := strings.IndexByte(remain, ')')
			if end < 0 {
				return nil, errors.Errorf("invalid template '%s', named capture starts with '$(' should end with ')'", t)
			}
			if end == 1 {
				return nil, errors.Errorf("invalid template '%s', named capture must not be empty", t)
			}
			if !isAlphaNum(remain[1:end]) {
				return nil, errors.Errorf("invalid template '%s', invalid named capture '%s'", t, remain[1:end-1])
			}

			res = append(res, remain[1:end])
			remain = remain[end+1:]
			continue
		}

		endIdx := strings.IndexFunc(remain, func(r rune) bool {
			return r < 48 || (r > 57 && r < 65) || (r > 90 && r < 97) || r > 122
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

// extractor extract one or more values from path base on pattern to result
// return true for success and false for failed
type regexExtractor interface {
	Extract(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool
}

// patExpander extract string by expanding template with the regexp pattern
type patExpander struct {
	template string
	applyFn  func(result *RouteResult, value string)
}

func (p *patExpander) Extract(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool {
	if len(matchIndex) == 0 {
		return false
	}
	value := pattern.ExpandString([]byte{}, p.template, path, matchIndex)
	p.applyFn(result, string(value))
	return true
}

// constExtractor is a extractor that always return a constant string
type constExtractor struct {
	value   string
	applyFn func(result *RouteResult, value string)
}

func (p *constExtractor) Extract(pattern *regexp.Regexp, path string, matchIndex []int, result *RouteResult) bool {
	p.applyFn(result, p.value)
	return true
}

type RouteResult struct {
	filter.Table
	Key         string
	Compression Compression
	Type        SourceType
}
