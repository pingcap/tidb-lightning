package mydump

import (
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-tools/pkg/filter"

	. "github.com/pingcap/check"
)

var _ = Suite(&testFileRouterSuite{})

type testFileRouterSuite struct{}

func (t *testFileRouterSuite) TestRouteParser(c *C) {
	// valid rules
	rules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1", "$2", "$3", "", ""},
		{`^.+\.(csv|sql)`, "test", "t", "$1", "", ""},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$schema", "$table", "$type", "$key", "$cp"},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$(schema)s", "$table", "$(3)_0", "$4", "$cp"},
		{`^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$(1)s", "$table", "$(3)_0", "$4", "$cp"},
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1_schema", "$1_table", "$2", "", ""},
	}
	for _, r := range rules {
		_, err := Parse([]*config.FileRouteRule{r})
		c.Assert(err, IsNil)
	}

	// invalid rules
	invalidRules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*(?P<schema>\.(?P<table>[^./]+).*$`, "$test", "$table", "", "", ""},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+).*$`, "$schemas", "$table", "", "", ""},
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1", "$2", "$3", "", "$4"},
	}
	for _, r := range invalidRules {
		_, err := Parse([]*config.FileRouteRule{r})
		c.Assert(err, NotNil)
	}
}

func (t *testFileRouterSuite) TestSingleRouteRule(c *C) {
	rules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$1", "$table", "$type", "$key", "$cp"},
	}

	r, err := Parse(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"my_schema.my_table.sql":              {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql":    {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":       {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":         {"my_schema", "my_table", "0001", "", "sql"},
		"my_schema.my_table.0001.sql.gz":      {"my_schema", "my_table", "0001", "gz", "sql"},
		"my_schema.my_table.0001.sql.part001": {"my_schema", "my_table", "0001", "", "sql"},
	}
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		compress := parseCompressionType(fields[3])
		ty := parseSourceType(fields[4])
		exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
		c.Assert(res, DeepEquals, exp)
	}

	notMatchPaths := []string{
		"my_table.sql",
		"/schema/table.sql",
		"my_schema.my_table.txt",
		"my_schema.my_table.001.txt",
		"my_schema.my_table.0001-002.sql",
	}
	for _, p := range notMatchPaths {
		c.Assert(r.Route(p), IsNil)
	}
}

func (t *testFileRouterSuite) TestMultiRouteRule(c *C) {
	// multi rule don't intersect with each other
	rules := []*config.FileRouteRule{
		{`(?:[^/]*/)*([^/.]+)-schema-create\.sql`, "$1", "", SchemaSchema, "", ""},
		{`(?:[^/]*/)*([^/.]+)\.([^/.]+)-schema\.sql`, "$1", "$2", TableSchema, "", ""},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$schema", "$table", "$type", "$key", "$cp"},
	}

	r, err := Parse(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"test-schema-create.sql":              {"test", "", "", "", SchemaSchema},
		"test.t-schema.sql":                   {"test", "t", "", "", TableSchema},
		"my_schema.my_table.sql":              {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql":    {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":       {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":         {"my_schema", "my_table", "0001", "", "sql"},
		"my_schema.my_table.0001.sql.gz":      {"my_schema", "my_table", "0001", "gz", "sql"},
		"my_schema.my_table.0001.sql.part001": {"my_schema", "my_table", "0001", "", "sql"},
	}
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		compress := parseCompressionType(fields[3])
		ty := parseSourceType(fields[4])
		exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
		c.Assert(res, DeepEquals, exp)
	}

	// multi rule don't intersect with each other
	// add another rule that math same pattern with the third rule, the result should be no different
	p := &config.FileRouteRule{`^(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "test_schema", "test_table", "$type", "$key", "$cp"}
	rules = append(rules, p)
	r, err = Parse(rules)
	c.Assert(err, IsNil)
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		compress := parseCompressionType(fields[3])
		ty := parseSourceType(fields[4])
		exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
		c.Assert(res, DeepEquals, exp)
	}
}
