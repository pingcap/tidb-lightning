package mydump

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/tidb-lightning/lightning/config"
)

var _ = Suite(&testFileRouterSuite{})

type testFileRouterSuite struct{}

func (t *testFileRouterSuite) TestRouteParser(c *C) {
	// valid rules
	rules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1", "$2", "$3", "", ""},
		{`^.+\.(csv|sql)`, "test", "t", "$1", "", ""},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$schema", "$table", "$type", "$key", "$cp"},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "${schema}s", "$table", "${3}_0", "$4", "$cp"},
		{`^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.([0-9]+))?\.(csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "${1}s", "$table", "${3}_0", "$4", "$cp"},
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1-schema", "$1-table", "$2", "", ""},
	}
	for _, r := range rules {
		_, err := NewFileRouter([]*config.FileRouteRule{r})
		c.Assert(err, IsNil)
	}

	// invalid rules
	invalidRules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*(?P<schema>\.(?P<table>[^./]+).*$`, "$test", "$table", "", "", ""},
		{`^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table>[^./]+).*$`, "$schemas", "$table", "", "", ""},
		{`^(?:[^/]*/)*([^/.]+)\.([^./]+)(?:\.[0-9]+)?\.(csv|sql)`, "$1", "$2", "$3", "", "$4"},
	}
	for _, r := range invalidRules {
		_, err := NewFileRouter([]*config.FileRouteRule{r})
		c.Assert(err, NotNil)
	}
}

func (t *testFileRouterSuite) TestSingleRouteRule(c *C) {
	rules := []*config.FileRouteRule{
		{`^(?:[^/]*/)*([^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "$1", "$table", "$type", "$key", "$cp"},
	}

	r, err := NewFileRouter(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"my_schema.my_table.sql":           {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql": {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":    {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":      {"my_schema", "my_table", "0001", "", "sql"},
	}
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		compress, e := parseCompressionType(fields[3])
		c.Assert(e, IsNil)
		ty, e := parseSourceType(fields[4])
		c.Assert(e, IsNil)
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

	r, err := NewFileRouter(rules)
	c.Assert(err, IsNil)

	inputOutputMap := map[string][]string{
		"test-schema-create.sql":           {"test", "", "", "", SchemaSchema},
		"test.t-schema.sql":                {"test", "t", "", "", TableSchema},
		"my_schema.my_table.sql":           {"my_schema", "my_table", "", "", "sql"},
		"/test/123/my_schema.my_table.sql": {"my_schema", "my_table", "", "", "sql"},
		"my_dir/my_schema.my_table.csv":    {"my_schema", "my_table", "", "", "csv"},
		"my_schema.my_table.0001.sql":      {"my_schema", "my_table", "0001", "", "sql"},
		//"my_schema.my_table.0001.sql.gz":      {"my_schema", "my_table", "0001", "gz", "sql"},
		"my_schema.my_table.0001.sql.gz": {},
	}
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		if len(fields) == 0 {
			c.Assert(res, IsNil)
		} else {
			compress, e := parseCompressionType(fields[3])
			c.Assert(e, IsNil)
			ty, e := parseSourceType(fields[4])
			c.Assert(e, IsNil)
			exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
			c.Assert(res, DeepEquals, exp)
		}
	}

	// multi rule don't intersect with each other
	// add another rule that math same pattern with the third rule, the result should be no different
	p := &config.FileRouteRule{`^(?P<schema>[^/.]+)\.(?P<table>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`, "test_schema", "test_table", "$type", "$key", "$cp"}
	rules = append(rules, p)
	r, err = NewFileRouter(rules)
	c.Assert(err, IsNil)
	for path, fields := range inputOutputMap {
		res := r.Route(path)
		if len(fields) == 0 {
			c.Assert(res, IsNil)
		} else {
			compress, e := parseCompressionType(fields[3])
			c.Assert(e, IsNil)
			ty, e := parseSourceType(fields[4])
			c.Assert(e, IsNil)
			exp := &RouteResult{filter.Table{Schema: fields[0], Name: fields[1]}, fields[2], compress, ty}
			c.Assert(res, DeepEquals, exp)
		}
	}
}

func (t *testFileRouterSuite) TestRouteExpanding(c *C) {
	rule := &config.FileRouteRule{
		Pattern:     `^(?:[^/]*/)*(?P<schema>[^/.]+)\.(?P<table_name>[^./]+)(?:\.(?P<key>[0-9]+))?\.(?P<type>csv|sql)(?:\.(?P<cp>[A-Za-z0-9]+))?$`,
		Schema:      "$schema",
		Type:        "$type",
		Key:         "$key",
		Compression: "$cp",
	}
	path := "db.table.001.sql"
	tablePatternResMap := map[string]string{
		"$schema":             "db",
		"$table_name":         "table",
		"$schema.$table_name": "db.table",
		"${1}":                "db",
		"${1}_$table_name":    "db_table",
		"${2}.schema":         "table.schema",
		"$${2}":               "${2}",
		"$$table_name":        "$table_name",
		"$table_name-123":     "table-123",
		"$$12$1$schema":       "$12dbdb",
		"${table_name}$$2":    "table$2",
		"${table_name}$$":     "table$",
		"{1}$$":               "{1}$",
	}

	for pat, value := range tablePatternResMap {
		rule.Table = pat
		router, err := NewFileRouter([]*config.FileRouteRule{rule})
		c.Assert(err, IsNil)
		res := router.Route(path)
		c.Assert(res, NotNil)
		c.Assert(res.Name, Equals, value)
	}

	invalidPatterns := []string{"$1_$schema", "$table_name$", "${1", "${schema", "$schema_$table_name"}
	for _, pat := range invalidPatterns {
		rule.Table = pat
		_, err := NewFileRouter([]*config.FileRouteRule{rule})
		c.Assert(err, NotNil)
	}
}
