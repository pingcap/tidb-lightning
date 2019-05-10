package config_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func startMockServer(c *C, statusCode int, content string) (*httptest.Server, string, int) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		fmt.Fprint(w, content)
	}))

	url, err := url.Parse(ts.URL)
	c.Assert(err, IsNil)
	host, portString, err := net.SplitHostPort(url.Host)
	c.Assert(err, IsNil)
	port, err := strconv.Atoi(portString)
	c.Assert(err, IsNil)

	return ts, host, port
}

func (s *configTestSuite) TestAdjustPdAddrAndPort(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK,
		`{"port":4444,"advertise-address":"","path":"123.45.67.89:1234,56.78.90.12:3456"}`,
	)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port

	err := cfg.Adjust()
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 4444)
	c.Assert(cfg.TiDB.PdAddr, Equals, "123.45.67.89:1234")
}

func (s *configTestSuite) TestAdjustPdAddrAndPortViaAdvertiseAddr(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK,
		`{"port":6666,"advertise-address":"121.212.121.212:5555","path":"34.34.34.34:3434"}`,
	)
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port

	err := cfg.Adjust()
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 6666)
	c.Assert(cfg.TiDB.PdAddr, Equals, "34.34.34.34:3434")
}

func (s *configTestSuite) TestAdjustPageNotFound(c *C) {
	ts, host, port := startMockServer(c, http.StatusNotFound, "{}")
	defer ts.Close()

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port

	err := cfg.Adjust()
	c.Assert(err, ErrorMatches, ".*404 Not Found.*")
}

func (s *configTestSuite) TestAdjustConnectRefused(c *C) {
	ts, host, port := startMockServer(c, http.StatusOK, "{}")

	cfg := config.NewConfig()
	cfg.TiDB.Host = host
	cfg.TiDB.StatusPort = port

	ts.Close() // immediately close to ensure connection refused.

	err := cfg.Adjust()
	c.Assert(err, ErrorMatches, "cannot fetch settings from TiDB.*")
}

func (s *configTestSuite) TestAdjustWillNotContactServerIfEverythingIsDefined(c *C) {
	cfg := config.NewConfig()
	cfg.TiDB.Host = "123.45.67.89"
	cfg.TiDB.Port = 4567
	cfg.TiDB.StatusPort = 8901
	cfg.TiDB.PdAddr = "234.56.78.90:12345"

	err := cfg.Adjust()
	c.Assert(err, IsNil)
	c.Assert(cfg.TiDB.Port, Equals, 4567)
	c.Assert(cfg.TiDB.PdAddr, Equals, "234.56.78.90:12345")
}

func (s *configTestSuite) TestInvalidCSV(c *C) {
	d := c.MkDir()
	p := path.Join(d, "cfg.toml")

	testCases := []struct {
		input string
		err   string
	}{
		{
			input: `
				[mydumper.csv]
				separator = ''
			`,
			err: "invalid config: `mydumper.csv.separator` must be exactly one byte long",
		},
		{
			input: `
				[mydumper.csv]
				separator = 'hello'
			`,
			err: "invalid config: `mydumper.csv.separator` must be exactly one byte long",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\'
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				separator = '，'
			`,
			err: "invalid config: `mydumper.csv.separator` must be exactly one byte long",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = ''
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = 'hello'
			`,
			err: "invalid config: `mydumper.csv.delimiter` must be one byte long or empty",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
			`,
			err: "",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '“'
			`,
			err: "invalid config: `mydumper.csv.delimiter` must be one byte long or empty",
		},
		{
			input: `
				[mydumper.csv]
				separator = '|'
				delimiter = '|'
			`,
			err: "invalid config: cannot use the same character for both CSV delimiter and separator",
		},
		{
			input: `
				[mydumper.csv]
				separator = '\'
				backslash-escape = true
			`,
			err: "invalid config: cannot use '\\' as CSV separator when `mydumper.csv.backslash-escape` is true",
		},
		{
			input: `
				[mydumper.csv]
				delimiter = '\'
				backslash-escape = true
			`,
			err: "invalid config: cannot use '\\' as CSV delimiter when `mydumper.csv.backslash-escape` is true",
		},
	}

	for _, tc := range testCases {
		comment := Commentf("input = %s", tc.input)

		cfg := config.NewConfig()
		cfg.ConfigFile = p
		err := ioutil.WriteFile(p, []byte(tc.input), 0644)
		c.Assert(err, IsNil, comment)

		err = cfg.Load()
		if tc.err != "" {
			c.Assert(err, ErrorMatches, regexp.QuoteMeta(tc.err), comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}
