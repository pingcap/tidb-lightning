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

package restore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
)

var _ = Suite(&checkReqSuite{})

type checkReqSuite struct{}

func (s *checkReqSuite) TestExtractTiDBVersion(c *C) {
	vers, err := extractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	vers, err = extractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.4"))

	vers, err = extractTiDBVersion("5.7.10-TiDB-v2.0.7")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.7"))

	vers, err = extractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = extractTiDBVersion("5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.0-beta"))

	vers, err = extractTiDBVersion("8.0.12-TiDB-v3.0.5-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5"))

	vers, err = extractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = extractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	_, err = extractTiDBVersion("")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = extractTiDBVersion("8.0.12")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = extractTiDBVersion("not-a-valid-version")
	c.Assert(err, NotNil)
}

func (s *checkReqSuite) TestCheckVersion(c *C) {
	err := checkVersion("TiNB", *semver.New("2.1.0"), *semver.New("2.3.5"))
	c.Assert(err, IsNil)

	err = checkVersion("TiNB", *semver.New("2.3.5"), *semver.New("2.1.0"))
	c.Assert(err, ErrorMatches, "TiNB version too old.*")
}

func (s *checkReqSuite) TestCheckTiDBVersion(c *C) {
	var version string

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/status")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"version": version,
		})
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)
	mockPort, err := strconv.Atoi(mockURL.Port())
	c.Assert(err, IsNil)

	rc := &RestoreController{
		cfg: &config.Config{
			TiDB: config.DBStore{
				Host:       mockURL.Hostname(),
				StatusPort: mockPort,
			},
		},
		tls: common.NewTLSFromMockServer(mockServer),
	}

	version = "5.7.25-TiDB-v9999.0.0"
	c.Assert(rc.checkTiDBVersion(), IsNil)

	version = "5.7.25-TiDB-v1.0.0"
	c.Assert(rc.checkTiDBVersion(), ErrorMatches, "TiDB version too old.*")
}

func (s *checkReqSuite) TestCheckPDVersion(c *C) {
	var version string

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/pd/api/v1/config/cluster-version")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(version)
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	rc := &RestoreController{
		cfg: &config.Config{
			TiDB: config.DBStore{
				PdAddr: mockURL.Host,
			},
		},
		tls: common.NewTLSFromMockServer(mockServer),
	}

	version = "9999.0.0"
	c.Assert(rc.checkPDVersion(), IsNil)

	version = "1.0.0"
	c.Assert(rc.checkPDVersion(), ErrorMatches, "PD version too old.*")
}

func (s *checkReqSuite) TestCheckTiKVVersion(c *C) {
	var versions []string

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/pd/api/v1/stores")
		w.WriteHeader(http.StatusOK)

		stores := make([]map[string]interface{}, 0, len(versions))
		for i, v := range versions {
			stores = append(stores, map[string]interface{}{
				"store": map[string]interface{}{
					"address": fmt.Sprintf("tikv%d.test:20160", i),
					"version": v,
				},
			})
		}
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"count":  len(versions),
			"stores": stores,
		})
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	rc := &RestoreController{
		cfg: &config.Config{
			TiDB: config.DBStore{
				PdAddr: mockURL.Host,
			},
		},
		tls: common.NewTLSFromMockServer(mockServer),
	}

	versions = []string{"9999.0.0", "9999.0.0"}
	c.Assert(rc.checkTiKVVersion(), IsNil)

	versions = []string{"4.1.0", "v4.1.0-alpha-9-ga27a7dd"}
	c.Assert(rc.checkTiKVVersion(), IsNil)

	versions = []string{"9999.0.0", "1.0.0"}
	c.Assert(rc.checkTiKVVersion(), ErrorMatches, `TiKV \(at tikv1\.test:20160\) version too old.*`)
}
