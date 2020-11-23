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

package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"

	"github.com/pingcap/tidb-lightning/lightning/common"
)

var _ = Suite(&checkReqSuite{})

type checkReqSuite struct{}

func (s *checkReqSuite) TestCheckVersion(c *C) {
	err := checkVersion("TiNB", *semver.New("2.1.0"), *semver.New("2.3.5"))
	c.Assert(err, IsNil)

	err = checkVersion("TiNB", *semver.New("2.3.5"), *semver.New("2.1.0"))
	c.Assert(err, ErrorMatches, "TiNB version too old.*")
}

func (s *checkReqSuite) TestCheckTiDBVersion(c *C) {
	var version string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/status")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"version": version,
		})
		c.Assert(err, IsNil)
	}))

	tls := common.NewTLSFromMockServer(mockServer)

	version = "5.7.25-TiDB-v9999.0.0"
	c.Assert(checkTiDBVersionByTLS(ctx, tls, requiredTiDBVersion), IsNil)

	version = "5.7.25-TiDB-v1.0.0"
	c.Assert(checkTiDBVersionByTLS(ctx, tls, requiredTiDBVersion), ErrorMatches, "TiDB version too old.*")
}

func (s *checkReqSuite) TestCheckPDVersion(c *C) {
	var version string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/pd/api/v1/config/cluster-version")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(version)
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	tls := common.NewTLSFromMockServer(mockServer)

	version = "9999.0.0"
	c.Assert(checkPDVersion(ctx, tls, mockURL.Host, requiredPDVersion), IsNil)

	version = "1.0.0"
	c.Assert(checkPDVersion(ctx, tls, mockURL.Host, requiredPDVersion), ErrorMatches, "PD version too old.*")
}

func (s *checkReqSuite) TestCheckTiKVVersion(c *C) {
	var versions []string
	ctx := context.Background()

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

	tls := common.NewTLSFromMockServer(mockServer)

	versions = []string{"9999.0.0", "9999.0.0"}
	c.Assert(checkTiKVVersion(ctx, tls, mockURL.Host, requiredTiKVVersion), IsNil)

	versions = []string{"4.1.0", "v4.1.0-alpha-9-ga27a7dd"}
	c.Assert(checkTiKVVersion(ctx, tls, mockURL.Host, requiredTiKVVersion), IsNil)

	versions = []string{"9999.0.0", "1.0.0"}
	c.Assert(checkTiKVVersion(ctx, tls, mockURL.Host, requiredTiKVVersion), ErrorMatches, `TiKV \(at tikv1\.test:20160\) version too old.*`)
}
