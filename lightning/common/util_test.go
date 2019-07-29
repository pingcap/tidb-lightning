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

package common_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type utilSuite struct{}

var _ = Suite(&utilSuite{})

func (s *utilSuite) TestDirNotExist(c *C) {
	c.Assert(common.IsDirExists("."), IsTrue)
	c.Assert(common.IsDirExists("not-exists"), IsFalse)
}

func (s *utilSuite) TestGetJSON(c *C) {
	type TestPayload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	var request = TestPayload{
		Username: "lightning",
		Password: "lightning-ctl",
	}

	// Mock success response
	handle := func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
		err := json.NewEncoder(res).Encode(request)
		c.Assert(err, IsNil)
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		handle(res, req)
	}))
	defer testServer.Close()

	client := &http.Client{Timeout: time.Second}

	response := TestPayload{}
	err := common.GetJSON(client, "http://not-exists", &response)
	c.Assert(err, NotNil)
	err = common.GetJSON(client, testServer.URL, &response)
	c.Assert(err, IsNil)
	c.Assert(request, DeepEquals, response)

	// Mock `StatusNoContent` response
	handle = func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusNoContent)
	}
	err = common.GetJSON(client, testServer.URL, &response)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*http status code != 200.*")
}

func (s *utilSuite) TestIsRetryableError(c *C) {
	c.Assert(common.IsRetryableError(context.Canceled), IsFalse)
	c.Assert(common.IsRetryableError(context.DeadlineExceeded), IsFalse)
	c.Assert(common.IsRetryableError(io.EOF), IsFalse)
	c.Assert(common.IsRetryableError(&net.AddrError{}), IsFalse)
	c.Assert(common.IsRetryableError(&net.DNSError{}), IsFalse)
	c.Assert(common.IsRetryableError(&net.DNSError{IsTimeout: true}), IsTrue)

	// MySQL Errors
	c.Assert(common.IsRetryableError(&mysql.MySQLError{}), IsFalse)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrUnknown}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrLockDeadlock}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrPDServerTimeout}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerTimeout}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerBusy}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrResolveLockTimeout}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrRegionUnavailable}), IsTrue)
	c.Assert(common.IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrWriteConflictInTiDB}), IsTrue)

	// gRPC Errors
	c.Assert(common.IsRetryableError(status.Error(codes.Canceled, "")), IsFalse)
	c.Assert(common.IsRetryableError(status.Error(codes.Unknown, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.DeadlineExceeded, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.NotFound, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.AlreadyExists, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.PermissionDenied, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.ResourceExhausted, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.Aborted, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.OutOfRange, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.Unavailable, "")), IsTrue)
	c.Assert(common.IsRetryableError(status.Error(codes.DataLoss, "")), IsTrue)

	// sqlmock errors
	c.Assert(common.IsRetryableError(fmt.Errorf("call to database Close was not expected")), IsFalse)
	c.Assert(common.IsRetryableError(errors.New("call to database Close was not expected")), IsTrue)
}
