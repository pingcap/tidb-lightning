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

package common

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	retryTimeout = 3 * time.Second

	defaultMaxRetry = 3
)

func ToDSN(host string, port int, user string, psw string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", user, psw, host, port)
}

func ConnectDB(host string, port int, user string, psw string) (*sql.DB, error) {
	dbDSN := ToDSN(host, port, user, psw)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, errors.Trace(db.Ping())
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		return false
	}
	return f != nil && f.IsDir()
}

func QueryRowWithRetry(ctx context.Context, db *sql.DB, query string, dest ...interface{}) (err error) {
	maxRetry := defaultMaxRetry
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			AppLogger.Warnf("query %s retry %d", query, i)
			time.Sleep(retryTimeout)
		}

		err = db.QueryRowContext(ctx, query).Scan(dest...)
		if err != nil {
			if !IsRetryableError(err) {
				return errors.Trace(err)
			}
			AppLogger.Warnf("query %s [error] %v", query, err)
			continue
		}

		return nil
	}

	return errors.Errorf("query sql [%s] failed", query)
}

// TransactWithRetry executes an action in a transaction, and retry if the
// action failed with a retryable error.
func TransactWithRetry(ctx context.Context, db *sql.DB, purpose string, action func(context.Context, *sql.Tx) error) error {
	maxRetry := defaultMaxRetry

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			AppLogger.Warnf("transaction %s retry %d", purpose, i)
			time.Sleep(retryTimeout)
		}

		if err = transactImpl(ctx, db, purpose, action); err != nil {
			if IsRetryableError(err) {
				continue
			}
			if ShouldLogError(err) {
				AppLogger.Errorf("transaction %s [error] %v", purpose, err)
			}
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Annotatef(err, "transaction %s failed", purpose)
}

func transactImpl(ctx context.Context, db *sql.DB, purpose string, action func(context.Context, *sql.Tx) error) error {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Annotate(err, "begin transaction failed")
	}

	err = action(ctx, txn)
	if err != nil {
		AppLogger.Warnf("transaction %s [error]%v", purpose, err)
		rerr := txn.Rollback()
		if rerr != nil {
			if ShouldLogError(rerr) {
				AppLogger.Errorf("transaction %s [error] %v", purpose, rerr)
			}
		}
		// we should return the exec err, instead of the rollback rerr.
		// no need to errors.Trace() it, as the error comes from user code anyway.
		return err
	}

	err = txn.Commit()
	if err != nil {
		return errors.Annotate(err, "commit failed")
	}
	return nil
}

// ExecWithRetry executes a single SQL with optional retry.
func ExecWithRetry(ctx context.Context, db *sql.DB, purpose string, query string, args ...interface{}) error {
	return errors.Trace(TransactWithRetry(ctx, db, purpose, func(c context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(c, query, args...)
		return errors.Trace(err)
	}))
}

// IsRetryableError returns whether the error is transient (e.g. network
// connection dropped) or irrecoverable (e.g. user pressing Ctrl+C). This
// function returns `false` (irrecoverable) if `err == nil`.
func IsRetryableError(err error) bool {
	err = errors.Cause(err)

	switch err {
	case nil, context.Canceled, context.DeadlineExceeded, io.EOF:
		return false
	}

	switch nerr := err.(type) {
	case net.Error:
		return nerr.Timeout()
	case *mysql.MySQLError:
		switch nerr.Number {
		// ErrLockDeadlock can retry to commit while meet deadlock
		case tmysql.ErrUnknown, tmysql.ErrLockDeadlock, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable:
			return true
		default:
			return false
		}
	default:
		switch status.Code(err) {
		case codes.Unknown, codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return true
		default:
			return false
		}
	}
}

// ShouldLogError returns whether the error should be logged.
// This function should only be used for inhabiting logs related to canceling,
// where the log is usually just noise.
//
// This function returns `false` when:
//
//  - the error `IsContextCanceledError`
//  - the log level is above "debug"
//
// This function also returns `false` when `err == nil`.
func ShouldLogError(err error) bool {
	if err == nil {
		return false
	}
	if AppLogger.IsLevelEnabled(logrus.DebugLevel) {
		return true
	}
	return !IsContextCanceledError(err)
}

// IsContextCanceledError returns whether the error is caused by context
// cancellation. This function should only be used when the code logic is
// affected by whether the error is canceling or not. Normally, you should
// simply use ShouldLogError.
//
// This function returns `false` (not a context-canceled error) if `err == nil`.
func IsContextCanceledError(err error) bool {
	err = errors.Cause(err)
	return err == context.Canceled || status.Code(err) == codes.Canceled
}

// UniqueTable returns an unique table name.
func UniqueTable(schema string, table string) string {
	var builder strings.Builder
	WriteMySQLIdentifier(&builder, schema)
	builder.WriteByte('.')
	WriteMySQLIdentifier(&builder, table)
	return builder.String()
}

// Writes a MySQL identifier into the string builder.
// The identifier is always escaped into the form "`foo`".
func WriteMySQLIdentifier(builder *strings.Builder, identifier string) {
	builder.Grow(len(identifier) + 2)
	builder.WriteByte('`')

	// use a C-style loop instead of range loop to avoid UTF-8 decoding
	for i := 0; i < len(identifier); i++ {
		b := identifier[i]
		if b == '`' {
			builder.WriteString("``")
		} else {
			builder.WriteByte(b)
		}
	}

	builder.WriteByte('`')
}

// GetJSON fetches a page and parses it as JSON. The parsed result will be
// stored into the `v`. The variable `v` must be a pointer to a type that can be
// unmarshalled from JSON.
//
// Example:
//
//	client := &http.Client{}
//	var resp struct { IP string }
//	if err := util.GetJSON(client, "http://api.ipify.org/?format=json", &resp); err != nil {
//		return errors.Trace(err)
//	}
//	fmt.Println(resp.IP)
func GetJSON(client *http.Client, url string, v interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Errorf("get %s http status code != 200, message %s", url, string(body))
	}

	return errors.Trace(json.NewDecoder(resp.Body).Decode(v))
}

// KillMySelf sends sigint to current process, used in integration test only
func KillMySelf() error {
	return errors.Trace(syscall.Kill(syscall.Getpid(), syscall.SIGINT))
}
