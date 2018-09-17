package common

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"database/sql"
	"path/filepath"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	tmysql "github.com/pingcap/tidb/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	retryTimeout = 3 * time.Second

	defaultMaxRetry = 3
)

func Percent(a int, b int) string {
	return fmt.Sprintf("%.2f %%", float64(a)/float64(b)*100)
}

func ConnectDB(host string, port int, user string, psw string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", user, psw, host, port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, errors.Trace(db.Ping())
}

func GetFileSize(file string) (int64, error) {
	fd, err := os.Open(file)
	if err != nil {
		return -1, errors.Trace(err)
	}
	defer fd.Close()

	fstat, err := fd.Stat()
	if err != nil {
		return -1, errors.Trace(err)
	}

	return fstat.Size(), nil
}

func FileExists(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		return false
	}
	return f != nil && f.IsDir()
}

func EnsureDir(dir string) error {
	if !FileExists(dir) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func ListFiles(dir string) map[string]string {
	files := make(map[string]string)
	filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			AppLogger.Errorf("list file failed : %s", err.Error())
			return nil
		}

		if f == nil {
			return nil
		}

		if f.IsDir() {
			return nil
		}

		// relPath, _ := filepath.Rel(dir, path)
		fname := strings.TrimSpace(f.Name())
		files[path] = fname

		return nil
	})

	return files
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
			AppLogger.Errorf("transaction %s [error] %v", purpose, err)
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Annotatef(err, "transaction %s failed", purpose)
}

func transactImpl(ctx context.Context, db *sql.DB, purpose string, action func(context.Context, *sql.Tx) error) error {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		AppLogger.Errorf("transaction %s begin failed %v", purpose, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = action(ctx, txn)
	if err != nil {
		AppLogger.Warnf("transaction %s [error]%v", purpose, err)
		rerr := txn.Rollback()
		if rerr != nil {
			AppLogger.Errorf("transaction %s [error] %v", purpose, rerr)
		}
		// we should return the exec err, instead of the rollback rerr.
		// no need to errors.Trace() it, as the error comes from user code anyway.
		return err
	}

	err = txn.Commit()
	if err != nil {
		AppLogger.Errorf("transaction %s commit failed %v", purpose, errors.ErrorStack(err))
		return errors.Trace(err)
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

// IsContextCanceledError returns whether the error is caused by context
// cancellation.
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
