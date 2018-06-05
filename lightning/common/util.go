package common

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"database/sql"
	"database/sql/driver"
	"path/filepath"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	tmysql "github.com/pingcap/tidb/mysql"
	log "github.com/sirupsen/logrus"
)

const (
	retryTimeout = 3 * time.Second

	defaultMaxRetry = 100
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
			log.Errorf("list file failed : %s", err.Error())
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

func QueryRowWithRetry(db *sql.DB, query string, dest ...interface{}) (err error) {
	maxRetry := defaultMaxRetry
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			log.Warnf("query %s retry %d: %v", query, i, dest)
			time.Sleep(retryTimeout)
		}

		err = db.QueryRow(query).Scan(dest...)
		if err != nil {
			if !isRetryableError(err) {
				return errors.Trace(err)
			}
			log.Warnf("query %s [error] %v", query, err)
			continue
		}

		return nil
	}

	return errors.Errorf("query sql [%s] failed", query)
}

// ExecWithRetry executes sqls with optional retry.
func ExecWithRetry(db *sql.DB, sqls []string) error {
	maxRetry := defaultMaxRetry

	if len(sqls) == 0 {
		return nil
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			log.Warnf("sql stmt_exec retry %d: %v", i, sqls)
			time.Sleep(retryTimeout)
		}

		if err = executeSQLImp(db, sqls); err != nil {
			if isRetryableError(err) {
				continue
			}
			log.Errorf("[exec][sql] %s [error] %v", sqls, err)
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Errorf("exec sqls [%v] failed, err:%s", sqls, err.Error())
}

func executeSQLImp(db *sql.DB, sqls []string) error {
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("exec sqls [%v] begin failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		log.Debugf("[exec][sql] %s", sqls[i])

		_, err = txn.Exec(sqls[i])
		if err != nil {
			log.Warnf("[exec][sql] %s [error]%v", sqls[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql] %s [error] %v", sqls[i], rerr)
			}
			// we should return the exec err, instead of the rollback rerr.
			return errors.Trace(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls [%v] commit failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}
	return nil
}

func isRetryableError(err error) bool {
	err = errors.Cause(err)
	if err == driver.ErrBadConn {
		return true
	}

	if nerr, ok := err.(net.Error); ok {
		return nerr.Timeout()
	}

	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		// ErrLockDeadlock can retry to commit while meet deadlock
		case tmysql.ErrUnknown, tmysql.ErrLockDeadlock, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable:
			return true
		default:
			return false
		}
	}

	return true
}

// UniqueTable returns an unique table name.
func UniqueTable(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}
