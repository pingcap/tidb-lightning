package common

import (
	"fmt"
	"os"
	"strings"

	"database/sql"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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
