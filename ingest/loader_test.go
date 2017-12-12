package ingest

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"
)

func initDB(dbName string) *sql.DB {
	db := ConnectDB("localhost", 3306, "root", "")
	db.Exec("create database if not exists " + dbName)
	db.Exec("use " + dbName)

	return db
}

func clear(db *sql.DB, dbName string) {
	db.Exec("drop database " + dbName)
}

func prerun(dbName string) {
	db := initDB(dbName)
	clear(db, dbName)
	db.Close()
}

func checkTableData(db *sql.DB) {
	count := 0

	sql := "select count(distinct ID) cnt from `tbl_autoid`"
	db.QueryRow(sql).Scan(&count)
	if count != 10000 {
		panic(fmt.Sprintf("%d vs %d", count, 10000))
	}

	sql = "select count(distinct Name) cnt from `tbl_multi_index`"
	db.QueryRow(sql).Scan(&count)
	if count != 10000 {
		panic(fmt.Sprintf("%d vs %d", count, 10000))
	}
}

func loadDataIntoMysql(dbMeta *MDDatabaseMeta, maxBlockSize int64) {
	db := initDB(dbMeta.Name)
	defer func() {
		clear(db, dbMeta.Name)
		db.Close()
	}()

	start := time.Now()
	for _, tblMeta := range dbMeta.Tables {
		// schema
		sqlCreteTable, _ := ExportStatment(tblMeta.SchemaFile)
		db.Exec(string(sqlCreteTable))

		// data
		for _, file := range tblMeta.DataFiles {
			reader, _ := NewMDDataReader(file, 0)
			for {
				sqlData, err := reader.Read(maxBlockSize)
				if err == io.EOF {
					break
				}

				statment := string(sqlData)
				for _, sql := range strings.Split(statment, ";") {
					sql = strings.TrimSpace(sql)
					if len(sql) > 0 {
						_, err = db.Exec(sql)
						if err != nil {
							fmt.Println(err.Error())
						}
					}
				}
			}
			reader.AssertEnd()
			reader.Close()
		}
	}
	fmt.Printf("cost = %.2f sec\n", time.Since(start).Seconds())

	checkTableData(db)
}

func TestLoaderReader() {
	cfg := &Config{
		Source: DataSource{
			Type: "mydumper",
			URL:  "/Users/silentsai/mys/mydumper-datas",
		},
	}

	mdl := NewMyDumpLoader(cfg)
	dbMeta := mdl.GetTree()
	prerun(dbMeta.Name)

	var maxSize int64 = 1024 * 128
	var minSize int64 = 1024

	for blockSize := minSize; blockSize <= maxSize; blockSize += 1024 {
		fmt.Printf("block size = %d\n", blockSize)
		loadDataIntoMysql(dbMeta, blockSize)
	}

	fmt.Println("end")
}
