package model

import "github.com/pingcap/tidb/model"

// DBInfo represents database information.
type DBInfo struct {
	Name   string
	Tables map[string]*TableInfo
}

// TableInfo represents table information.
type TableInfo struct {
	ID              int64
	DBName          string
	Name            string
	Columns         int
	Indices         int
	CreateTableStmt string
	Core            *model.TableInfo
}
