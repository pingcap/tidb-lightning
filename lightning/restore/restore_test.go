package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-lightning/lightning/common"
)

var _ = Suite(&restoreSuite{})

type restoreSuite struct{}

func (s *restoreSuite) TestNewTableRestore(c *C) {
	testCases := []struct {
		name       string
		createStmt string
		errRegexp  string
	}{
		{"t1", "CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", ""},
		{"t2", "CREATE TABLE `t2` (`c1` varchar(30000) NOT NULL)", "failed to ExecDDLSQL `mockdb`.`t2`:.*"},
	}

	dbInfo := &TidbDBInfo{Name: "mockdb", Tables: map[string]*TidbTableInfo{}}
	for _, c := range testCases {
		dbInfo.Tables[c.name] = &TidbTableInfo{
			Name:            c.name,
			CreateTableStmt: c.createStmt,
		}
	}

	for _, tc := range testCases {
		tableInfo := dbInfo.Tables[tc.name]
		tableName := common.UniqueTable("mockdb", tableInfo.Name)
		tr, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &TableCheckpoint{})
		if tc.errRegexp != "" {
			c.Assert(err, ErrorMatches, tc.errRegexp)
		} else {
			c.Assert(tr, NotNil)
			c.Assert(err, IsNil)
		}
	}
}
