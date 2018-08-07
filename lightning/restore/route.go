package restore

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-tools/pkg/table-router"
)

// renameShardingTable replaces srcTable with dstTable in query
func renameShardingTable(query, srcTable, dstTable string) string {
	return replaceOne(query, srcTable, dstTable)
}

// renameShardingSchema replaces srcSchema with dstSchema in query
func renameShardingSchema(query, srcSchema, dstSchema string) string {
	return replaceOne(query, srcSchema, dstSchema)
}

// replaceOne works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func replaceOne(s, old, new string) string {
	old = fmt.Sprintf("`%s`", old)
	new = fmt.Sprintf("`%s`", new)
	return strings.Replace(s, old, new, 1)
}

func fetchMatchedLiteral(router *router.Table, schema, table string) (targetSchema string, targetTable string) {
	if schema == "" {
		// nothing change
		return schema, table
	}
	schemaL, tableL := strings.ToLower(schema), strings.ToLower(table)
	targetSchema, targetTable, err := router.Route(schemaL, tableL)
	if err != nil {
		common.AppLogger.Error(errors.ErrorStack(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		// nothing change
		return schema, table
	}
	if targetTable == "" {
		// table still same;
		targetTable = table
	}

	return targetSchema, targetTable
}
