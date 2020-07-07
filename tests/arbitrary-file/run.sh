#!/bin/sh

set -eu

TEST_NAME=arbitrary-file
DB=$TEST_NAME
make_file_args() {
    result=""
    for f in $@; do
        result="$result -from-file $f "
    done
    echo $result
}

sbtable="CREATE TABLE \`$DB\`.\`sbtest1\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`k\` int(11) NOT NULL DEFAULT 0,
  \`c\` char(120) NOT NULL DEFAULT '',
  \`pad\` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (\`id\`),
  KEY \`k_1\` (\`k\`)
);"

run_sql "CREATE DATABASE \`$DB\`"
run_sql "$sbtable"

make_file_args tests/$TEST_NAME/datas/*.csv
csvs=(tests/$TEST_NAME/datas/*.csv)
run_lightning `make_file_args ${csvs[@]:2}` -to-db $DB -to-table sbtest1 --backend tidb --tidb-port 4000
run_sql "select count(*) from \`$DB\`.sbtest1"
check_contains "count(*): 100"

run_lightning `make_file_args ${csvs[@]:0:2}` -to-db $DB -to-table sbtest1 --backend tidb --tidb-port 4000
run_sql "select count(*) from \`$DB\`.sbtest1"
check_contains "count(*): 200"