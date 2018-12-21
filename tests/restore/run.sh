#!/bin/sh

set -eu

# Populate the mydumper source
DBPATH="$TEST_DIR/restore.mydump"
TABLE_COUNT=35

mkdir -p $DBPATH
echo 'CREATE DATABASE restore_tsr;' > "$DBPATH/restore_tsr-schema-create.sql"
for i in $(seq "$TABLE_COUNT"); do
    echo "CREATE TABLE tbl$i(i TINYINT);" > "$DBPATH/restore_tsr.tbl$i-schema.sql"
    echo "INSERT INTO tbl$i VALUES (1);" > "$DBPATH/restore_tsr.tbl$i.sql"
done

# Count OpenEngine and CloseEngine events.
# Abort if number of unbalanced OpenEngine is >= 4
export GOFAIL_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/kv/FailIfEngineCountExceeds=return(4)'

# Start importing
run_sql 'DROP DATABASE IF EXISTS restore_tsr'
run_lightning
echo "Import finished"

# Verify all data are imported
for i in $(seq "$TABLE_COUNT"); do
    run_sql "SELECT sum(i) FROM restore_tsr.tbl$i;"
    check_contains 'sum(i): 1'
done
