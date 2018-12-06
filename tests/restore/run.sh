#!/bin/sh

set -eu

# Start the tikv-importer proxy to catch potential table concurrency violation
bin/importer_proxy --control 127.0.0.1:40250 --import-delay 500 &
shutdown_proxy() {
    curl -X POST http://127.0.0.1:40250/api/v1/shutdown
}
trap shutdown_proxy EXIT

# Wait until the importer_proxy server is ready to avoid spurious HTTP error
while ! curl -o /dev/null http://127.0.0.1:40250/ -s; do
    sleep 1
done

# Populate the mydumper source
DBPATH="$TEST_DIR/restore.mydump"
TABLE_COUNT=35

mkdir -p $DBPATH
echo 'CREATE DATABASE restore_tsr;' > "$DBPATH/restore_tsr-schema-create.sql"
for i in $(seq "$TABLE_COUNT"); do
    echo "CREATE TABLE tbl$i(i TINYINT);" > "$DBPATH/restore_tsr.tbl$i-schema.sql"
    echo "INSERT INTO tbl$i VALUES (1);" > "$DBPATH/restore_tsr.tbl$i.sql"
done

# Start importing
run_sql 'DROP DATABASE IF EXISTS restore_tsr'
run_lightning
echo "Import finished"

# Dump the import events, then count OpenEngine and CloseEngine events.
# Abort if number of unbalanced OpenEngine is >= 4
python2.7 tests/restore/count_open_engines.py 4 40250

# Verify all data are imported
for i in $(seq "$TABLE_COUNT"); do
    run_sql "SELECT sum(i) FROM restore_tsr.tbl$i;"
    check_contains 'sum(i): 1'
done
