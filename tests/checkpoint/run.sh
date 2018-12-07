#!/bin/sh

set -euE

# Start the tikv-importer proxy so we could kill the progress immediately after each import
PIDFILE="$TEST_DIR/cppk.pid"
KILLER_PID=0
bin/importer_proxy --control 127.0.0.1:38227 --listen 127.0.0.1:20180 &
shutdown_proxy() {
    kill -9 "$KILLER_PID" || true
    curl -o "$TEST_DIR/cppk.dump.jsonl" http://127.0.0.1:38227/api/v1/dump
    curl -X POST http://127.0.0.1:38227/api/v1/shutdown
}
trap shutdown_proxy EXIT

# Populate the mydumper source
DBPATH="$TEST_DIR/cppk.mydump"
TABLE_COUNT=9
CHUNK_COUNT=50

mkdir -p $DBPATH
echo 'CREATE DATABASE cppk_tsr;' > "$DBPATH/cppk_tsr-schema-create.sql"
PARTIAL_IMPORT_QUERY='SELECT 0'
for i in $(seq "$TABLE_COUNT"); do
    case $i in
        1)
            INDICES="PRIMARY KEY"
            ;;
        2)
            INDICES="UNIQUE"
            ;;
        3)
            INDICES=", INDEX(j)"
            ;;
        4)
            INDICES=", PRIMARY KEY(i, j)"
            ;;
        5)
            INDICES=", UNIQUE KEY(j)"
            ;;
        6)
            INDICES=", PRIMARY KEY(j)"
            ;;
        *)
            INDICES=""
            ;;
    esac
    echo "CREATE TABLE tbl$i(i TINYINT, j INT $INDICES);" > "$DBPATH/cppk_tsr.tbl$i-schema.sql"
    PARTIAL_IMPORT_QUERY="$PARTIAL_IMPORT_QUERY + coalesce((SELECT sum(j) FROM cppk_tsr.tbl$i), 0)"
    for j in $(seq "$CHUNK_COUNT"); do
        echo "INSERT INTO tbl$i VALUES ($i,${j}000),($i,${j}001);" > "$DBPATH/cppk_tsr.tbl$i.$j.sql"
    done
done
PARTIAL_IMPORT_QUERY="$PARTIAL_IMPORT_QUERY AS s;"

# Start the background killer (kill the lightning instance as soon as one table is imported)
# If checkpoint does work, this should only kill 9 instances of lightnings.
"tests/$TEST_NAME/kill_lightning_after_one_import.py" "$PIDFILE" &
KILLER_PID="$!"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cppk'

set +e
for i in $(seq "$TABLE_COUNT"); do
    echo "******** Importing Table Now (step $i/$TABLE_COUNT) ********"
    PIDFILE="$PIDFILE" run_lightning
done
set -e

# After everything is done, there should be no longer new calls to ImportEngine
# (and thus `kill_lightning_after_one_import` will spare this final check)
echo "******** Verify checkpoint no-op ********"
PIDFILE="$PIDFILE" run_lightning
run_sql "$PARTIAL_IMPORT_QUERY"
check_contains "s: $(( (1000 * $CHUNK_COUNT + 1001) * $CHUNK_COUNT * $TABLE_COUNT ))"
run_sql "SELECT count(*) FROM tidb_lightning_checkpoint_test_cppk.table_v1 WHERE status >= 200"
check_contains "count(*): $TABLE_COUNT"
