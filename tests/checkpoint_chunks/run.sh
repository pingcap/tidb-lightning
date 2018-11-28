#!/bin/sh

set -euE

# Start the tikv-importer proxy so we could kill the progress immediately after each import
PIDFILE="$TEST_DIR/cpch.pid"
KILLER_PID=0
bin/importer_proxy --control 127.0.0.1:63804 --listen 127.0.0.1:19557 &
shutdown_proxy() {
    kill -9 "$KILLER_PID" || true
    curl -o "$TEST_DIR/cpch.dump.jsonl" http://127.0.0.1:63804/api/v1/dump
    curl -X POST http://127.0.0.1:63804/api/v1/shutdown
}
trap shutdown_proxy EXIT

# Populate the mydumper source
DBPATH="$TEST_DIR/cpch.mydump"
CHUNK_COUNT=5
ROW_COUNT=1000

mkdir -p $DBPATH
echo 'CREATE DATABASE cpch_tsr;' > "$DBPATH/cpch_tsr-schema-create.sql"
echo 'CREATE TABLE tbl(i BIGINT UNSIGNED PRIMARY KEY);' > "$DBPATH/cpch_tsr.tbl-schema.sql"
for i in $(seq "$CHUNK_COUNT"); do
    rm -f "$DBPATH/cpch_tsr.tbl.$i.sql"
    for j in $(seq "$ROW_COUNT"); do
        # the values run from ($ROW_COUNT + 1) to $CHUNK_COUNT*($ROW_COUNT + 1).
        echo "INSERT INTO tbl VALUES($(($i*$ROW_COUNT+$j)));" >> "$DBPATH/cpch_tsr.tbl.$i.sql"
    done
done

# Start the background killer (kill the lightning instance as soon as one chunk is imported)
# If checkpoint does work, this should only kill $CHUNK_COUNT instances of lightnings.
"tests/$TEST_NAME/kill_lightning_after_one_chunk.py" "$PIDFILE" &
KILLER_PID="$!"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cpch_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cpch'

set +e
for i in $(seq "$CHUNK_COUNT"); do
    echo "******** Importing Chunk Now (step $i/$CHUNK_COUNT) ********"
    PIDFILE="$PIDFILE" run_lightning
done
set -e

# After everything is done, there should be no longer new calls to WriteEngine/CloseAndRecv
# (and thus `kill_lightning_after_one_chunk` will spare this final check)
echo "******** Verify checkpoint no-op ********"
PIDFILE="$PIDFILE" run_lightning
run_sql 'SELECT count(i), sum(i) FROM cpch_tsr.tbl;'
check_contains "count(i): $(($ROW_COUNT*$CHUNK_COUNT))"
check_contains "sum(i): $(( $ROW_COUNT*$CHUNK_COUNT*(($CHUNK_COUNT+2)*$ROW_COUNT + 1)/2 ))"
run_sql "SELECT count(*) FROM tidb_lightning_checkpoint_test_cpch.table_v1 WHERE status = 180"
check_contains "count(*): 1"
run_sql "SELECT count(*) FROM tidb_lightning_checkpoint_test_cpch.chunk_v3 WHERE pos = end_offset"
check_contains "count(*): $CHUNK_COUNT"

# Repeat, but using the file checkpoint
run_sql 'DROP DATABASE IF EXISTS cpch_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cpch'
rm -f "$TEST_DIR/cpch.pb"

set +e
for i in $(seq "$CHUNK_COUNT"); do
    echo "******** Importing Chunk using File checkpoint Now (step $i/$CHUNK_COUNT) ********"
    PIDFILE="$PIDFILE" run_lightning file
done
set -e

echo "******** Verify File checkpoint no-op ********"
PIDFILE="$PIDFILE" run_lightning file
run_sql 'SELECT count(i), sum(i) FROM cpch_tsr.tbl;'
check_contains "count(i): $(($ROW_COUNT*$CHUNK_COUNT))"
check_contains "sum(i): $(( $ROW_COUNT*$CHUNK_COUNT*(($CHUNK_COUNT+2)*$ROW_COUNT + 1)/2 ))"
[ -f "$TEST_DIR/cpch.pb" ]
