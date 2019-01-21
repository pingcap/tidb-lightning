#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -euE

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

# Set the failpoint to kill the lightning instance as soon as one chunk is imported
# If checkpoint does work, this should only kill $CHUNK_COUNT instances of lightnings.
export GOFAIL_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/restore/FailIfImportedChunk=return'

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cpch_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cpch'

set +e
for i in $(seq "$CHUNK_COUNT"); do
    echo "******** Importing Chunk Now (step $i/$CHUNK_COUNT) ********"
    run_lightning 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

# After everything is done, there should be no longer new calls to WriteEngine/CloseAndRecv
# (and thus `kill_lightning_after_one_chunk` will spare this final check)
echo "******** Verify checkpoint no-op ********"
run_lightning
run_sql 'SELECT count(i), sum(i) FROM cpch_tsr.tbl;'
check_contains "count(i): $(($ROW_COUNT*$CHUNK_COUNT))"
check_contains "sum(i): $(( $ROW_COUNT*$CHUNK_COUNT*(($CHUNK_COUNT+2)*$ROW_COUNT + 1)/2 ))"
run_sql "SELECT count(*) FROM tidb_lightning_checkpoint_test_cpch.table_v4 WHERE status >= 200"
check_contains "count(*): 1"

# Repeat, but using the file checkpoint
run_sql 'DROP DATABASE IF EXISTS cpch_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cpch'
rm -f "$TEST_DIR/cpch.pb"

set +e
for i in $(seq "$CHUNK_COUNT"); do
    echo "******** Importing Chunk using File checkpoint Now (step $i/$CHUNK_COUNT) ********"
    run_lightning file 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

echo "******** Verify File checkpoint no-op ********"
run_lightning file
run_sql 'SELECT count(i), sum(i) FROM cpch_tsr.tbl;'
check_contains "count(i): $(($ROW_COUNT*$CHUNK_COUNT))"
check_contains "sum(i): $(( $ROW_COUNT*$CHUNK_COUNT*(($CHUNK_COUNT+2)*$ROW_COUNT + 1)/2 ))"
[ -f "$TEST_DIR/cpch.pb" ]
