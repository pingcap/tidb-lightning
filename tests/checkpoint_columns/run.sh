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
DBPATH="$TEST_DIR/cppk.mydump"
TABLE_COUNT=1
CHUNK_COUNT=50

mkdir -p $DBPATH
echo 'CREATE DATABASE cp_tsr;' > "$DBPATH/cp_tsr-schema-create.sql"
PARTIAL_IMPORT_QUERY='SELECT 0'

echo "CREATE TABLE tbl(i TINYINT PRIMARY KEY, j INT);" > "$DBPATH/cp_tsr.tbl-schema.sql"
echo "INSERT INTO tbl (i) VALUES (1),(2);" > "$DBPATH/cp_tsr.tbl.sql"

# Set the failpoint to kill the lightning instance as soon as one row is write
export GO_FAILPOINTS="github.com/pingcap/tidb-lightning/lightning/restore/FailAfterWriteRows=return;github.com/pingcap/tidb-lightning/lightning/restore/SetMinDeliverBytes=return(1);"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cp_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test'

set +e
run_lightning -d "$DBPATH" --backend tidb --enable-checkpoint=1 2> /dev/null
set -e
run_sql 'SELECT count(*) FROM `cp_tsr`.tbl'
check_contains "count(*): 1"

set +e
run_lightning -d "$DBPATH" --backend $BACKEND --enable-checkpoint=1 2> /dev/null
set -e

run_sql 'SELECT count(*) FROM `cp_tsr`.tbl'
check_contains "count(*): 2"
