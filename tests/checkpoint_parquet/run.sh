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
DBPATH="$TEST_DIR/cppq.mydump"
ROW_COUNT=100

do_run_lightning() {
    run_lightning -d "$DBPATH" --enable-checkpoint=1
}

mkdir -p $DBPATH
echo 'CREATE DATABASE cppq_tsr;' > "$DBPATH/cppq_tsr-schema-create.sql"
echo 'CREATE TABLE tbl(i INT, s VARCHAR(16));' > "$DBPATH/cppq_tsr.tbl-schema.sql"
bin/parquet_gen --dir $DBPATH --schema cppq_tsr --table tbl --chunk 1 --rows $ROW_COUNT

# Set the failpoint to kill the lightning instance as soon as one batch data is written
export GO_FAILPOINTS="github.com/pingcap/tidb-lightning/lightning/restore/FailAfterWriteRows=return;github.com/pingcap/tidb-lightning/lightning/restore/SetMinDeliverBytes=return(1)"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cppq_tsr'
run_sql 'DROP DATABASE IF EXISTS checkpoint_test_parquet'

set +e
run_lightning -d "$DBPATH" --backend tidb --enable-checkpoint=1 2> /dev/null
set -e
run_sql 'SELECT count(*), sum(i) FROM `cppq_tsr`.tbl'
check_contains "count(*): 32"
# sum(0..31)
check_contains "sum(i): 496"

# check chunk offset and update checkpoint current row id to a higher value so that
# if parse read from start, the generated rows will be different
run_sql "UPDATE checkpoint_test_parquet.chunk_v5 SET prev_rowid_max = prev_rowid_max + 1000, rowid_max = rowid_max + 1000;"

# restart lightning from checkpoint, the second line should be written successfully
export GO_FAILPOINTS=
set +e
run_lightning -d "$DBPATH" --backend tidb --enable-checkpoint=1 2> /dev/null
set -e

run_sql 'SELECT count(*), sum(i) FROM `cppq_tsr`.tbl'
check_contains "count(*): 100"
# sum(0..99)
check_contains "sum(i): 4950"
