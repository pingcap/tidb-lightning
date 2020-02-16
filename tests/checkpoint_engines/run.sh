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

set -eux

do_run_lightning() {
    run_lightning --enable-checkpoint=1 --config "tests/$TEST_NAME/$1.toml"
}

# First, verify that a normal operation is fine.

rm -f "$TEST_DIR/lightning-checkpoint-engines.log"
rm -f "/tmp/tidb_lightning_checkpoint.pb"
run_sql 'DROP DATABASE IF EXISTS cpeng;'

do_run_lightning config

# Check that we have indeed opened 6 engines (index + data engine)
DATA_ENGINE_COUNT=4
INDEX_ENGINE_COUNT=2
ENGINE_COUNT=6
OPEN_ENGINES_COUNT=$(grep 'open engine' "$TEST_DIR/lightning-checkpoint-engines.log" | wc -l)
echo "Number of open engines: $OPEN_ENGINES_COUNT"
[ "$OPEN_ENGINES_COUNT" -eq $ENGINE_COUNT ]

# Check that everything is correctly imported
run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Now, verify it works with checkpoints as well.

run_sql 'DROP DATABASE cpeng;'
rm -f "/tmp/tidb_lightning_checkpoint.pb"

# Data engine part
export GO_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/restore/SlowDownImport=sleep(500);github.com/pingcap/tidb-lightning/lightning/restore/FailIfStatusBecomes=return(120);github.com/pingcap/tidb-lightning/lightning/restore/FailIfIndexEngineImported=return(140)'
set +e
for i in $(seq "$ENGINE_COUNT"); do
    echo "******** Importing Table Now (step $i/$ENGINE_COUNT) ********"
    do_run_lightning config 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

echo "******** Verify checkpoint no-op ********"
do_run_lightning config

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Now, try again with MySQL checkpoints

run_sql 'DROP DATABASE cpeng;'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint;'

set +e
for i in $(seq "$ENGINE_COUNT"); do
    echo "******** Importing Table Now (step $i/$ENGINE_COUNT) ********"
    do_run_lightning mysql 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

echo "******** Verify checkpoint no-op ********"
do_run_lightning mysql

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'