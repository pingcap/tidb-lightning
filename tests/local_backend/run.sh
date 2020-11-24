#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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

check_cluster_version 4 0 0 'local backend' || exit 0

ENGINE_COUNT=6

# First, verify that inject with not leader error is fine.
rm -f "$TEST_DIR/lightning-local.log"
rm -f "/tmp/tidb_lightning_checkpoint.pb"
run_sql 'DROP DATABASE IF EXISTS cpeng;'
export GO_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/backend/local/FailIngestMeta=return("notleader")'

run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/config.toml"

# Check that everything is correctly imported
run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Now, verify it works with epoch not match as well.
run_sql 'DROP DATABASE cpeng;'
rm -f "/tmp/tidb_lightning_checkpoint.pb"

export GO_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/backend/local/FailIngestMeta=return("epochnotmatch")'

run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/config.toml"

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'


# Now, verify it works with checkpoints as well.
run_sql 'DROP DATABASE cpeng;'
rm -f "/tmp/tidb_lightning_checkpoint.pb"

set +e
export GO_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/restore/FailBeforeDataEngineImported=return'
for i in $(seq "$ENGINE_COUNT"); do
    echo "******** Importing Table Now (step $i/$ENGINE_COUNT) ********"
    run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/config.toml"
    [ $? -ne 0 ] || exit 1
done
set -e

export GO_FAILPOINTS=''
echo "******** Verify checkpoint no-op ********"
run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/config.toml"

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Verify GetLocalStoringTables works
# failpoint works for per table not task, so we limit this test to task that allow one table
for ckpt in mysql file; do
  run_sql 'DROP DATABASE IF EXISTS cpeng;'
  rm -f "/tmp/tidb_lightning_checkpoint.pb"
  
  # before chunk pos is updated, local files could handle lost
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb-lightning/lightning/restore/FailAfterWriteRows=return"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/$ckpt.toml"
  set -e
  bin/tidb-lightning-ctl.test DEVEL --check-local-storing=all \
    --config=tests/$TEST_NAME/$ckpt.toml \
    --tidb-port 4000 \
    --pd-urls '127.0.0.1:2379' 2>&1 >/dev/null | grep -Fq "Those tables should have intermediate files: []"
  
  # when position of chunk file doesn't equal to offset, intermediate file should exist
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb-lightning/lightning/restore/LocalBackendSaveCheckpoint=return;github.com/pingcap/tidb-lightning/lightning/restore/FailIfImportedChunk=return(1)"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/$ckpt.toml"
  set -e
  bin/tidb-lightning-ctl.test DEVEL --check-local-storing=all \
    --config=tests/$TEST_NAME/$ckpt.toml \
    --tidb-port 4000 \
    --pd-urls '127.0.0.1:2379' 2>&1 >/dev/null | grep -qE "Those tables should have intermediate files: \[.+\]"
  # don't distinguish whole sort-kv directory missing and table's directory missing for now
  ls -lA $TEST_DIR/sorted
  
  # after index engine is imported, local file could handle lost
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb-lightning/lightning/restore/FailIfIndexEngineImported=return(1)"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "tests/$TEST_NAME/$ckpt.toml"
  set -e
  bin/tidb-lightning-ctl.test DEVEL --check-local-storing=all \
    --config=tests/$TEST_NAME/$ckpt.toml \
    --tidb-port 4000 \
    --pd-urls '127.0.0.1:2379' 2>&1 >/dev/null | grep -Fq "Those tables should have intermediate files: []"
done