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

for BACKEND in importer tidb local; do
  if [ "$BACKEND" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  # Set the failpoint to kill the lightning instance as soon as one table is imported
  # If checkpoint does work, this should only kill 9 instances of lightnings.
  SLOWDOWN_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/restore/SlowDownImport=sleep(250)'
  export GO_FAILPOINTS="$SLOWDOWN_FAILPOINTS;github.com/pingcap/tidb-lightning/lightning/restore/FailBeforeIndexEngineImported=return"

  # Start importing the tables.
  run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
  run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cppk'
  run_sql 'DROP DATABASE IF EXISTS `tidb_lightning_checkpoint_test_cppk.1357924680.bak`'

  # panic after saving index engine checkpoint status before saving table checkpoint status
  set +e
  for i in $(seq "$TABLE_COUNT"); do
      echo "******** Importing Table Now (step $i/$TABLE_COUNT) ********"
      run_lightning -d "$DBPATH" --backend $BACKEND --enable-checkpoint=1 2> /dev/null
      [ $? -ne 0 ] || exit 1
  done
  set -e

  export GO_FAILPOINTS="$SLOWDOWN_FAILPOINTS"
  set +e
  for i in $(seq "$TABLE_COUNT"); do
      echo "******** Importing Table Now (step $i/$TABLE_COUNT) ********"
      run_lightning -d "$DBPATH" --backend $BACKEND --enable-checkpoint=1 2> /dev/null
  done
  set -e

  # Start importing the tables.
  run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
  run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cppk'
  run_sql 'DROP DATABASE IF EXISTS `tidb_lightning_checkpoint_test_cppk.1357924680.bak`'

  export GO_FAILPOINTS="$SLOWDOWN_FAILPOINTS;github.com/pingcap/tidb-lightning/lightning/SetTaskID=return(1357924680);github.com/pingcap/tidb-lightning/lightning/restore/FailIfIndexEngineImported=return(1)"

  set +e
  for i in $(seq "$TABLE_COUNT"); do
      echo "******** Importing Table Now (step $i/$TABLE_COUNT) ********"
      run_lightning -d "$DBPATH" --backend $BACKEND --enable-checkpoint=1 2> /dev/null
      [ $? -ne 0 ] || exit 1
  done
  set -e

  # After everything is done, there should be no longer new calls to ImportEngine
  # (and thus `kill_lightning_after_one_import` will spare this final check)
  echo "******** Verify checkpoint no-op ********"
  run_lightning -d "$DBPATH" --backend $BACKEND --enable-checkpoint=1
  run_sql "$PARTIAL_IMPORT_QUERY"
  check_contains "s: $(( (1000 * $CHUNK_COUNT + 1001) * $CHUNK_COUNT * $TABLE_COUNT ))"
  run_sql 'SELECT count(*) FROM `tidb_lightning_checkpoint_test_cppk.1357924680.bak`.table_v6 WHERE status >= 200'
  check_contains "count(*): $TABLE_COUNT"

  # Ensure there is no dangling open engines
  ls -lA "$TEST_DIR"/importer/.temp/
  [ -z "$(ls -A "$TEST_DIR"/importer/.temp/)" ]

done
