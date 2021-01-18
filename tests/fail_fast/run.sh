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

export GO_FAILPOINTS='github.com/pingcap/tidb-lightning/lightning/restore/SlowDownWriteRows=return(50);github.com/pingcap/tidb-lightning/lightning/restore/SetMinDeliverBytes=return(1)'

for CFG in chunk engine; do
  rm -f "$TEST_DIR/lightning-tidb.log"
  run_sql 'DROP DATABASE IF EXISTS fail_fast;'

  set +e
  run_lightning --backend tidb --enable-checkpoint=0 --log-file "$TEST_DIR/lightning-tidb.log" --config "tests/$TEST_NAME/$CFG.toml"
  ERRORCODE=$?
  set -e

  cat $TEST_DIR/lightning-tidb.log > $TEST_DIR/lightning.log

  [ "$ERRORCODE" -ne 0 ]

  tail -n 1 $TEST_DIR/lightning-tidb.log | grep -Fq "Error 1062: Duplicate entry '1-1' for key 'uq'"
  ! grep -Fq "restore file completed" $TEST_DIR/lightning-tidb.log
  ! grep -Fq "restore engine completed" $TEST_DIR/lightning-tidb.log
done
