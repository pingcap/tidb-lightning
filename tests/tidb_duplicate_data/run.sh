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

set -eu

run_sql 'DROP DATABASE IF EXISTS dup;'
run_lightning 'replace'
run_sql 'SELECT count(*) FROM dup.dup'
check_contains 'count(*): 1'
run_sql 'SELECT d FROM dup.dup'
check_contains 'd: 200'

run_sql 'DROP DATABASE IF EXISTS dup;'
run_lightning 'ignore'
run_sql 'SELECT count(*) FROM dup.dup'
check_contains 'count(*): 1'
run_sql 'SELECT d FROM dup.dup'
check_contains 'd: 100'

run_sql 'DROP DATABASE IF EXISTS dup;'
set +e
run_lightning 'error'
ERRORCODE=$?
set -e
[ "$ERRORCODE" -ne 0 ]

tail -20 "$TEST_DIR/lightning-error-on-dup.log" > "$TEST_DIR/lightning-error-on-dup.tail"
grep -Fq 'Duplicate entry' "$TEST_DIR/lightning-error-on-dup.tail"