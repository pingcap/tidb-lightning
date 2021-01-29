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

# enable start tiflash
export TIFLASH="$TIFLASH"
export NEW_COLLATION="$NEW_COLLATION"
set -eu
export TEST_DIR=/tmp/lightning_test_result
SELECTED_TEST_NAME="${TEST_NAME-$(find tests -mindepth 2 -maxdepth 2 -name run.sh | cut -d/ -f2 | sort)}"
export PATH="tests/_utils:$PATH"
. tests/_utils/run_services

trap stop_services EXIT
start_services

# Intermediate file needed because `read` can be used as a pipe target.
# https://stackoverflow.com/q/2746553/
run_curl 'https://127.0.0.1:2379/pd/api/v1/version' | grep -o 'v[0-9.]\+' > "$TEST_DIR/cluster_version.txt"
IFS='.' read CLUSTER_VERSION_MAJOR CLUSTER_VERSION_MINOR CLUSTER_VERSION_REVISION < "$TEST_DIR/cluster_version.txt"

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

echo "selected test cases: $SELECTED_TEST_NAME"

# disable cluster index by default
run_sql 'set @@global.tidb_enable_clustered_index = 0' || echo "tidb does not support cluster index yet, skipped!"
# wait for global variable cache invalid
sleep 2

for casename in $SELECTED_TEST_NAME; do
    script=tests/$casename/run.sh
    echo "\x1b[32;1m@@@@@@@ Running test $script...\x1b[0m"
    TEST_DIR="$TEST_DIR" \
    TEST_NAME="$casename" \
    CLUSTER_VERSION_MAJOR="${CLUSTER_VERSION_MAJOR#v}" \
    CLUSTER_VERSION_MINOR="$CLUSTER_VERSION_MINOR" \
    CLUSTER_VERSION_REVISION="$CLUSTER_VERSION_REVISION" \
    sh "$script"
done
