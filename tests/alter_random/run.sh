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

set -eu

for backend in tidb importer local; do
    if [ "$backend" = 'local' -a "$CLUSTER_VERSION_MAJOR" -lt 4 ]; then
        continue
    fi

    run_sql 'DROP DATABASE IF EXISTS alter_random;'
    run_lightning --backend $backend

    run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM alter_random.t"
    check_contains 'inc: 1'
    check_contains 'inc: 2'
    check_contains 'inc: 3'

    # auto random base is 4
    run_sql "INSERT INTO alter_random.t VALUES ();"
    run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM alter_random.t"
    check_contains 'inc: 4'
done
