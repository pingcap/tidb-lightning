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

# Make sure we won't run out of table concurrency by destroying checkpoints

for i in $(seq 8); do
    set +e
    run_lightning bad
    set -e
    run_lightning_ctl bad -checkpoint-error-destroy=all
done

run_lightning good
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'

# Try again with the file checkpoints

run_sql 'DROP DATABASE cped'

for i in $(seq 8); do
    set +e
    run_lightning bad_file
    set -e
    ls -la /tmp/lightning_test_result/importer/.temp/
    run_lightning_ctl bad_file -checkpoint-error-destroy=all
    ls -la /tmp/lightning_test_result/importer/.temp/
done

run_lightning good_file
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'
