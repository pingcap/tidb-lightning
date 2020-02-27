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

# Check if black-white-list works.

set -eux

run_sql 'DROP DATABASE IF EXISTS firstdb;'
run_sql 'DROP DATABASE IF EXISTS seconddb;'
run_lightning --config "tests/$TEST_NAME/firstdb-only.toml"
run_sql 'SHOW DATABASES;'
check_contains 'Database: firstdb'
check_not_contains 'Database: seconddb'
run_sql 'SHOW TABLES IN firstdb;'
check_contains 'Tables_in_firstdb: first'
check_contains 'Tables_in_firstdb: second'
run_sql 'SHOW TABLES IN mysql;'
check_not_contains 'Tables_in_mysql: testtable'

run_sql 'DROP DATABASE IF EXISTS firstdb;'
run_sql 'DROP DATABASE IF EXISTS seconddb;'
run_lightning --config "tests/$TEST_NAME/even-table-only.toml"
run_sql 'SHOW DATABASES;'
check_contains 'Database: firstdb'
check_contains 'Database: seconddb'
run_sql 'SHOW TABLES IN firstdb;'
check_not_contains 'Tables_in_firstdb: first'
check_contains 'Tables_in_firstdb: second'
run_sql 'SHOW TABLES IN seconddb;'
check_not_contains 'Tables_in_seconddb: third'
check_contains 'Tables_in_seconddb: fourth'
run_sql 'SHOW TABLES IN mysql;'
check_not_contains 'Tables_in_mysql: testtable'
