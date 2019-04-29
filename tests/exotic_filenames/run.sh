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

# Confirm the behavior for some exotic filenames
# Do not enable until https://github.com/pingcap/tidb/pull/8302 is merged.

set -eu

run_sql 'DROP DATABASE IF EXISTS `x``f"n`;'
run_sql 'DROP DATABASE IF EXISTS `中文庫🥳`;'
run_lightning
echo 'Import finished'

run_sql 'SELECT count(*) FROM `x``f"n`.`exotic``table````name`'
check_contains 'count(*): 5'
run_sql 'INSERT INTO `x``f"n`.`exotic``table````name` (a) VALUES ("ffffff"), ("gggggg")'
run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM `x``f"n`.`exotic``table````name` WHERE a = "ffffff"'
check_contains '_tidb_rowid > 80000: 1'
check_contains 'b > 80000: 1'
run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM `x``f"n`.`exotic``table````name` WHERE a = "gggggg"'
check_contains '_tidb_rowid > 80000: 1'
check_contains 'b > 80000: 1'

run_sql 'SELECT * FROM `中文庫🥳`.中文表'
check_contains 'a: 2345'
