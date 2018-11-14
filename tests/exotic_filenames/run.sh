#!/bin/sh

# Confirm the behavior for some exotic filenames
# Do not enable until https://github.com/pingcap/tidb/pull/8302 is merged.

exit 0

set -eu

run_sql 'DROP DATABASE IF EXISTS `x``f"n`;'
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
