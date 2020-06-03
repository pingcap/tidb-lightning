#!/bin/sh

# Basic check for correctness when row format v2 is active.
# (There's no way to verify if the rows are really in format v2 though...)

set -eux

run_sql 'SET @@global.tidb_row_format_version = 2;' || { echo 'TiDB does not support changing row format version! skipping test'; exit 0; }

run_sql 'DROP DATABASE IF EXISTS rowformatv2;'

run_lightning

run_sql 'SELECT count(1) FROM rowformatv2.t1;'
check_contains 'count(1): 50'

run_sql 'SELECT DISTINCT col14 FROM rowformatv2.t1;'
check_contains 'col14: NULL'
check_contains 'col14: 39'

run_sql 'SET @@global.tidb_row_format_version = 1;'