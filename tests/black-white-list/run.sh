#!/bin/sh

# Check if black-white-list works.

set -eux

run_sql 'DROP DATABASE IF EXISTS firstdb;'
run_sql 'DROP DATABASE IF EXISTS seconddb;'
run_lightning 'firstdb-only'
run_sql 'SHOW DATABASES;'
check_contains 'Database: firstdb'
check_not_contains 'Database: seconddb'
run_sql 'SHOW TABLES IN firstdb;'
check_contains 'Tables_in_firstdb: first'
check_contains 'Tables_in_firstdb: second'

run_sql 'DROP DATABASE IF EXISTS firstdb;'
run_sql 'DROP DATABASE IF EXISTS seconddb;'
run_lightning 'even-table-only'
run_sql 'SHOW DATABASES;'
check_contains 'Database: firstdb'
check_contains 'Database: seconddb'
run_sql 'SHOW TABLES IN firstdb;'
check_not_contains 'Tables_in_firstdb: first'
check_contains 'Tables_in_firstdb: second'
run_sql 'SHOW TABLES IN seconddb;'
check_not_contains 'Tables_in_seconddb: third'
check_contains 'Tables_in_seconddb: fourth'
