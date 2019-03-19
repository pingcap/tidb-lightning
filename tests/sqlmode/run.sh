#!/bin/sh

run_sql 'DROP DATABASE IF EXISTS sqlmodedb'

run_lightning off

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 1'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 127'
check_contains 'hex(c): 74'
check_contains 'd: '

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 2'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: -128'
check_contains 'hex(c): F0'
check_contains 'd: x,y'

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 3'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 0'
check_contains 'hex(c): 99'
check_contains 'd: '

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 4'
check_contains 'a: 2000-01-01 00:00:00'
check_contains 'b: 100'
check_contains 'hex(c): '
check_contains 'd: x,y'

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 5'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 0'
check_contains 'hex(c): '
check_contains 'd: '

run_sql 'DROP DATABASE IF EXISTS sqlmodedb'

set +e
run_lightning on
[ $? -ne 0 ] || exit 1
set -e
