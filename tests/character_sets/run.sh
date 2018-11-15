#!/bin/sh

set -eux

run_lightning_expecting_fail() {
    set +e
    run_lightning "$1"
    ERRCODE=$?
    set -e
    [ "$ERRCODE" != 0 ]
}

run_sql 'DROP DATABASE IF EXISTS charsets;'

# gb18030

run_lightning 'gb18030-auto'
run_sql 'SELECT sum(`主键`) AS s FROM charsets.gb18030'
check_contains 's: 267'
run_sql 'DROP TABLE charsets.gb18030;'

run_lightning 'gb18030-gb18030'
run_sql 'SELECT sum(`主键`) AS s FROM charsets.gb18030'
check_contains 's: 267'
run_sql 'DROP TABLE charsets.gb18030;'

run_lightning_expecting_fail 'gb18030-utf8mb4'

run_lightning 'gb18030-binary'
run_sql 'SELECT sum(`Ö÷¼ü`) AS s FROM charsets.gb18030'
check_contains 's: 267'

# utf8mb4

run_lightning 'utf8mb4-auto'
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning 'utf8mb4-gb18030'
run_sql 'SELECT sum(`涓婚敭`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning 'utf8mb4-utf8mb4'
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning 'utf8mb4-utf8mb4'
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'

# mixed

run_lightning_expecting_fail 'mixed-auto'
run_lightning_expecting_fail 'mixed-gb18030'
run_lightning_expecting_fail 'mixed-utf8mb4'

run_lightning 'mixed-binary'
run_sql 'SELECT sum(`唯一键`) AS s FROM charsets.mixed'
check_contains 's: 5291'

