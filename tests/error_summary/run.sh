#!/bin/sh

set -eu

# Check that error summary are written at the bottom of import.

# The easiest way to induce error is to prepopulate the target table with conflicting content.
run_sql 'CREATE DATABASE IF NOT EXISTS error_summary;'
run_sql 'DROP TABLE IF EXISTS error_summary.a;'
run_sql 'DROP TABLE IF EXISTS error_summary.c;'
run_sql 'CREATE TABLE error_summary.a (id INT NOT NULL PRIMARY KEY, k INT NOT NULL);'
run_sql 'CREATE TABLE error_summary.c (id INT NOT NULL PRIMARY KEY, k INT NOT NULL);'
run_sql 'INSERT INTO error_summary.a VALUES (2, 4), (6, 8);'
run_sql 'INSERT INTO error_summary.c VALUES (3, 9), (27, 81);'

set +e
run_lightning
set -e

# Verify that table `b` is indeed imported
run_sql 'SELECT sum(id), sum(k) FROM error_summary.b'
check_contains 'sum(id): 28'
check_contains 'sum(k): 32'

# Verify the log contains the expected messages at the last few lines
tail -10 "$TEST_DIR/lightning-error-summary.log" > "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '[error] Totally **2** tables failed to be imported.' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '[`error_summary`.`a`] [checksum] checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '[`error_summary`.`c`] [checksum] checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"
! grep -Fq '[`error_summary`.`b`] [checksum] checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"
