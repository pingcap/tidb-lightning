#!/bin/sh

# This test verifies if TOOL-200 and TOOL-241 are all fixed.
# They all involve data source with lots of empty tables.

set -eu

run_sql 'DROP DATABASE IF EXISTS qyjc;'
run_lightning
echo 'Import finished'

# Verify all data are imported
for table_name in \
    q_alarm_group \
    q_alarm_message_log \
    q_alarm_receiver \
    q_config \
    q_report_circular_data \
    q_report_desc \
    q_report_summary \
    q_system_update \
    q_user_log
do
    run_sql "SELECT count(*) FROM qyjc.$table_name;"
    check_contains 'count(*): 0'
done

# ensure the non-empty table is not affected
run_sql 'SELECT count(id), min(id), max(id) FROM qyjc.q_fish_event;'
check_contains 'count(id): 84'
check_contains 'min(id): 8343146'
check_contains 'max(id): 8343229'
