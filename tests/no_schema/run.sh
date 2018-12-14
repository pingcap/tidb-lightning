#!/bin/sh

set -eu

run_lightning schema_config
run_sql "show databases"
check_not_contains "noschema"

run_sql "create database noschema;"
run_sql "create table noschema.t (x int primary key);"

# Starting importing
run_lightning

run_sql "SELECT sum(x) FROM noschema.t;"
check_contains 'sum(x): 120'
