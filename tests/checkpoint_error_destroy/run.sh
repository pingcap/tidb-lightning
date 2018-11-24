#!/bin/sh

set -eu

# Make sure we won't run out of table concurrency by destroying checkpoints

for i in $(seq 8); do
    set +e
    run_lightning bad
    set -e
    bin/tidb-lightning-ctl -checkpoint-error-destroy=all
done

run_lightning good
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'
