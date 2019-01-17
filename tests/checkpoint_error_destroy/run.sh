#!/bin/sh

set -eu

# Make sure we won't run out of table concurrency by destroying checkpoints

for i in $(seq 8); do
    set +e
    run_lightning bad
    set -e
    run_lightning_ctl bad -checkpoint-error-destroy=all
done

run_lightning good
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'

# Try again with the file checkpoints

run_sql 'DROP DATABASE cped'

for i in $(seq 8); do
    set +e
    run_lightning bad_file
    set -e
    ls -la /tmp/lightning_test_result/importer/.temp/
    run_lightning_ctl bad_file -checkpoint-error-destroy=all
    ls -la /tmp/lightning_test_result/importer/.temp/
done

run_lightning good_file
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'
