This folder contains all tests which relies on external processes such as TiDB.

Unit tests (the `*_test.go` files inside the source directory) should *never* rely on external
programs.

## Preparations

1. The following 6 executables must be copied or linked into these locations:

    * `bin/pd-server`
    * `bin/tikv-server`
    * `bin/tidb-server`
    * `bin/tikv-importer`
    * `bin/tiflash` (needed if environment variable TIFLASH=1 is set)
    * `bin/minio`

    The versions must be ≥2.1.0 as usual.

2. The following programs must be installed:

    * `mysql` (the CLI client)
    * `wget`
    * `openssl`

3. The user executing the tests must have permission to create the folder
    `/tmp/lightning_test_result`. All test artifacts will be written into this folder.

## Running

Run `make test` to execute the unit tests.

Run `make integration_test` to execute the integration tests. This command will

1. Check that all 4 executables exist.
2. Build a `tidb-lightning` executable for collecting code coverage with failpoint support
3. Execute `tests/run.sh`
4. to start cluster with tiflash, please run `TIFLASH=1 tests/run.sh`

If the first two steps are done before, you could also run `tests/run.sh` directly.
This script will

1. Start PD, TiKV, TiDB and TiKV-Importer in background with local storage
2. Find out all `tests/*/run.sh` and run it

Run `tests/run.sh --debug` to pause immediately after all servers are started.

After executing the tests, run `make coverage` to get a coverage report at
`/tmp/lightning_test_result/all_cov.html`.

## Writing new tests

New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
The script should exit with a nonzero error code on failure.

Several convenient commands are provided:

* `run_lightning [CONFIG]` — Starts `tidb-lightning` using `tests/TEST_NAME/CONFIG.toml`
* `run_sql <SQL> <ARGS...>` — Executes an SQL query on the TiDB database
* `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
    (in `-E` format)
* `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
    text (in `-E` format)
