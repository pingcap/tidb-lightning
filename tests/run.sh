#!/bin/sh

set -eu

TEST_DIR=/tmp/lightning_test_result

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true
    killall -9 tikv-importer || true

    find "$TEST_DIR" -d -mindepth 1 -not -name 'cov.*' -delete || true
}

start_services() {
    stop_services

    mkdir -p "$TEST_DIR"

    echo "Starting PD..."
    bin/pd-server \
        --client-urls http://127.0.0.1:2379 \
        --log-file "$TEST_DIR/pd.log" \
        --data-dir "$TEST_DIR/pd" &
    # wait until PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$TEST_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
EOF

    echo "Starting TiKV..."
    bin/tikv-server \
        --pd 127.0.0.1:2379 \
        -A 127.0.0.1:20160 \
        --log-file "$TEST_DIR/tikv.log" \
        -C "$TEST_DIR/tikv-config.toml" \
        -s "$TEST_DIR/tikv" &
    sleep 1

    echo "Starting TiDB..."
    bin/tidb-server \
        -P 4000 \
        --store tikv \
        --path 127.0.0.1:2379 \
        --log-file "$TEST_DIR/tidb.log" &

    echo "Starting Importer..."
    bin/tikv-importer \
        -A 127.0.0.1:8808 \
        --log-file "$TEST_DIR/importer.log" \
        --import-dir "$TEST_DIR/importer" &

    echo "Verifying TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P4000 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done
}

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

for script in tests/*/run.sh; do
    echo "Running test $script..."
    TEST_DIR="$TEST_DIR" \
    PATH="tests/_utils:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    sh "$script"
done


