#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

TEST_DIR=/tmp/lightning_test_result
export PATH="tests/_utils:$PATH"

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true
    killall -9 tikv-importer || true

    find "$TEST_DIR" -maxdepth 1 -not -path "$TEST_DIR" -not -name "cov.*" -not -name "*.log" | xargs rm -r || true
}

start_services() {
    stop_services

    TT="$TEST_DIR/tls"
    mkdir -p "$TT"
    rm -f "$TEST_DIR"/*.log

    # Ref: https://docs.microsoft.com/en-us/azure/application-gateway/self-signed-certificates
    # gRPC only supports P-256 curves, see https://github.com/grpc/grpc/issues/6722
    echo "Generate TLS keys..."
    cat - > "$TT/ipsan.cnf" <<EOF
[dn]
CN = localhost
[req]
distinguished_name = dn
[EXT]
subjectAltName = @alt_names
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth,serverAuth
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl ecparam -out "$TT/ca.key" -name prime256v1 -genkey
    openssl req -new -batch -sha256 -subj '/CN=localhost' -key "$TT/ca.key" -out "$TT/ca.csr"
    openssl x509 -req -sha256 -days 2 -in "$TT/ca.csr" -signkey "$TT/ca.key" -out "$TT/ca.pem" 2> /dev/null
    for cluster in tidb pd tikv importer lightning curl; do
        openssl ecparam -out "$TT/$cluster.key" -name prime256v1 -genkey
        openssl req -new -batch -sha256 -subj '/CN=localhost' -key "$TT/$cluster.key" -out "$TT/$cluster.csr"
        openssl x509 -req -sha256 -days 1 -extensions EXT -extfile "$TT/ipsan.cnf" -in "$TT/$cluster.csr" -CA "$TT/ca.pem" -CAkey "$TT/ca.key" -CAcreateserial -out "$TT/$cluster.pem" 2> /dev/null
    done

    cat - > "$TEST_DIR/pd-config.toml" <<EOF
data-dir = "$TEST_DIR/pd"
client-urls = "https://127.0.0.1:2379"
peer-urls = "https://127.0.0.1:2380"
[log.file]
filename = "$TEST_DIR/pd.log"
[security]
cacert-path = "$TT/ca.pem"
cert-path = "$TT/pd.pem"
key-path = "$TT/pd.key"
EOF
    echo "Starting PD..."
    bin/pd-server --config "$TEST_DIR/pd-config.toml" &
    # wait until PD is online...
    i=0
    while ! run_curl https://127.0.0.1:2379/pd/api/v1/version; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start PD'
            exit 1
        fi
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$TEST_DIR/tikv-config.toml" <<EOF
log-file = "$TEST_DIR/tikv.log"
[server]
addr = "127.0.0.1:20160"
[storage]
data-dir = "$TEST_DIR/tikv"
[pd]
endpoints = ["https://127.0.0.1:2379"]
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[security]
ca-path = "$TT/ca.pem"
cert-path = "$TT/tikv.pem"
key-path = "$TT/tikv.key"
EOF
    echo "Starting TiKV..."
    bin/tikv-server -C "$TEST_DIR/tikv-config.toml" &
    sleep 1

    cat - > "$TEST_DIR/tidb-config.toml" <<EOF
host = "127.0.0.1"
port = 4000
store = "tikv"
path = "127.0.0.1:2379"
[status]
status-host = "127.0.0.1"
status-port = 10080
[log.file]
filename = "$TEST_DIR/tidb.log"
[security]
ssl-ca = "$TT/ca.pem"
ssl-cert = "$TT/tidb.pem"
ssl-key = "$TT/tidb.key"
cluster-ssl-ca = "$TT/ca.pem"
cluster-ssl-cert = "$TT/tidb.pem"
cluster-ssl-key = "$TT/tidb.key"
[experimental]
allow-auto-random = true
EOF
    echo "Starting TiDB..."
    bin/tidb-server --config "$TEST_DIR/tidb-config.toml" &

    cat - > "$TEST_DIR/importer-config.toml" <<EOF
log-file = "$TEST_DIR/importer.log"
[server]
addr = "127.0.0.1:8808"
[import]
import-dir = "$TEST_DIR/importer"
[security]
ca-path = "$TT/ca.pem"
cert-path = "$TT/importer.pem"
key-path = "$TT/importer.key"
EOF
    echo "Starting Importer..."
    bin/tikv-importer --config "$TEST_DIR/importer-config.toml" &

    echo "Verifying TiDB is started..."
    i=0
    export TEST_NAME=waiting_for_tidb
    while ! run_sql 'select * from mysql.tidb;'; do
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
    echo "\x1b[32;1m@@@@@@@ Running test $script...\x1b[0m"
    TEST_DIR="$TEST_DIR" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    sh "$script"
done
