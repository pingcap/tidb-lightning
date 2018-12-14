#!/bin/sh

set -eu

# should be OK when the version is normal
run_sql 'DROP DATABASE IF EXISTS checkreq'
run_lightning

# now try to reduce the version to below 2.1.0
API='http://127.0.0.1:2379/pd/api/v1/config/cluster-version'
OLD_VERSION=$(curl "$API")
reset_cluster_version() {
    curl "$API" --data-binary '{"cluster-version":'"$OLD_VERSION"'}'
}
trap reset_cluster_version EXIT

curl "$API" --data-binary '{"cluster-version":"2.0.0"}'

run_sql 'DROP DATABASE IF EXISTS checkreq'
set +e
run_lightning
ERRORCODE=$?
set -e

# ensure lightning will reject the cluster version.
[ "$ERRORCODE" -ne 0 ]
