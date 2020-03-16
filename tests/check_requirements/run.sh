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

set -eux

curl_cluster_version() {
    run_curl 'https://127.0.0.1:2379/pd/api/v1/config/cluster-version' "$@"
}

# should be OK when the version is normal
run_sql 'DROP DATABASE IF EXISTS checkreq'
run_lightning --check-requirements=1 -L warning

# now try to reduce the version to below 2.1.0
OLD_VERSION=$(curl_cluster_version)
reset_cluster_version() {
    curl_cluster_version '{"cluster-version":'"$OLD_VERSION"'}'
}
trap reset_cluster_version EXIT

curl_cluster_version '{"cluster-version":"2.0.0-fake.and.error.expected"}'
sleep 1

run_sql 'DROP DATABASE IF EXISTS checkreq'
set +e
run_lightning --check-requirements=1 -L warning
ERRORCODE=$?
set -e

# ensure lightning will reject the cluster version.
[ "$ERRORCODE" -ne 0 ]
