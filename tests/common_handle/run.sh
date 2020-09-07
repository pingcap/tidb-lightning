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

check_cluster_version 4 0 0 'local backend' || exit 0

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/ch.mydump"

mkdir -p $DBPATH
echo 'CREATE DATABASE ch;' > "$DBPATH/ch-schema-create.sql"
echo "CREATE TABLE t(s varchar(32), i INT, j TINYINT,  PRIMARY KEY(s, i));" > "$DBPATH/ch.t-schema.sql"
cat > "$DBPATH/ch.t.0.sql" << _EOF_
INSERT INTO t (s, i, j) VALUES
  ("this_is_test1", 1, 1),
  ("this_is_test2", 2, 2),
  ("this_is_test3", 3, 3),
  ("this_is_test4", 4, 4),
  ("this_is_test5", 5, 5);
_EOF_

# Set minDeliverBytes to a small enough number to only write only 1 row each time
# Set the failpoint to kill the lightning instance as soon as one row is written

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS ch'
# enable cluster index
run_sql 'set @@global.tidb_enable_clustered_index = 1' || echo "tidb does not support cluster index yet, skipped!"
# wait for global variable cache invalid
sleep 2

set +e
run_lightning -d "$DBPATH" --backend local 2> /dev/null
set -e
run_sql 'SELECT count(*), sum(i) FROM `ch`.t'
check_contains "count(*): 5"
check_contains "sum(i): 15"

# restore global variables, other tests needs this to handle the _tidb_row_id column
run_sql 'set @@global.tidb_enable_clustered_index = 0' || echo ""
# wait for global variable cache invalid
sleep 2
