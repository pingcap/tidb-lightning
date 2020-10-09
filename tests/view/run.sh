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

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/fr.mydump"

echo 'CREATE DATABASE db1;' > "$DBPATH/db1-schema-create.sql"
echo 'CREATE DATABASE db0;' > "$DBPATH/db0-schema-create.sql"
echo "CREATE TABLE tbl(i TINYINT PRIMARY KEY, j INT, s VARCHAR(16));" > "$DBPATH/db1.tbl-schema.sql"
echo "INSERT INTO tbl (i, j, s) VALUES (1, 1, 'test1'),(2, 2, 'test2'), (3, 3, 'test3');" > "$DBPATH/db1.tbl.0.sql"

# view schema
echo "CREATE TABLE v1(i TINYINT, s VARCHAR(16));" > "$DBPATH/db1.v1-schema.sql"
cat > "$DBPATH/db1.v1-schema-view.sql" << '_EOF_'
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS `v1`;
DROP VIEW IF EXISTS `v1`;
SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
SET character_set_client = utf8;
SET character_set_results = utf8;
SET collation_connection = utf8_general_ci;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`192.168.198.178` SQL SECURITY DEFINER VIEW `v1` (`i`, `s`) AS SELECT `i`,`s` FROM `db1`.`tbl`;
SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
SET collation_connection = @PREV_COLLATION_CONNECTION;
_EOF_

# view schema
echo "CREATE TABLE v2(s VARCHAR(16));" > "$DBPATH/db0.v2-schema.sql"
cat > "$DBPATH/db0.v2-schema-view.sql" << '_EOF_'
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS `v2`;
DROP VIEW IF EXISTS `v2`;
SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
SET character_set_client = utf8;
SET character_set_results = utf8;
SET collation_connection = utf8_general_ci;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`192.168.198.178` SQL SECURITY DEFINER VIEW `v2` (`s`) AS SELECT `s` FROM `db1`.`v1` WHERE `i`<2;
SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
SET collation_connection = @PREV_COLLATION_CONNECTION;
_EOF_

for BACKEND in local importer tidb; do
  if [ "$BACKEND" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  run_sql 'DROP DATABASE IF EXISTS db0'
  run_sql 'DROP DATABASE IF EXISTS db1'

  # Start importing the tables.
  run_lightning -d "$DBPATH" --backend $BACKEND 2> /dev/null

  run_sql 'SELECT count(*), sum(i) FROM `db1`.v1'
  check_contains "count(*): 3"
  check_contains "sum(i): 6"

  run_sql 'SELECT count(*) FROM `db0`.v2'
  check_contains "count(*): 1"
  run_sql 'SELECT s FROM `db0`.v2'
  check_contains "s: test1"
done
