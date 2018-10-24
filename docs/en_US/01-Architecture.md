Architecture
============

![Architecture of TiDB Lightning tool set](./tidb-lightning.svg)

The TiDB Lightning tool set consists of two components:

- **`tidb-lightning`** (the "front end") reads the SQL dump and import the database structure
    into TiDB cluster, and also transforms the data into Key-Value (KV) pairs to `tikv-importer`.

- **`tikv-importer`** (the "back end") combines and sorts the KV pairs and then
    import these sorted pairs as a whole into the TiKV cluster.

The complete import process is like this:

1. Before importing, `tidb-lightning` will switch the TiKV cluster to "import mode", which optimizes
    the cluster for writing and disables automatic compaction.

2. `tidb-lightning` will create the skeleton of all tables from the data source.

3. For each table, `tidb-lightning` will inform `tikv-importer` via gRPC to create an *engine file*
    to store KV pairs. `tidb-lightning` will then read the SQL dump in parallel, transforms the data
    into KV pairs according to the TiDB rules, and send them to `tikv-importer`'s engine files.

4. Once a full table of KV pairs are received, `tikv-importer` will divide and schedule these data
    and imports them into the target TiKV cluster.

5. `tidb-lightning` will then perform a checksum comparison between the local data source and
    those calculated from the cluster, to ensure there is no data corruption in the process.

6. After all tables are imported, `tidb-lightning` will perform a global compaction on the TiKV
    cluster, and tell TiDB to `ANALYZE` all imported tables, to prepare for optimal query planning.

7. Finally, `tidb-lightning` will switch the TiKV cluster back to "normal mode" so the cluster
    would resume normal services.

