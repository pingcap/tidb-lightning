# TiDB Lightning User Guide

TiDB Lightning is a data import tool which is used to fast import a large amount of data to the TiDB cluster. Currently, it only supports source data in the Mydumper file format and in the future it will support more formats like CSV.

Now TiDB Lightning only supports full import of new tables. During the importing process, the cluster stops the service; as a result, TiDB Lightning is not suitable for importing data online.

## TiDB Lightning architecture

The following diagram shows the architecture of TiDB Lightning: 

![](./media/tidb-lightning-architecture.png)

TiDB Lightning has two components:

- `tidb-lightning`
    
    The front-end part of TiDB Lightning. It transforms the source data into Key-Value (KV) pairs and writes the data into `tikv-importer`.
- `tikv-importer`
    
    The back-end part of TiDB Lightning. It caches, sorts, and splits the KV pairs written by `tidb-lightning` and imports the KV pairs to the TiKV cluster.

## TiDB Lightning workflow

1. Before importing data, `tidb-lightning` automatically switches the TiKV mode to the import mode via API.
2. `tidb-lightning` obtains data from the data source, transforms the source data into KV data, and then writes the data into `tikv-importer`.
3. When the data written by `tidb-lightning` reaches a specific size, `tidb-lightning` sends the `Import` command to `tikv-importer`.
4. `tikv-importer` splits and schedules the TiKV data of the target cluster and then imports the data to the TiKV cluster.
5. `tidb-lightning` transforms and imports the source data continuously until it finishes importing the data in the source data directory.
6. `tidb-lightning` performs the `Compact`, `Checksum`, and `Analyze` operation on tables in the target cluster.
7. `tidb-lightning` automatically switches the TiKV mode to the normal mode. Then the TiDB cluster can provide services.

## Deploy process

### Hardware requirements

#### Hardware requirements for separate deployment

The following are the hardware requirements for deploying one set of TiDB Lighting. If you have enough machines, you can deploy multiple sets of `tidb-lightning` and `tikv-importer`, split the source code based on the table grain and then import the data concurrently.

- tidb-lightning
  
   - CPU bound, with over 32 logical cores
   - 16 GB+ memory
   - 1 TB+ SSD
   - 10 Gigabit network card
   - Use standalone deployment because CPU will be fully occupied by default in the operation process. In the limited condition, you can deploy it along with another component (like `tidb-server`) on one machine and configure to limit the CPU usage of `tidb-lightning`. See the `worker-pool-size` part in the first step of [Deploy `tidb-lightning`](#deploy-tidb-lightning). 

- tikv-importer
 
   - CPU bound and I/O bound, CPU with over 32 logical cores
   - 32 GB+ memory
   - 1 TB+ SSD
   - 10 Gigabit network card
   - Use standalone deployment because CPU, I/O and the network bandwidth might be fully occupied in the operation process. In the limited condition, you can deploy it along with other component (like `tikv-server`) on one machine, but the importing speed may be affected.

#### Hardware requirements for mixed deployment

In the limited condition, you can deploy `tidb-lightning` and `tikv-importer` (or another application) mixedly on one machine.
      
- CPU with over 32 logical cores
- 32 GB+ memory
- 1 TB+ SSD
- 10 Gigabit network card

> **Note:** `tidb-lightning` is CPU bound. If you use mixed deployment for it, you need to configure `worker-pool-size` to limit the number of occupied CPU cores of `tidb-lightning`. Otherwise other applications may be affected.

>   You can configure the `worker-pool-size` parameter of `tidb-lightning` to allocate 75% of CPU resources to `tidb-lightning`. For example, if CPU has 32 logical cores, you can set `worker-pool-size` to 24.

### Prepare

Before importing, you should:

- Deploy a set of TiDB cluster (TiDB version is 2.0.4 or later) which is the target cluster for importing (the target cluster).
- Prepare the binary file and the configuration file of `tikv-importer`. It is recommended to use standalone deployment.
- Prepare the binary file and the configuration file of `tidb-lightning`. It is recommended to use standalone deployment.

Download the installation packages of `tikv-importer` and `tidb-lightning` via:

https://download.pingcap.org/tidb-lightning-latest-linux-amd64.tar.gz

### Deploy

#### Deploy the TiDB cluster

For details, see [Deploy TiDB Using Ansible](https://pingcap.com/docs/op-guide/ansible-deployment/).

#### Deploy `tikv-importer`

1. Configure `tikv-importer.toml.

    ```
    # TiKV Importer configuration file template

    # log file.
    log-file = "tikv-importer.log"
    # log level: trace, debug, info, warn, error, off.
    log-level = "info"

    [server]
    # the listening address of tikv-importer. tidb-lightning needs to connect to this address to write data. Set it to the actual IP address.
    addr = "172.16.30.4:20170"
    # size of thread pool for the gRPC server.
    grpc-concurrency = 16

    [metric]
    # the Prometheus client push job name.
    job = "tikv-importer"
    # the Prometheus client push interval.
    interval = "15s"
    # the Prometheus Pushgateway address.
    address = ""

    [rocksdb]
    # the maximum number of concurrent background jobs.
    max-background-jobs = 32

    [rocksdb.defaultcf]
    # amount of data to build up in memory before flushing data to the disk.
    write-buffer-size = "1GB"
    # the maximum number of write buffers that are built up in memory.
    max-write-buffer-number = 8

    # the compression algorithms used in different levels.
    # the algorithm at level-0 is used to compress KV data.
    # the algorithm at level-6 is used to compress SST files.
    # the algorithms at level-1 ~ level-5 are not used now.
    compression-per-level = ["lz4", "no", "no", "no", "no", "no", "zstd"]

    [import]
    # this directory is used to store the data written by `tidb-lightning`.
    import-dir = "/tmp/tikv/import"
    # the number of threads to handle RPC requests.
    num-threads = 16
    # the number of concurrent import jobs.
    num-import-jobs = 24
    # the stream channel window size. Stream will be blocked when the channel is full. 
    stream-channel-window = 128
    ```

2. Run the executable file of `tikv-importer`.

    ```
    nohup ./tikv-importer -C tikv-importer.toml > nohup.out &
    ```

#### Deploy `tidb-lightning`

1. Configure `tidb-lightning.toml`.

    ```
    ### tidb-lightning configuration
    [lightning]

    # background profile for debugging ( 0 to disable )
    pprof-port = 10089

    # change the concurrency number of data. It is set to the number of logical CPU cores by default and needs no configuration.
    # in mixed configuration, you can set it to 75% of the size of logical CPU cores.
    # worker-pool-size =

    # logging
    level = "info"
    file = "tidb-lightning.log"
    max-size = 128 # MB
    max-days = 28
    max-backups = 14

    [tikv-importer]
    # the listening address of tikv-importer. Change it to the actual address in tikv-importer.toml.
    addr = "172.16.31.4:20170"
    # size of batch to import KV data into TiKV: xxx (GB)
    batch-size = 500 # GB

    [mydumper]
    # block size of file reading
    read-block-size = 4096 # Byte (default = 4 KB)
    # split source data file into multiple Region/chunk to execute restoring in parallel
    region-min-size = 268435456 # Byte (default = 256 MB)
    # the source data directory of Mydumper. tidb-lightning will automatically create the corresponding database and tables based on the schema file in the directory.
    data-source-dir = "/data/mydumper"
    # If no-schema is set to true, tidb-lightning will obtain the table schema information from tidb-server, instead of creating the database or tables based on the schema file of data-source-dir. This applies to manually creating tables or the situation where the table schema exits in TiDB.
    no-schema = false



    # configuration for TiDB (pick one of them if it has many TiDB servers) and the PD server.
    [tidb]
    # the target cluster information
    # the listening address of tidb-server. Setting one of them is enough.
    host = "127.0.0.1"
    port = 4000
    user = "root"
    password = ""
    # table schema information is fetched from TiDB via this status-port.
    status-port = 10080
    # the listening address of pd-server. Setting one of them is enough.
    pd-addr = "127.0.0.1:2379"
    # Lightning uses some code of TiDB (used as a library) and the flag controls its log level.
    log-level = "error"
    # set TiDB session variable to speed up performing the Checksum or Analyze operation on the table.
    distsql-scan-concurrency = 16

    # when data importing is finished, tidb-lightning can automatically perform the Checksum, Compact and Analyze operations.
    # it is recommended to set it to true in the production environment.
    # the execution order: Checksum -> Compact -> Analyze
    [post-restore]
    if it is set to true, tidb-lightning will perform the ADMIN CHECKSUM TABLE <table> operation on the tables one by one.
    checksum = true
    # if it is set to true, tidb-lightning will perform a full Compact operation on all the data. If the Compact operation fails, you can use ./bin/tidb-lightning -compact or the command of tikv-ctl to compact the data manually.
    compact = true
    # if it is set to true, tidb-lightning will perform the ANALYZE TABLE <table> operation on the tables one by one. If the Analyze operation fails, you can analyze data manually on the Mysql client.
    analyze = true
    ```

2. Run the executable file of `tidb-lightning`.

    ```
    nohup ./tidb-lightning -c tidb-lightning.toml > nohup.out &
    ```

### Notes

- When TiDB Lightning is running, the TiDB cluster cannot provide services.
- When you import data using TiDB Lightning, you cannot check some source data constraints such as the primary key conflict and unique index conflict. If needed, you can check using `ADMIN CHECK TABLE` via the MySQL client after importing, but it may take a long time.
- Currently，TiDB Lightning does not support breakpoint. If any error occurs during importing, delete the data using `DROP TABLE` and import the data again.
- If TiDB Lightning exits abnormally, you need to use the `-swtich-mode` command line parameter of `tidb-lightning` to manually close the import mode of the TiKV cluster and change it to the normal mode:

    ```
    ./bin/tidb-lightning -switch-mode normal
    ```
