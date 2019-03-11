# TiDB-Lightning Changelog

All notable changes to this project will be documented in this file.
See also [Release Notes](https://pingcap.com/docs/releases/rn/).

## [2.1.6] (2019-03-15)

[2.1.6]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.6

* Separated the "data" and "index" parts of a table into different engine files.
    This greatly improves import speed when "batching" is used with huge tables.
* Supported importing CSV files.
* Supported database with non-alphanumeric characters.

## [2.1.5] (2019-03-01)

[2.1.5]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.5

* Do not report an error or exit when a Tombstone store exists in the cluster.

## [2.1.4] (2019-02-15)

[2.1.4]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.4

* Added an option to disable periodic Level-1 compaction.
    When the TiKV cluster is 2.1.4 or above, Level-1 compaction is performed
    automatically even during import mode, and thus should be turned off
    on Lightning's side.
* Limited the number of engines queued for import, to avoid overflowing
    Importer's hard disk space.

## [2.1.3] (2019-01-29)

[2.1.3]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.3

* Open sourced.
* Improved memory use and performance when parsing the SQL dump.
* Lightning no longer splits a data file into multiple chunks, which eliminates
    time wasted on parsing every file twice.
* Limited I/O concurrency to avoid performance drop caused by cache miss.
* Implemented "batching" for partially ingest a large table into TiDB.
    This should improve stability of the import process.

## [2.1.2] (2018-12-22)

[2.1.2]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.2

* The minimum cluster version requirement enforced as 2.1.0
* Fixed parse error when the data file contains JSON data
* Fixed the `Too many open engines` error after resuming from checkpoint

## [2.1.1] (2018-12-12)

[2.1.1]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.1

* Parallelized ANALYZE calls, improving import speed when ANALYZE is enabled
* Support storing checkpoints in local filesystem
* Added a progress log
* Ensure Compact is always executed sequentially
* Fixed the issue where a checkpoint database cannot be created on MySQL due to the 3072-byte PRIMARY KEY limit.

## [2.1.0] (2018-11-30)

[2.1.0]: https://github.com/pingcap/tidb-lightning/releases/tag/v2.1.0

* Initial public release.
