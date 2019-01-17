# TiDB-Lightning Changelog

All notable changes to this project will be documented in this file.
See also [Release Notes](https://pingcap.com/docs/releases/rn/).

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
