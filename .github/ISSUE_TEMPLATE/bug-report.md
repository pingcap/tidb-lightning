---
name: "\U0001F41B Bug Report"
labels: "bug"
about: Something isn't working as expected

---

## Bug Report

Please answer these questions before submitting your issue. Thanks!

1. What did you do? If possible, provide a recipe for reproducing the error.

2. What did you expect to see?

3. What did you see instead?

4. Versions of the cluster

    - TiDB-Lightning version (run `tidb-lightning -V`):

        ```
        (paste TiDB-Lightning version here)
        ```
        
    - TiKV-Importer version (run `tikv-importer -V`)

        ```
        (paste TiKV-Importer version here)
        ```
        
    - TiKV version (run `tikv-server -V`):

        ```
        (paste TiKV version here)
        ```

    - TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste TiDB cluster version here)
        ```

    - Other interesting information (system version, hardware config, etc):

        >
        >

5. Operation logs
   - Please upload `tidb-lightning.log` for TiDB-Lightning if possible
   - Please upload `tikv-importer.log` from TiKV-Importer if possible
   - Other interesting logs
   
6. Configuration of the cluster and the task
   - `tidb-lightning.toml` for TiDB-Lightning if possible
   - `tikv-importer.toml` for TiKV-Importer if possible
   - `inventory.ini` if deployed by Ansible

7. Screenshot/exported-PDF of Grafana dashboard or metrics' graph in Prometheus for TiDB-Lightning if possible
