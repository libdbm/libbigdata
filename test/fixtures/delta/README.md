# Delta Lake regression fixtures

Checked-in Delta tables used by `test/delta_test.dart` (the `cross-engine fixtures` group) to guard
deletion-vector **read** compatibility across engines without needing
Docker/Spark/DuckDB in CI. Each table deletes rows via a merge-on-read deletion
vector (no data-file rewrite), so reading them exercises the DV path.

| Fixture | Author | Expected live `id`s |
|---|---|---|
| `dart_written` | this library | `1,3,5,7,9,100..104` |
| `spark_written` | Apache Spark 3.5.3 + delta-spark 3.2.0 | `0,1,2,3,4` |
| `spark4_written` | Apache Spark 4.0.0 + delta-spark 4.0.0 | `0,1,2,3,4` |

The tables are relocatable (relative `add.path`, `storageType: 'u'` deletion
vectors), so they read correctly from this directory.

## Regenerating / cross-validating

The `dart_written` fixture is checked in. It was cross-validated by
DuckDB's delta extension and by both Spark versions reading it:

```
duckdb -c "INSTALL delta; LOAD delta; SELECT * FROM delta_scan('<path>')"
```

The Spark fixtures were produced in a container (Spark range(0,10) →
`repartition(1)` so a single file survives the delete → `DELETE WHERE id >= 5`
with `delta.enableDeletionVectors=true`):

```
docker run --rm -v $PWD:/data eclipse-temurin:17-jdk-jammy bash -c '
  apt-get update -qq && apt-get install -y -qq python3 python3-pip
  pip3 install -q pyspark==4.0.0 delta-spark==4.0.0     # or 3.5.3 / 3.2.0
  python3 /data/run_interop4.py'
```
