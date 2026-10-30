# Interop tests

The `interop`-tagged tests check that files written by libbigdata read correctly
in other engines, and that files those engines write read correctly here:

| Test | Direction |
|---|---|
| `test/interop/spark_interop_test.dart` | libbigdata writes (`write_samples.dart`); Spark, pyarrow, delta-rs and pyiceberg read (`validate_all.py`) |
| `test/interop/external_read_test.dart` | pyarrow, delta-rs, pyiceberg, fastavro and Spark write (`gen_external.py`); libbigdata reads |
| `test/interop/duckdb_read_test.dart` | DuckDB writes and reads Parquet; libbigdata reads the same files |
| `test/interop/parquet_interop_test.dart` | libbigdata writes; pyarrow reads |

The tests generate their fixtures themselves, so there is no separate setup
step. They are skipped by a plain `dart test`.

## Running

```bash
make interop
```

This checks the prerequisites (`tool/interop/check.py`), naming anything
missing with an install hint, then runs `dart test -t interop --run-skipped`.

## Prerequisites

- Python 3 with `pyarrow`, `deltalake`, `pyiceberg[sql-sqlite]`, `fastavro` and
  `pyspark`:

  ```bash
  pip install pyarrow deltalake 'pyiceberg[sql-sqlite]' fastavro pyspark
  ```

- A JDK (17 or 21) with `java` on `PATH`, for Spark.
- The `duckdb` command-line tool (`brew install duckdb`, or duckdb.org).

delta-spark is not needed: Delta tables are read and written through delta-rs.

Last verified with pyarrow 24.0.0, deltalake 1.6.0, pyspark 4.1.1, DuckDB 1.4.1
and OpenJDK 21.
