# Changelog

## 1.0.0

First public release.

### DataFrame API

- Chainable, lazily-evaluated operations — `select`, `filter`, `orderBy`, `groupBy`, `agg`,
  `join`, `withColumn`, `distinct`, `union`, `sample` — executed on a columnar engine with
  vectorized filter and projection kernels.
- Typed columnar storage (`Column`, `Table`, `Schema`, `Field`) with null bitmaps, zero-copy
  slicing, and automatic schema inference.

### SQL engine

- Query files directly: `SELECT * FROM 'data.parquet' WHERE x > 10`, or register DataFrames
  and tables with `SqlEngine`.
- Joins across multiple files, CTEs and views, set operations, grouping sets, window-free
  aggregation with exact `BigInt` sums, and full three-valued (NULL) logic.
- `VALUES` as a table source (`SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(n, s)`), and
  `SELECT` without a `FROM` clause.
- `INSERT INTO '<delta table>' [(cols)] VALUES (...)`, alongside `UPDATE` and `MERGE`.
- Predicate and projection pushdown into the Parquet reader; `executeStream` for incremental
  results.
- Operators spill to disk past a configurable memory budget (default 256 MB), so `ORDER BY`,
  `GROUP BY` and equi-joins work on inputs larger than memory.

### Formats

- **CSV** — auto-detected delimiters and types, streaming batch reads, streaming writes.
- **JSON / JSON Lines** — streaming batch reads and writes.
- **Parquet** — read and write, with Snappy, GZIP, ZSTD and LZ4 (`LZ4_RAW` read+write, Hadoop
  `LZ4` read) codecs, optional page CRC32 verification, and nested list/struct/map types.
- **Arrow IPC** — read and write, streaming batches.
- **Delta Lake** — append/overwrite, `delete`/`update`/`merge`, time travel, deletion vectors,
  and pluggable commit coordinators for multi-writer safety.
- **Apache Iceberg** — snapshots, time travel, schema evolution, partitioning, positional and
  equality deletes, and pluggable catalogs.
- **Datasets** — read partitioned directory trees as a single table, with optional parallel
  reads across isolates.

### Correctness

Numeric values are never silently rewritten. Integer literals are typed by range, the fixed-width
Parquet encoders reject a value they would truncate, and a mixed integer/float column (CSV, JSON, or
`Schema.infer`) promotes to an exact `decimal` rather than rounding under float64. Where an exact
result is impossible — unifying `int64` with `float64` in a set operation — the conversion fails
loudly instead of rounding. JSON field flattening rejects a name collision rather than silently
keeping whichever value came last.

### Types

`Decimal` is an exact decimal value (unscaled `BigInt` + scale) with value-based equality, returned
by `Table.toRows()` for decimal columns — including integer `SUM`, which is exact beyond 2^63.

Temporal, decimal, and nested types round-trip across Arrow and Parquet. Read paths are bounded by
opt-out resource caps and fail loudly rather than silently truncating or corrupting data. Data files
are flushed to stable media before being published. Interoperability with PySpark, `deltalake` and
PyIceberg is verified by a gated test suite.

### Command line

- `libbigdata` — run SQL against CSV and Parquet files, with table/JSON/CSV output and an
  interactive REPL. Install with `dart pub global activate libbigdata`.
