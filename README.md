# libbigdata

A pure-Dart library for big-data manipulation — a DataFrame API and an in-memory
SQL query engine, inspired by pandas and DuckDB, with native readers and writers
for the common columnar and table formats. No native dependencies.

## Features

- **DataFrame API** — chainable `select` / `filter` / `orderBy` / `groupBy` /
  `agg` / `join` / `withColumn`, evaluated on a columnar engine.
- **SQL engine** — query DataFrames and files directly, e.g.
  `SELECT * FROM 'data.parquet' WHERE x > 10`.
- **Formats** — CSV, JSON / JSON Lines, Parquet, Arrow IPC, Delta Lake, and
  Apache Iceberg (read), with streaming/batch variants for out-of-core inputs.
- **Parquet codecs** — Snappy, GZIP, ZSTD, LZ4 (`LZ4_RAW` read+write; Hadoop
  `LZ4` read), plus optional page CRC32 integrity checks.

## Install

```yaml
dependencies:
  libbigdata: ^1.0.0
```

```bash
dart pub get
```

## Quick start

### DataFrame API

```dart
import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  final df = await DataFrameIO.readCsv('people.csv');

  final adults = await df
      .filter('age >= 18')
      .select(['name', 'age', 'city'])
      .orderBy('age', ascending: false)
      .limit(10)
      .execute();

  print(adults.toRows());
}
```

### Grouping, joins, derived columns

```dart
final byCity = await df
    .groupBy(['city'])
    .agg({'age': 'avg', 'name': 'count'})
    .execute();

final joined = await orders
    .join(customers, 'customer_id', type: JoinType.left)
    .withColumn('total', 'price * quantity')
    .execute();
```

The `filter` and `withColumn` strings are small, deliberately narrow expression
languages rather than SQL. See [doc/DATAFRAME.md](doc/DATAFRAME.md) for the
exact grammar each accepts, and use the SQL engine for anything richer.

### SQL

```dart
// Query a file directly.
final result = await sql.execute("SELECT city, count(*) FROM 'people.csv' GROUP BY city");

// Or register a DataFrame and query it.
final engine = SqlEngine();
await engine.registerDataFrame('people', df);
final top = await engine.sql('SELECT name FROM people WHERE age > 30');
```

## Formats

| Format        | Read | Write | Notes                                   |
|---------------|:----:|:-----:|-----------------------------------------|
| CSV           | Yes  | Yes   | auto-inference, streaming batches        |
| JSON / JSONL  | Yes  | Yes   | streaming batches (JSON Lines)           |
| Parquet       | Yes  | Yes   | predicate/column pushdown, optional CRC32|
| Arrow IPC     | Yes  | Yes   | streaming batches                        |
| Delta Lake    | Yes  | Yes   | append/overwrite, delete/update/merge, time travel |
| Iceberg       | Yes  | Yes   | snapshots, time travel                   |

For files larger than memory, use the streaming readers
(`readCsvBatches`, `readJsonBatches`, `readArrowBatches`), the streaming Parquet
writer (`writeParquetStream`), and `SqlEngine.executeStream`.

## Documentation

- [doc/DATAFRAME.md](doc/DATAFRAME.md) — the DataFrame API: operations, expression syntax, execution.
- [doc/SQL.md](doc/SQL.md) — the SQL engine: API, supported syntax, architecture.
- [doc/DESIGN.md](doc/DESIGN.md) — architecture: data model, query pipeline, spilling, commit coordination.
- [doc/CLI.md](doc/CLI.md) — the native command-line interface.
- [doc/parquet_support.md](doc/parquet_support.md) — Parquet format coverage.

Runnable examples live in [example/](example/).

## Status

API under active development; the surface may change between minor versions.
Contributions and bug reports welcome at
[github.com/libdbm/libbigdata](https://github.com/libdbm/libbigdata/issues).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
