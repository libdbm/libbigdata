# libbigdata

A pure-Dart library for big-data manipulation: a DataFrame API and an in-memory
SQL query engine, inspired by pandas and DuckDB, with native readers and writers
for the common columnar and table formats. No native dependencies.

## Features

- **DataFrame API**: chainable `select` / `filter` / `orderBy` / `groupBy` /
  `agg` / `join` / `withColumn`, evaluated on a columnar engine.
- **SQL engine**: query DataFrames and files directly, e.g.
  `SELECT * FROM 'data.parquet' WHERE x > 10`.
- **Formats**: CSV, JSON / JSON Lines, Parquet, Arrow IPC (file and stream),
  Delta Lake, and Apache Iceberg, each read and written, with streaming/batch
  variants for out-of-core inputs.
- **Parquet codecs**: Snappy, GZIP, ZSTD, LZ4 (`LZ4_RAW` read+write; Hadoop
  `LZ4` read), plus optional page CRC32 integrity checks.

## Install

```yaml
dependencies:
  libbigdata: ^2.0.0
```

```bash
dart pub get
```

## Quick start

### DataFrame API

```dart
import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  switch (await DataFrameIO.readCsv('people.csv')) {
    case Ok(value: final df):
      final adults = await df
          .filter('age >= 18')
          .select(['name', 'age', 'city'])
          .orderBy('age', ascending: false)
          .limit(10)
          .execute();
      if (adults case Ok(:final value)) print(value.toRows());
    case Err(:final error):
      print('could not read people.csv: $error');
  }
}
```

### Grouping, joins, derived columns

```dart
final byCity = await df
    .groupBy(['city'])
    .agg({'age': 'avg', 'name': 'count'})
    .execute(); // Result<Table, ErrorDetails>

final joined = await orders
    .join(customers, 'customer_id', type: JoinType.left)
    .withColumn('total', 'price * quantity')
    .execute();
```

The `filter` and `withColumn` strings are SQL scalar expressions, as in a
`WHERE` clause or a `SELECT` item. See [doc/DATAFRAME.md](doc/DATAFRAME.md) for
the details, and use the SQL engine for what the DataFrame API does not cover.

### SQL

```dart
// Query a file directly, on a shared engine.
switch (await sql("SELECT city, count(*) FROM 'people.csv' GROUP BY city")) {
  case Ok(:final value):
    print(value.toRows());
  case Err(:final error) when error.kind == Kind.invalid:
    print('bad query: ${error.message}');
  case Err(:final error):
    print('query failed: $error');
}

// Or open an engine of your own, register a DataFrame and query it.
if (SqlEngine.open() case Ok(value: final engine)) {
  if (await engine.registerDataFrame('people', df) case Ok()) {
    final top = await engine.sql('SELECT name FROM people WHERE age > 30');
    print(top);
  }
  await engine.dispose();
}
```

### Errors

Reads, writes, queries and commits return a `Result<T, ErrorDetails>`: `Ok`
with the value, or `Err` with an `ErrorDetails` whose `kind` says what went
wrong (`invalid`, `missing`, `corrupt`, `limit`, `conflict`, ...) and whose
`message` says why. Nothing else escapes these methods; only argument
preconditions (such as a batch size of 0) throw. Upgrading from 1.x? See
[doc/MIGRATION.md](doc/MIGRATION.md).

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

- [doc/DATAFRAME.md](doc/DATAFRAME.md): the DataFrame API: operations, expression syntax, execution.
- [doc/SQL.md](doc/SQL.md): the SQL engine: API, supported syntax, architecture.
- [doc/DESIGN.md](doc/DESIGN.md): architecture: data model, query pipeline, spilling, commit coordination.
- [doc/CLI.md](doc/CLI.md): the native command-line interface.
- [doc/parquet_support.md](doc/parquet_support.md): Parquet format coverage.
- [doc/MIGRATION.md](doc/MIGRATION.md): moving from 1.x to 2.0.

Runnable examples live in [example/](example/).

## Status

API under active development. 2.0 changed every fallible entry point to return
a `Result`; see [doc/MIGRATION.md](doc/MIGRATION.md).
Contributions and bug reports welcome at
[github.com/libdbm/libbigdata](https://github.com/libdbm/libbigdata/issues).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
