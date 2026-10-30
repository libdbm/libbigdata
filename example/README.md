# libbigdata examples

Run any example with `dart run example/<file>`.

## [`dataframe_example.dart`](dataframe_example.dart)

The DataFrame API end to end: building from rows, `filter` / `select` / `orderBy` / `limit`,
`withColumn`, `groupBy` + `agg`, `join`, terminal operations, bounded execution, a
round-trip through a CSV file, and handing off to SQL for what the DataFrame API does not
cover.

```bash
dart run example/dataframe_example.dart
```

## [`sql_example.dart`](sql_example.dart)

The SQL engine end to end: build a `Table` from rows, register it with a `SqlEngine`, then
run `SELECT`, `WHERE`, `GROUP BY` / aggregate, `ORDER BY` and `JOIN` queries, printing each
result as a formatted table.

```bash
dart run example/sql_example.dart
```

## [`logging_example.dart`](logging_example.dart)

Configuring `LoggingConfig`: log levels, the library's per-subsystem loggers
(`Loggers.delta`, `Loggers.json` and the others under `Loggers.root`), coloured console
output, and logging to a file.

```bash
dart run example/logging_example.dart
```

## Minimal usage

```dart
import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  // Query a file directly with SQL. Queries return a Result.
  switch (await sql("SELECT city, count(*) FROM 'people.csv' GROUP BY city")) {
    case Ok(:final value):
      print(value.toRows());
    case Err(:final error):
      print('query failed: $error');
  }

  // Or use the DataFrame API.
  if (await DataFrameIO.readCsv('people.csv') case Ok(value: final df)) {
    final adults = await df
        .filter('age >= 18')
        .select(['name', 'age'])
        .orderBy('age', ascending: false)
        .limit(10)
        .execute();
    if (adults case Ok(:final value)) print(value.toRows());
  }
}
```

See the [main README](../README.md), [doc/DATAFRAME.md](../doc/DATAFRAME.md) and
[doc/SQL.md](../doc/SQL.md) for more.
